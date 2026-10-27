package config

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"regexp"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

const (
	defaultCount      = 1
	defaultRetryLimit = 1
	defaultDLQTTLDays = 7
)

// File is the root YAML configuration.
type File struct {
	Defaults Defaults          `yaml:"defaults"`
	Admin    Admin             `yaml:"admin"`
	Log      Log               `yaml:"log"`
	Servers  map[string]Server `yaml:"servers"`
	Jobs     []Job             `yaml:"jobs"`
}

// Defaults are applied to every job that leaves the field unset.
// Count and RetryLimit are pointers so that an explicit 0 is distinguishable from "unset".
type Defaults struct {
	URL             string `yaml:"url"`
	URLHost         string `yaml:"url_host"`
	Count           *int   `yaml:"count"`
	TimeoutMS       int    `yaml:"timeout_ms"`
	Encoding        string `yaml:"encoding"`
	RetryLimit      *int   `yaml:"retry_limit"`
	Prefetch        int    `yaml:"prefetch"`
	DeclareTopology *bool  `yaml:"declare_topology"`
	DLQTTLDays      *int   `yaml:"dlq_ttl_days"` // 0 => DLQ messages never expire

	Autoscale AutoscaleFile `yaml:"autoscale"`
}

type Server struct {
	Host             string `yaml:"host"`
	Port             int    `yaml:"port"`
	Username         string `yaml:"username"`
	Password         string `yaml:"password"`
	VHost            string `yaml:"vhost"`
	ReconnectSeconds int    `yaml:"reconnect_seconds"`
}

type Job struct {
	Name             string     `yaml:"name"`
	Server           StringList `yaml:"server"` // one name, or a list of server names
	Queue            string     `yaml:"queue"`
	Exchange         string     `yaml:"exchange"`
	Type             string     `yaml:"type"`
	URL              string     `yaml:"url"`
	URLHost          string     `yaml:"url_host"`
	Encoding         string     `yaml:"encoding"`
	TimeoutMS        int        `yaml:"timeout_ms"`
	Count            *int       `yaml:"count"`
	RetryLimit       *int       `yaml:"retry_limit"`
	Prefetch         int        `yaml:"prefetch"`
	RouteKey         string     `yaml:"route_key"`
	BlockPatterns    string     `yaml:"block_patterns"`
	DiscardMatchJobs bool       `yaml:"discard_match_jobs"`
	ExchangeDurable  *bool      `yaml:"exchange_durable"`
	QueueDurable     *bool      `yaml:"queue_durable"`
	DeclareTopology  *bool      `yaml:"declare_topology"`
	DLQ              string     `yaml:"dlq"` // empty => queue + ".dlq"
	DLQTTLDays       *int       `yaml:"dlq_ttl_days"`

	Autoscale AutoscaleFile `yaml:"autoscale"`
}

// StringList unmarshals a YAML string or sequence of strings.
type StringList []string

func (s *StringList) UnmarshalYAML(value *yaml.Node) error {
	switch value.Kind {
	case yaml.ScalarNode:
		var one string
		if err := value.Decode(&one); err != nil {
			return err
		}
		one = strings.TrimSpace(one)
		if one == "" {
			*s = nil
			return nil
		}
		*s = StringList{one}
		return nil
	case yaml.SequenceNode:
		var many []string
		if err := value.Decode(&many); err != nil {
			return err
		}
		out := make(StringList, 0, len(many))
		for _, v := range many {
			v = strings.TrimSpace(v)
			if v == "" {
				continue
			}
			out = append(out, v)
		}
		*s = out
		return nil
	default:
		return fmt.Errorf("server: want string or list, got yaml kind %v", value.Kind)
	}
}

// ResolvedJob is a job with defaults and server binding applied.
type ResolvedJob struct {
	Name             string
	LogicName        string
	ServerName       string
	Server           Server
	Queue            string
	Exchange         string
	Type             string
	URL              string
	URLHost          string
	Encoding         string
	Timeout          time.Duration
	Count            int
	RetryLimit       int
	Prefetch         int
	RouteKey         string
	BlockPatterns    string
	DiscardMatchJobs bool
	ExchangeDurable  bool
	QueueDurable     bool
	DeclareTopology  bool
	DLQ              string
	DLQTTL           time.Duration // 0 => DLQ messages never expire
	Autoscale        Autoscale
}

// Only ${VAR} and ${VAR:-default} are expanded; a bare "$" is kept literally
// so values such as passwords may contain it.
var envPattern = regexp.MustCompile(`\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-([^}]*))?\}`)

var exchangeTypes = map[string]bool{"direct": true, "fanout": true, "topic": true, "headers": true}

// Load reads YAML from path, expands ${ENV} / ${ENV:-default}, and resolves jobs.
// Expansion runs on parsed scalar values, so environment values are never
// re-interpreted as YAML syntax.
func Load(path string) (*File, []ResolvedJob, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, nil, fmt.Errorf("read config: %w", err)
	}

	var root yaml.Node
	if err := yaml.Unmarshal(raw, &root); err != nil {
		return nil, nil, fmt.Errorf("parse yaml: %w", err)
	}
	expandNode(&root)

	var cfg File
	if err := root.Decode(&cfg); err != nil {
		return nil, nil, fmt.Errorf("parse yaml: %w", err)
	}
	if err := applyDefaults(&cfg); err != nil {
		return nil, nil, err
	}

	jobs, err := resolveJobs(&cfg)
	if err != nil {
		return nil, nil, err
	}
	return &cfg, jobs, nil
}

func expandNode(n *yaml.Node) {
	if n.Kind == yaml.ScalarNode {
		expanded := expandEnv(n.Value)
		if expanded != n.Value {
			n.Value = expanded
			// Re-resolve the type from the expanded value so "${PORT}" can
			// fill an int field (string fields accept any scalar). A value
			// that would resolve to null ("", "null", "~") stays a string so
			// it is never silently dropped.
			n.Style = 0
			n.Tag = ""
			if n.ShortTag() == "!!null" {
				n.Tag = "!!str"
			}
		}
		return
	}
	for _, c := range n.Content {
		expandNode(c)
	}
}

func applyDefaults(cfg *File) error {
	if cfg.Defaults.Count == nil {
		cfg.Defaults.Count = new(defaultCount)
	}
	if cfg.Defaults.RetryLimit == nil {
		cfg.Defaults.RetryLimit = new(defaultRetryLimit)
	}
	if *cfg.Defaults.Count < 0 {
		return errors.New("defaults.count must be >= 0")
	}
	if *cfg.Defaults.RetryLimit < 0 {
		return errors.New("defaults.retry_limit must be >= 0")
	}
	if cfg.Defaults.DLQTTLDays == nil {
		cfg.Defaults.DLQTTLDays = new(defaultDLQTTLDays)
	}
	if *cfg.Defaults.DLQTTLDays < 0 {
		return errors.New("defaults.dlq_ttl_days must be >= 0")
	}
	if cfg.Defaults.TimeoutMS <= 0 {
		cfg.Defaults.TimeoutMS = 30000
	}
	if cfg.Defaults.Encoding == "" {
		cfg.Defaults.Encoding = "utf-8"
	}
	if cfg.Defaults.Prefetch <= 0 {
		cfg.Defaults.Prefetch = 1
	}
	if err := cfg.Admin.normalize(); err != nil {
		return err
	}
	return cfg.Log.normalize()
}

func resolveJobs(cfg *File) ([]ResolvedJob, error) {
	if len(cfg.Servers) == 0 {
		return nil, errors.New("no servers defined")
	}
	if len(cfg.Jobs) == 0 {
		return nil, errors.New("no jobs defined")
	}

	out := make([]ResolvedJob, 0, len(cfg.Jobs))
	seen := map[string]struct{}{}

	for i, j := range cfg.Jobs {
		j.Name = strings.TrimSpace(j.Name)
		if j.Name == "" {
			return nil, fmt.Errorf("jobs[%d]: name is required", i)
		}
		if len(j.Server) == 0 {
			return nil, fmt.Errorf("job %q: server is required", j.Name)
		}

		for _, serverName := range j.Server {
			srv, ok := cfg.Servers[serverName]
			if !ok {
				return nil, fmt.Errorf("job %q: unknown server %q", j.Name, serverName)
			}
			if err := validateServer(serverName, srv); err != nil {
				return nil, err
			}

			declare := boolOrDefault(j.DeclareTopology, boolOrDefault(cfg.Defaults.DeclareTopology, true))
			rj := ResolvedJob{
				Name:             j.Name,
				LogicName:        j.Name + "-" + serverName,
				ServerName:       serverName,
				Server:           withServerDefaults(srv),
				Queue:            firstNonEmpty(j.Queue, j.Name),
				Exchange:         firstNonEmpty(j.Exchange, j.Name+"_router"),
				Type:             firstNonEmpty(j.Type, "direct"),
				URL:              firstNonEmpty(j.URL, cfg.Defaults.URL),
				URLHost:          firstNonEmpty(j.URLHost, cfg.Defaults.URLHost),
				Encoding:         firstNonEmpty(j.Encoding, cfg.Defaults.Encoding),
				Timeout:          time.Duration(pickPositive(j.TimeoutMS, cfg.Defaults.TimeoutMS)) * time.Millisecond,
				Count:            intOrDefault(j.Count, *cfg.Defaults.Count),
				RetryLimit:       intOrDefault(j.RetryLimit, *cfg.Defaults.RetryLimit),
				Prefetch:         pickPositive(j.Prefetch, cfg.Defaults.Prefetch),
				RouteKey:         j.RouteKey,
				BlockPatterns:    j.BlockPatterns,
				DiscardMatchJobs: j.DiscardMatchJobs,
				ExchangeDurable:  boolOrDefault(j.ExchangeDurable, true),
				QueueDurable:     boolOrDefault(j.QueueDurable, true),
				DeclareTopology:  declare,
				DLQ:              firstNonEmpty(j.DLQ, firstNonEmpty(j.Queue, j.Name)+".dlq"),
				DLQTTL:           time.Duration(intOrDefault(j.DLQTTLDays, *cfg.Defaults.DLQTTLDays)) * 24 * time.Hour,
				Autoscale:        resolveAutoscale(j.Autoscale, cfg.Defaults.Autoscale),
			}
			if err := validateJob(rj); err != nil {
				return nil, fmt.Errorf("job %q: %w", j.Name, err)
			}
			if _, dup := seen[rj.LogicName]; dup {
				return nil, fmt.Errorf("duplicate job logic name %q", rj.LogicName)
			}
			seen[rj.LogicName] = struct{}{}
			out = append(out, rj)
		}
	}
	return out, nil
}

func validateJob(j ResolvedJob) error {
	if j.URL == "" {
		return errors.New("url is required (set job.url or defaults.url)")
	}
	if err := ValidateURL(j.URL); err != nil {
		return err
	}
	if err := ValidatePattern(j.BlockPatterns); err != nil {
		return err
	}
	if !exchangeTypes[j.Type] {
		return fmt.Errorf("type %q: want direct, fanout, topic or headers", j.Type)
	}
	if j.Count < 0 {
		return errors.New("count must be >= 0")
	}
	if j.RetryLimit < 0 {
		return errors.New("retry_limit must be >= 0")
	}
	if j.DLQTTL < 0 {
		return errors.New("dlq_ttl_days must be >= 0")
	}
	return j.Autoscale.Validate()
}

// ValidateURL reports whether raw is an absolute http(s) URL with a host.
func ValidateURL(raw string) error {
	u, err := url.ParseRequestURI(raw)
	if err != nil {
		return fmt.Errorf("url %q: %w", raw, err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("url %q: scheme must be http or https", raw)
	}
	if u.Host == "" {
		return fmt.Errorf("url %q: host is required", raw)
	}
	return nil
}

// ValidatePattern reports whether p is empty or a valid regular expression.
func ValidatePattern(p string) error {
	if p == "" {
		return nil
	}
	if _, err := regexp.Compile(p); err != nil {
		return fmt.Errorf("block_patterns: %w", err)
	}
	return nil
}

func validateServer(name string, s Server) error {
	if s.Host == "" {
		return fmt.Errorf("server %q: host is required", name)
	}
	if s.Port <= 0 {
		return fmt.Errorf("server %q: port is required", name)
	}
	if s.Username == "" {
		return fmt.Errorf("server %q: username is required", name)
	}
	if s.VHost == "" {
		return fmt.Errorf("server %q: vhost is required", name)
	}
	return nil
}

func withServerDefaults(s Server) Server {
	if s.ReconnectSeconds <= 0 {
		s.ReconnectSeconds = 30
	}
	return s
}

func expandEnv(s string) string {
	return envPattern.ReplaceAllStringFunc(s, func(m string) string {
		sub := envPattern.FindStringSubmatch(m)
		if v, ok := os.LookupEnv(sub[1]); ok {
			return v
		}
		return sub[2] // default after :-
	})
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if s := strings.TrimSpace(v); s != "" {
			return s
		}
	}
	return ""
}

func pickPositive(v, def int) int {
	if v > 0 {
		return v
	}
	return def
}

func intOrDefault(p *int, def int) int {
	if p == nil {
		return def
	}
	return *p
}

func boolOrDefault(p *bool, def bool) bool {
	if p == nil {
		return def
	}
	return *p
}

func (j ResolvedJob) String() string {
	s := fmt.Sprintf("job=%s server=%s queue=%s url=%s workers=%d timeout=%s retry=%d prefetch=%d",
		j.LogicName, j.ServerName, j.Queue, j.URL, j.InitialWorkers(), j.Timeout, j.RetryLimit, j.Prefetch)
	if a := j.Autoscale; a.Enabled {
		s += fmt.Sprintf(" autoscale=%d-%d", a.Min, a.Max)
	}
	return s
}
