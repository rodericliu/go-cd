package config

import (
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// File is the root YAML configuration.
type File struct {
	Defaults Defaults            `yaml:"defaults"`
	Admin    Admin               `yaml:"admin"`
	Servers  map[string]Server   `yaml:"servers"`
	Jobs     []Job               `yaml:"jobs"`
}

type Defaults struct {
	URL              string `yaml:"url"`
	URLHost          string `yaml:"url_host"`
	Count            int    `yaml:"count"`
	TimeoutMS        int    `yaml:"timeout_ms"`
	Encoding         string `yaml:"encoding"`
	RetryLimit       int    `yaml:"retry_limit"`
	Prefetch         int    `yaml:"prefetch"`
	DeclareTopology  *bool  `yaml:"declare_topology"`
}

type Admin struct {
	Host string `yaml:"host"`
	Port int    `yaml:"port"`
}

type Server struct {
	Host              string `yaml:"host"`
	Port              int    `yaml:"port"`
	Username          string `yaml:"username"`
	Password          string `yaml:"password"`
	VHost             string `yaml:"vhost"`
	ReconnectSeconds  int    `yaml:"reconnect_seconds"`
}

type Job struct {
	Name             string      `yaml:"name"`
	Server           StringList  `yaml:"server"` // one name, or a list of server names
	Queue            string      `yaml:"queue"`
	Exchange         string      `yaml:"exchange"`
	Type             string      `yaml:"type"`
	URL              string      `yaml:"url"`
	URLHost          string      `yaml:"url_host"`
	Encoding         string      `yaml:"encoding"`
	TimeoutMS        int         `yaml:"timeout_ms"`
	Count            int         `yaml:"count"`
	RetryLimit       int         `yaml:"retry_limit"`
	Prefetch         int         `yaml:"prefetch"`
	RouteKey         string      `yaml:"route_key"`
	BlockPatterns    string      `yaml:"block_patterns"`
	DiscardMatchJobs bool        `yaml:"discard_match_jobs"`
	ExchangeDurable  *bool       `yaml:"exchange_durable"`
	QueueDurable     *bool       `yaml:"queue_durable"`
	DeclareTopology  *bool       `yaml:"declare_topology"`
	DLQ              string      `yaml:"dlq"` // empty => queue + ".dlq"
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
}

var envPattern = regexp.MustCompile(`\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-([^}]*))?\}|\$([A-Za-z_][A-Za-z0-9_]*)`)

// Load reads YAML from path, expands ${ENV} / ${ENV:-default}, and resolves jobs.
func Load(path string) (*File, []ResolvedJob, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, nil, fmt.Errorf("read config: %w", err)
	}
	expanded := expandEnv(string(raw))

	var cfg File
	if err := yaml.Unmarshal([]byte(expanded), &cfg); err != nil {
		return nil, nil, fmt.Errorf("parse yaml: %w", err)
	}
	applyDefaults(&cfg)

	jobs, err := resolveJobs(&cfg)
	if err != nil {
		return nil, nil, err
	}
	return &cfg, jobs, nil
}

func applyDefaults(cfg *File) {
	if cfg.Defaults.Count <= 0 {
		cfg.Defaults.Count = 1
	}
	if cfg.Defaults.TimeoutMS <= 0 {
		cfg.Defaults.TimeoutMS = 30000
	}
	if cfg.Defaults.Encoding == "" {
		cfg.Defaults.Encoding = "utf-8"
	}
	if cfg.Defaults.RetryLimit <= 0 {
		cfg.Defaults.RetryLimit = 1
	}
	if cfg.Defaults.Prefetch <= 0 {
		cfg.Defaults.Prefetch = 1
	}
	if cfg.Admin.Host == "" {
		cfg.Admin.Host = "0.0.0.0"
	}
	if cfg.Admin.Port <= 0 {
		cfg.Admin.Port = 8888
	}
}

func resolveJobs(cfg *File) ([]ResolvedJob, error) {
	if len(cfg.Servers) == 0 {
		return nil, fmt.Errorf("no servers defined")
	}
	if len(cfg.Jobs) == 0 {
		return nil, fmt.Errorf("no jobs defined")
	}

	out := make([]ResolvedJob, 0, len(cfg.Jobs))
	seen := map[string]struct{}{}

	for i, j := range cfg.Jobs {
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
				Count:            pickPositive(j.Count, cfg.Defaults.Count),
				RetryLimit:       pickPositive(j.RetryLimit, cfg.Defaults.RetryLimit),
				Prefetch:         pickPositive(j.Prefetch, cfg.Defaults.Prefetch),
				RouteKey:         j.RouteKey,
				BlockPatterns:    j.BlockPatterns,
				DiscardMatchJobs: j.DiscardMatchJobs,
				ExchangeDurable:  boolOrDefault(j.ExchangeDurable, true),
				QueueDurable:     boolOrDefault(j.QueueDurable, true),
				DeclareTopology:  declare,
				DLQ:              firstNonEmpty(j.DLQ, firstNonEmpty(j.Queue, j.Name)+".dlq"),
			}
			if rj.URL == "" {
				return nil, fmt.Errorf("job %q: url is required (set job.url or defaults.url)", j.Name)
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
		if sub[1] != "" {
			if v, ok := os.LookupEnv(sub[1]); ok {
				return v
			}
			return sub[2] // default after :-
		}
		if sub[3] != "" {
			return os.Getenv(sub[3])
		}
		return m
	})
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
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

func boolOrDefault(p *bool, def bool) bool {
	if p == nil {
		return def
	}
	return *p
}

// AMQPURL builds an amqp URL (password redacted when redact=true).
func (s Server) AMQPURL(redact bool) string {
	pass := s.Password
	if redact {
		pass = "***"
	}
	vhost := s.VHost
	if !strings.HasPrefix(vhost, "/") {
		vhost = "/" + vhost
	}
	// amqp path is URL-escaped vhost; keep simple for common cases
	return fmt.Sprintf("amqp://%s:%s@%s:%d%s", s.Username, pass, s.Host, s.Port, vhost)
}

func (j ResolvedJob) String() string {
	return fmt.Sprintf("job=%s server=%s queue=%s url=%s workers=%d timeout=%s retry=%d prefetch=%d",
		j.LogicName, j.ServerName, j.Queue, j.URL, j.Count, j.Timeout, j.RetryLimit, j.Prefetch)
}

// Addr returns admin listen address.
func (a Admin) Addr() string {
	return a.Host + ":" + strconv.Itoa(a.Port)
}
