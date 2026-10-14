package xmlconv

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"os"
	"regexp"
	"strings"

	"gopkg.in/yaml.v3"
)

// Options controls conversion behavior.
type Options struct {
	// MergeServers merges jobs that share the same name and config (except server)
	// into one job with server: [a, b, ...].
	MergeServers bool
	// EnvSecrets replaces plaintext passwords with ${SERVER_<NAME>_PASSWORD}.
	EnvSecrets bool
}

type jobsXML struct {
	XMLName xml.Name   `xml:"jobs"`
	Conf    confXML    `xml:"conf"`
	Monitor monitorXML `xml:"monitor"`
	Servers serversXML `xml:"servers"`
	Jobs    []jobXML   `xml:"job"`
}

type confXML struct {
	RequestPre     string `xml:"request-pre"`
	RequestHost    string `xml:"request-host"`
	RequestCount   int    `xml:"request-count"`
	RequestTimeout int    `xml:"request-timeout"`
	Encoding       string `xml:"encoding"`
}

type monitorXML struct {
	HTTPHost string `xml:"httpHost,attr"`
	HTTPPort int    `xml:"httpPort,attr"`
}

type serversXML struct {
	QueueServers []queueServerXML `xml:"queueserver"`
}

type queueServerXML struct {
	Name                 string `xml:"name,attr"`
	Host                 string `xml:"host,attr"`
	Port                 int    `xml:"port,attr"`
	UserName             string `xml:"userName,attr"`
	Password             string `xml:"password,attr"`
	VHost                string `xml:"vhost,attr"`
	RetryIntervalSeconds int    `xml:"retryIntervalSeconds,attr"`
	RedisHost            string `xml:"redisHost,attr"`
}

type jobXML struct {
	Name             string `xml:"name,attr"`
	Server           string `xml:"server,attr"`
	Queue            string `xml:"queue,attr"`
	Exchange         string `xml:"exchange,attr"`
	Type             string `xml:"type,attr"`
	URL              string `xml:"url,attr"`
	URLHost          string `xml:"urlhost,attr"`
	Encoding         string `xml:"encoding,attr"`
	Timeout          int    `xml:"timeout,attr"`
	Count            int    `xml:"count,attr"`
	RetryLimit       int    `xml:"retryLimit,attr"`
	Prefetch         int    `xml:"prefetch,attr"`
	RouteKey         string `xml:"routeKey,attr"`
	BlockPatterns    string `xml:"blockPatterns,attr"`
	DiscardMatchJobs string `xml:"discardMatchJobs,attr"`
	ExchangeDurable  string `xml:"exchangeDurable,attr"`
	QueueDurable     string `xml:"queueDurable,attr"`
}

type yamlFile struct {
	Defaults defaultsYAML          `yaml:"defaults"`
	Admin    adminYAML             `yaml:"admin"`
	Log      logYAML               `yaml:"log"`
	Servers  map[string]serverYAML `yaml:"servers"`
	Jobs     []jobYAML             `yaml:"jobs"`
}

type defaultsYAML struct {
	URL             string `yaml:"url,omitempty"`
	URLHost         string `yaml:"url_host,omitempty"`
	Count           int    `yaml:"count,omitempty"`
	TimeoutMS       int    `yaml:"timeout_ms,omitempty"`
	Encoding        string `yaml:"encoding,omitempty"`
	DeclareTopology bool   `yaml:"declare_topology"`
}

type adminYAML struct {
	Host   string   `yaml:"host"`
	Port   int      `yaml:"port"`
	Tokens []string `yaml:"tokens"`
}

type logYAML struct {
	Level      string         `yaml:"level"`
	FailedBody failedBodyYAML `yaml:"failed_body"`
}

type failedBodyYAML struct {
	Enabled    bool    `yaml:"enabled"`
	MaxBytes   int     `yaml:"max_bytes"`
	SampleRate float64 `yaml:"sample_rate"`
}

// job.xml has no admin auth or log settings; these placeholders match
// configs/config.example.yaml so the secret and level come from the environment.
const (
	adminTokenPlaceholder = "${ADMIN_TOKEN:-}"
	logLevelPlaceholder   = "${LOG_LEVEL:-info}"
)

type serverYAML struct {
	Host             string `yaml:"host"`
	Port             int    `yaml:"port"`
	Username         string `yaml:"username"`
	Password         string `yaml:"password"`
	VHost            string `yaml:"vhost"`
	ReconnectSeconds int    `yaml:"reconnect_seconds,omitempty"`
}

type jobYAML struct {
	Name             string `yaml:"name"`
	Server           any    `yaml:"server"` // string or []string
	Queue            string `yaml:"queue,omitempty"`
	Exchange         string `yaml:"exchange,omitempty"`
	Type             string `yaml:"type,omitempty"`
	URL              string `yaml:"url,omitempty"`
	URLHost          string `yaml:"url_host,omitempty"`
	Encoding         string `yaml:"encoding,omitempty"`
	TimeoutMS        int    `yaml:"timeout_ms,omitempty"`
	Count            int    `yaml:"count,omitempty"`
	RetryLimit       int    `yaml:"retry_limit,omitempty"`
	Prefetch         int    `yaml:"prefetch,omitempty"`
	RouteKey         string `yaml:"route_key,omitempty"`
	BlockPatterns    string `yaml:"block_patterns,omitempty"`
	DiscardMatchJobs *bool  `yaml:"discard_match_jobs,omitempty"`
	ExchangeDurable  *bool  `yaml:"exchange_durable,omitempty"`
	QueueDurable     *bool  `yaml:"queue_durable,omitempty"`
}

var (
	reXMLNS = regexp.MustCompile(`\sxmlns(:[A-Za-z0-9_]+)?="[^"]*"`)
	reXSI   = regexp.MustCompile(`\sxsi:[A-Za-z0-9_]+="[^"]*"`)
	reTagNS = regexp.MustCompile(`(</?)([A-Za-z_][\w.-]*):`)
)

// ConvertFile reads job.xml and returns YAML bytes.
func ConvertFile(path string, opt Options) ([]byte, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read xml: %w", err)
	}
	return Convert(raw, opt)
}

// Convert converts job.xml bytes to YAML config.
func Convert(raw []byte, opt Options) ([]byte, error) {
	cleaned := normalizeXML(raw)
	var doc jobsXML
	dec := xml.NewDecoder(bytes.NewReader(cleaned))
	dec.Strict = false
	if err := dec.Decode(&doc); err != nil {
		return nil, fmt.Errorf("parse xml: %w", err)
	}

	out, notes := toYAML(doc, opt)
	var buf bytes.Buffer
	buf.WriteString("# Generated from job.xml by xml2yaml. Review secrets before commit.\n")
	for _, n := range notes {
		buf.WriteString("# NOTE: " + n + "\n")
	}
	buf.WriteByte('\n')

	enc := yaml.NewEncoder(&buf)
	enc.SetIndent(2)
	if err := enc.Encode(out); err != nil {
		return nil, fmt.Errorf("encode yaml: %w", err)
	}
	_ = enc.Close()
	return buf.Bytes(), nil
}

func normalizeXML(raw []byte) []byte {
	s := string(raw)
	s = reXMLNS.ReplaceAllString(s, "")
	s = reXSI.ReplaceAllString(s, "")
	s = reTagNS.ReplaceAllString(s, "$1")
	return []byte(s)
}

func toYAML(doc jobsXML, opt Options) (yamlFile, []string) {
	notes := []string{
		"set ADMIN_TOKEN (>= 16 chars) to protect /api/*; without it the admin API is unauthenticated. For HTTPS/mTLS add admin.tls (see configs/config.example.yaml)",
	}
	out := yamlFile{
		Defaults: defaultsYAML{
			URL:             strings.TrimSpace(doc.Conf.RequestPre),
			URLHost:         strings.TrimSpace(doc.Conf.RequestHost),
			Count:           doc.Conf.RequestCount,
			TimeoutMS:       doc.Conf.RequestTimeout,
			Encoding:        strings.TrimSpace(doc.Conf.Encoding),
			DeclareTopology: true,
		},
		Admin: adminYAML{
			Host:   firstNonEmpty(strings.TrimSpace(doc.Monitor.HTTPHost), "0.0.0.0"),
			Port:   doc.Monitor.HTTPPort,
			Tokens: []string{adminTokenPlaceholder},
		},
		Log: logYAML{
			Level:      logLevelPlaceholder,
			FailedBody: failedBodyYAML{MaxBytes: 512, SampleRate: 1},
		},
		Servers: make(map[string]serverYAML, len(doc.Servers.QueueServers)),
	}
	if out.Admin.Port <= 0 {
		out.Admin.Port = 8888
	}

	for _, qs := range doc.Servers.QueueServers {
		name := strings.TrimSpace(qs.Name)
		if name == "" {
			continue
		}
		pass := qs.Password
		if opt.EnvSecrets {
			env := "SERVER_" + sanitizeEnv(name) + "_PASSWORD"
			pass = "${" + env + ":-" + qs.Password + "}"
		}
		out.Servers[name] = serverYAML{
			Host:             qs.Host,
			Port:             qs.Port,
			Username:         qs.UserName,
			Password:         pass,
			VHost:            qs.VHost,
			ReconnectSeconds: qs.RetryIntervalSeconds,
		}
		if qs.RedisHost != "" {
			notes = append(notes, fmt.Sprintf("server %q had redisHost=%q; Go version uses DLQ instead of Redis failed-job store", name, qs.RedisHost))
		}
	}

	jobs := make([]jobYAML, 0, len(doc.Jobs))
	for _, j := range doc.Jobs {
		name := strings.TrimSpace(j.Name)
		server := strings.TrimSpace(j.Server)
		if name == "" || server == "" {
			continue
		}
		yj := jobYAML{
			Name:          name,
			Server:        server,
			Queue:         strings.TrimSpace(j.Queue),
			Exchange:      strings.TrimSpace(j.Exchange),
			Type:          strings.TrimSpace(j.Type),
			URL:           strings.TrimSpace(j.URL),
			URLHost:       strings.TrimSpace(j.URLHost),
			Encoding:      strings.TrimSpace(j.Encoding),
			TimeoutMS:     j.Timeout,
			Count:         j.Count,
			RetryLimit:    j.RetryLimit,
			Prefetch:      j.Prefetch,
			RouteKey:      strings.TrimSpace(j.RouteKey),
			BlockPatterns: strings.TrimSpace(j.BlockPatterns),
		}
		if v, ok := parseBoolAttr(j.DiscardMatchJobs); ok {
			yj.DiscardMatchJobs = &v
		}
		if v, ok := parseBoolAttr(j.ExchangeDurable); ok {
			yj.ExchangeDurable = &v
		}
		if v, ok := parseBoolAttr(j.QueueDurable); ok {
			yj.QueueDurable = &v
		}
		jobs = append(jobs, yj)
	}

	if opt.MergeServers {
		before := len(jobs)
		jobs = mergeJobsByFingerprint(jobs)
		if merged := before - len(jobs); merged > 0 {
			notes = append(notes, fmt.Sprintf("merged %d duplicate job entries into multi-server lists", merged))
		}
	}
	out.Jobs = jobs
	return out, notes
}

func mergeJobsByFingerprint(jobs []jobYAML) []jobYAML {
	type group struct {
		base    jobYAML
		servers []string
	}
	order := make([]string, 0, len(jobs))
	groups := map[string]*group{}

	for _, j := range jobs {
		server, _ := j.Server.(string)
		fp := jobFingerprint(j)
		g, ok := groups[fp]
		if !ok {
			base := j
			base.Server = nil
			groups[fp] = &group{base: base, servers: []string{server}}
			order = append(order, fp)
			continue
		}
		g.servers = append(g.servers, server)
	}

	out := make([]jobYAML, 0, len(order))
	for _, fp := range order {
		g := groups[fp]
		j := g.base
		if len(g.servers) == 1 {
			j.Server = g.servers[0]
		} else {
			j.Server = append([]string(nil), g.servers...)
		}
		out = append(out, j)
	}
	return out
}

func jobFingerprint(j jobYAML) string {
	// Exclude server; include name so different jobs never merge.
	var b strings.Builder
	b.WriteString(j.Name)
	b.WriteByte(0)
	b.WriteString(j.Queue)
	b.WriteByte(0)
	b.WriteString(j.Exchange)
	b.WriteByte(0)
	b.WriteString(j.Type)
	b.WriteByte(0)
	b.WriteString(j.URL)
	b.WriteByte(0)
	b.WriteString(j.URLHost)
	b.WriteByte(0)
	b.WriteString(j.Encoding)
	b.WriteByte(0)
	fmt.Fprintf(&b, "%d\x00%d\x00%d\x00%d\x00", j.TimeoutMS, j.Count, j.RetryLimit, j.Prefetch)
	b.WriteString(j.RouteKey)
	b.WriteByte(0)
	b.WriteString(j.BlockPatterns)
	b.WriteByte(0)
	b.WriteString(boolPtrKey(j.DiscardMatchJobs))
	b.WriteByte(0)
	b.WriteString(boolPtrKey(j.ExchangeDurable))
	b.WriteByte(0)
	b.WriteString(boolPtrKey(j.QueueDurable))
	return b.String()
}

func boolPtrKey(p *bool) string {
	if p == nil {
		return ""
	}
	if *p {
		return "1"
	}
	return "0"
}

func parseBoolAttr(s string) (bool, bool) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "true", "1", "yes":
		return true, true
	case "false", "0", "no":
		return false, true
	default:
		return false, false
	}
}

func sanitizeEnv(name string) string {
	var b strings.Builder
	for _, r := range strings.ToUpper(name) {
		if (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
		} else {
			b.WriteByte('_')
		}
	}
	return b.String()
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
	}
	return ""
}
