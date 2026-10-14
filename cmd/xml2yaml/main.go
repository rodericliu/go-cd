package main

import (
	"flag"
	"fmt"
	"os"

	"consumer-dispatcher/internal/xmlconv"
)

func main() {
	in := flag.String("in", "", "path to job.xml (required)")
	out := flag.String("out", "", "output YAML path (default: stdout)")
	merge := flag.Bool("merge", true, "merge jobs with identical config into server lists")
	envSecrets := flag.Bool("env-secrets", false, "rewrite passwords as ${SERVER_<NAME>_PASSWORD:-plaintext}")
	flag.Parse()

	if *in == "" {
		fmt.Fprintln(os.Stderr, "usage: xml2yaml -in job.xml [-out config.yaml] [-merge=true] [-env-secrets]")
		os.Exit(2)
	}

	yamlBytes, err := xmlconv.ConvertFile(*in, xmlconv.Options{
		MergeServers: *merge,
		EnvSecrets:   *envSecrets,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "xml2yaml: %v\n", err)
		os.Exit(1)
	}

	if *out == "" {
		_, _ = os.Stdout.Write(yamlBytes)
		return
	}
	if err := os.WriteFile(*out, yamlBytes, 0o644); err != nil {
		fmt.Fprintf(os.Stderr, "xml2yaml: write %s: %v\n", *out, err)
		os.Exit(1)
	}
	fmt.Fprintf(os.Stderr, "wrote %s (%d bytes)\n", *out, len(yamlBytes))
}
