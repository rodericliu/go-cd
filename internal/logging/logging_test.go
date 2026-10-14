package logging

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"consumer-dispatcher/internal/config"
)

func logConfig(path string, stdout bool) config.Log {
	backups, age, compress := 1, 1, false
	return config.Log{
		Level:  "info",
		Stdout: stdout,
		File: config.LogFile{
			Path: path, MaxSizeMB: 1, MaxBackups: &backups, MaxAgeDays: &age, Compress: &compress,
		},
	}
}

func TestNew(t *testing.T) {
	tests := []struct {
		name       string
		file       bool
		stdout     bool
		wantFile   bool
		wantStdout bool
	}{
		{name: "no file logs to stdout", file: false, wantStdout: true},
		{name: "file only", file: true, wantFile: true},
		{name: "file and stdout", file: true, stdout: true, wantFile: true, wantStdout: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := ""
			if tt.file {
				path = filepath.Join(t.TempDir(), "sub", "dir", "cd.log") // parent dirs are created
			}
			var stdout bytes.Buffer
			log, closeFn, err := New(logConfig(path, tt.stdout), &stdout)
			if err != nil {
				t.Fatal(err)
			}
			log.Info("hello", "k", "v")
			log.Debug("hidden")
			if err := closeFn(); err != nil {
				t.Fatal(err)
			}

			if got := strings.Contains(stdout.String(), `"msg":"hello"`); got != tt.wantStdout {
				t.Errorf("stdout has log = %v, want %v: %q", got, tt.wantStdout, stdout.String())
			}
			if strings.Contains(stdout.String(), "hidden") {
				t.Errorf("debug line written at info level: %q", stdout.String())
			}
			if !tt.file {
				return
			}
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if got := strings.Contains(string(data), `"msg":"hello"`); got != tt.wantFile {
				t.Errorf("file has log = %v, want %v: %q", got, tt.wantFile, data)
			}
		})
	}
}

func TestNewUnwritablePath(t *testing.T) {
	blocker := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(blocker, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	// A regular file cannot be used as a directory.
	_, _, err := New(logConfig(filepath.Join(blocker, "cd.log"), false), &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "log.file.path") {
		t.Fatalf("err = %v, want log.file.path error", err)
	}
}
