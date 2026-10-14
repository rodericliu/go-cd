// Package logging builds the process logger from config.
package logging

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"

	"gopkg.in/natefinch/lumberjack.v2"

	"consumer-dispatcher/internal/config"
)

// New returns a JSON logger for cfg and a close function that releases the
// log file. Without a file, logs go to stdout. Pass a *slog.LevelVar as level
// to change the level at runtime; nil uses cfg.Level.
func New(cfg config.Log, level slog.Leveler, stdout io.Writer) (*slog.Logger, func() error, error) {
	w, closeFn, err := Writer(cfg, stdout)
	if err != nil {
		return nil, nil, err
	}
	if level == nil {
		level = cfg.SlogLevel()
	}
	return slog.New(slog.NewJSONHandler(w, &slog.HandlerOptions{Level: level})), closeFn, nil
}

// Writer returns where logs are written according to cfg.
func Writer(cfg config.Log, stdout io.Writer) (io.Writer, func() error, error) {
	if !cfg.File.Enabled() {
		return stdout, func() error { return nil }, nil
	}
	f := cfg.File
	if err := checkWritable(f.Path); err != nil {
		return nil, nil, err
	}
	lj := &lumberjack.Logger{
		Filename:   f.Path,
		MaxSize:    f.MaxSizeMB,
		MaxBackups: *f.MaxBackups,
		MaxAge:     *f.MaxAgeDays,
		Compress:   *f.Compress,
		LocalTime:  f.LocalTime,
	}
	if cfg.Stdout {
		return io.MultiWriter(lj, stdout), lj.Close, nil
	}
	return lj, lj.Close, nil
}

// checkWritable fails fast at startup: lumberjack only opens the file on the
// first write, so a bad path would otherwise lose logs silently.
func checkWritable(path string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("log.file.path: %w", err)
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o640)
	if err != nil {
		return fmt.Errorf("log.file.path: %w", err)
	}
	return f.Close()
}
