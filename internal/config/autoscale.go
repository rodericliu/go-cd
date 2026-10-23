package config

import (
	"errors"
	"fmt"
	"time"
)

// MaxWorkers caps the workers of one job, set by count, autoscale or the admin API.
const MaxWorkers = 256

const (
	defaultAutoscaleMin      = 1
	defaultMessagesPerWorker = 100
	defaultAutoscaleInterval = 10
	defaultScaleDownDelay    = 120
	defaultMaxFailureRatio   = 0.5
)

// AutoscaleFile is the YAML form of autoscale settings. Fields left unset on a
// job inherit from defaults.autoscale, then from the built-in defaults.
type AutoscaleFile struct {
	Enabled               *bool    `yaml:"enabled"`
	Min                   *int     `yaml:"min"`
	Max                   *int     `yaml:"max"`
	MessagesPerWorker     *int     `yaml:"messages_per_worker"`
	IntervalSeconds       *int     `yaml:"interval_seconds"`
	ScaleDownDelaySeconds *int     `yaml:"scale_down_delay_seconds"`
	MaxStepUp             *int     `yaml:"max_step_up"`
	MaxFailureRatio       *float64 `yaml:"max_failure_ratio"`
}

// Autoscale sizes a job's workers from the ready messages of its queue:
// ceil(ready / MessagesPerWorker), kept within [Min, Max].
type Autoscale struct {
	Enabled           bool
	Min               int
	Max               int
	MessagesPerWorker int
	Interval          time.Duration
	// ScaleDownDelay is how long the backlog must stay low before workers
	// are removed; scaling up is immediate.
	ScaleDownDelay time.Duration
	// MaxStepUp limits workers added per evaluation (0 = no limit).
	MaxStepUp int
	// MaxFailureRatio blocks scaling up while the share of failed messages
	// in the last interval is above it, so a struggling consumer is not
	// loaded further. 1 disables the check.
	MaxFailureRatio float64
}

// DefaultAutoscale returns the settings of a job that configures no autoscale.
func DefaultAutoscale() Autoscale {
	return resolveAutoscale(AutoscaleFile{}, AutoscaleFile{})
}

func resolveAutoscale(job, def AutoscaleFile) Autoscale {
	return Autoscale{
		Enabled:           pick(job.Enabled, def.Enabled, false),
		Min:               pick(job.Min, def.Min, defaultAutoscaleMin),
		Max:               pick(job.Max, def.Max, 0),
		MessagesPerWorker: pick(job.MessagesPerWorker, def.MessagesPerWorker, defaultMessagesPerWorker),
		Interval:          time.Duration(pick(job.IntervalSeconds, def.IntervalSeconds, defaultAutoscaleInterval)) * time.Second,
		ScaleDownDelay:    time.Duration(pick(job.ScaleDownDelaySeconds, def.ScaleDownDelaySeconds, defaultScaleDownDelay)) * time.Second,
		MaxStepUp:         pick(job.MaxStepUp, def.MaxStepUp, 0),
		MaxFailureRatio:   pick(job.MaxFailureRatio, def.MaxFailureRatio, defaultMaxFailureRatio),
	}
}

// pick returns the first non-nil value, or fallback.
func pick[T any](job, def *T, fallback T) T {
	if job != nil {
		return *job
	}
	if def != nil {
		return *def
	}
	return fallback
}

// Validate checks the ranges. Max is only required while autoscale is
// enabled, so settings can be kept around disabled.
func (a Autoscale) Validate() error {
	switch {
	case a.Min < 0 || a.Min > MaxWorkers:
		return fmt.Errorf("autoscale.min must be between 0 and %d", MaxWorkers)
	case a.Max < 0 || a.Max > MaxWorkers:
		return fmt.Errorf("autoscale.max must be between 0 and %d", MaxWorkers)
	case a.Enabled && a.Max < 1:
		return errors.New("autoscale.max is required (>= 1) when autoscale is enabled")
	case a.Enabled && a.Min > a.Max:
		return errors.New("autoscale.min must be <= autoscale.max")
	case a.MessagesPerWorker < 1:
		return errors.New("autoscale.messages_per_worker must be >= 1")
	case a.Interval < time.Second:
		return errors.New("autoscale.interval_seconds must be >= 1")
	case a.ScaleDownDelay < 0:
		return errors.New("autoscale.scale_down_delay_seconds must be >= 0")
	case a.MaxStepUp < 0:
		return errors.New("autoscale.max_step_up must be >= 0")
	case !(a.MaxFailureRatio > 0 && a.MaxFailureRatio <= 1):
		return errors.New("autoscale.max_failure_ratio must be in (0, 1]")
	}
	return nil
}

// InitialWorkers is the worker count a job starts with: count, kept within
// [min, max] when autoscale is enabled.
func (j ResolvedJob) InitialWorkers() int {
	if !j.Autoscale.Enabled {
		return j.Count
	}
	return min(max(j.Count, j.Autoscale.Min), j.Autoscale.Max)
}
