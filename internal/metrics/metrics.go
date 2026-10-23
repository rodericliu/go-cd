package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	Processed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "cd_jobs_processed_total",
		Help: "Jobs processed by outcome",
	}, []string{"job", "outcome"}) // success | failed | discarded | dlq

	Duration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "cd_job_duration_seconds",
		Help:    "HTTP forward duration",
		Buckets: prometheus.DefBuckets,
	}, []string{"job"})

	Workers = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "cd_workers",
		Help: "Active workers per job",
	}, []string{"job"})

	QueueReady = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "cd_queue_ready_messages",
		Help: "Ready messages in the job queue, as last seen by the autoscaler",
	}, []string{"job"})

	AutoscaleDesired = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "cd_autoscale_desired_workers",
		Help: "Workers the autoscaler computed from the backlog, within min/max",
	}, []string{"job"})

	InFlight = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "cd_inflight_jobs",
		Help: "Messages currently being processed",
	}, []string{"job"})
)
