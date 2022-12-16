package consumer

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

type Metrics struct {
	TotalEvents   *prometheus.CounterVec
	TotalDuration *prometheus.HistogramVec
	TotalErrors   *prometheus.CounterVec
}

var (
	once sync.Once
	metr *Metrics
)

func Instance() *Metrics {
	once.Do(func() {
		metr = &Metrics{}
		metr.initMetrics()
	})

	return metr
}

func (m *Metrics) initMetrics() {
	m.TotalEvents = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "kafka_total_events",
			Help: "кол-во прочитанных событий",
		},
		[]string{"partition", "topic"},
	)

	m.TotalErrors = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "kafka_total_errors",
			Help: "кол-во ошибок",
		},
		[]string{"partition", "topic", "error"},
	)

	m.TotalDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "kafka_event_duration",
			Help:    "время обработки событий",
			Buckets: prometheus.LinearBuckets(0.020, 0.020, 5),
		},
		[]string{"partition", "topic"},
	)

	prometheus.MustRegister(m.TotalEvents)
	prometheus.MustRegister(m.TotalErrors)
	prometheus.MustRegister(m.TotalDuration)
}
