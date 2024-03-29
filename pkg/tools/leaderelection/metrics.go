package leaderelection

import (
	"sync"
)

// This file provides abstractions for setting the provider (e.g., prometheus)
// of metrics.

type leaderMetricsAdapter interface {
	leaderOn(name string)
	leaderOff(name string)
}

// GaugeMetric represents a single numerical value that can arbitrarily go up
// and down.
type SwitchMetric interface {
	On(name string)
	Off(name string)
}

type noopMetric struct{}

func (noopMetric) On(name string)  {}
func (noopMetric) Off(name string) {}

// defaultLeaderMetrics expects the caller to lock before setting any metrics.
type defaultLeaderMetrics struct {
	// leader's value indicates if the current process is the owner of name lease
	leader SwitchMetric
}

func (m *defaultLeaderMetrics) leaderOn(name string) {
	if m == nil {
		return
	}
	m.leader.On(name)
}

func (m *defaultLeaderMetrics) leaderOff(name string) {
	if m == nil {
		return
	}
	m.leader.Off(name)
}

type noMetrics struct{}

func (noMetrics) leaderOn(name string)  {}
func (noMetrics) leaderOff(name string) {}

// MetricsProvider generates various metrics used by the leader election.
type MetricsProvider interface {
	NewLeaderMetric() SwitchMetric
}

type noopMetricsProvider struct{}

func (_ noopMetricsProvider) NewLeaderMetric() SwitchMetric {
	return noopMetric{}
}

var globalMetricsFactory = leaderMetricsFactory{
	metricsProvider: noopMetricsProvider{},
}

type leaderMetricsFactory struct {
	metricsProvider MetricsProvider

	onlyOnce sync.Once
}

func (f *leaderMetricsFactory) setProvider(mp MetricsProvider) {
	f.onlyOnce.Do(func() {
		f.metricsProvider = mp
	})
}

func (f *leaderMetricsFactory) newLeaderMetrics() leaderMetricsAdapter {
	mp := f.metricsProvider
	if mp == (noopMetricsProvider{}) {
		return noMetrics{}
	}
	return &defaultLeaderMetrics{
		leader: mp.NewLeaderMetric(),
	}
}

// SetProvider sets the metrics provider for all subsequently created work
// queues. Only the first call has an effect.
func SetProvider(metricsProvider MetricsProvider) {
	globalMetricsFactory.setProvider(metricsProvider)
}
