// This file provides abstractions for setting the provider (e.g., prometheus)
// of metrics.

package cache

import (
	"sync"
)

// GaugeMetric represents a single numerical value that can arbitrarily go up
// and down.
type GaugeMetric interface {
	Set(float64)
}

// CounterMetric represents a single numerical value that only ever
// goes up.
type CounterMetric interface {
	Inc()
}

// SummaryMetric captures individual observations.
type SummaryMetric interface {
	Observe(float64)
}

type noopMetric struct{}

func (noopMetric) Inc()            {}
func (noopMetric) Dec()            {}
func (noopMetric) Observe(float64) {}
func (noopMetric) Set(float64)     {}

// MetricsProvider generates various metrics used by the reflector.
type MetricsProvider interface {
	NewListsMetric(name string) CounterMetric
	NewListDurationMetric(name string) SummaryMetric
	NewItemsInListMetric(name string) SummaryMetric

	NewWatchesMetric(name string) CounterMetric
	NewShortWatchesMetric(name string) CounterMetric
	NewWatchDurationMetric(name string) SummaryMetric
	NewItemsInWatchMetric(name string) SummaryMetric

	NewLastResourceVersionMetric(name string) GaugeMetric
}

type noopMetricsProvider struct{}

func (noopMetricsProvider) NewListsMetric(name string) CounterMetric         { return noopMetric{} }
func (noopMetricsProvider) NewListDurationMetric(name string) SummaryMetric  { return noopMetric{} }
func (noopMetricsProvider) NewItemsInListMetric(name string) SummaryMetric   { return noopMetric{} }
func (noopMetricsProvider) NewWatchesMetric(name string) CounterMetric       { return noopMetric{} }
func (noopMetricsProvider) NewShortWatchesMetric(name string) CounterMetric  { return noopMetric{} }
func (noopMetricsProvider) NewWatchDurationMetric(name string) SummaryMetric { return noopMetric{} }
func (noopMetricsProvider) NewItemsInWatchMetric(name string) SummaryMetric  { return noopMetric{} }
func (noopMetricsProvider) NewLastResourceVersionMetric(name string) GaugeMetric {
	return noopMetric{}
}

var metricsFactory = struct {
	metricsProvider MetricsProvider
	setProviders    sync.Once
}{
	metricsProvider: noopMetricsProvider{},
}

// SetReflectorMetricsProvider sets the metrics provider
func SetReflectorMetricsProvider(metricsProvider MetricsProvider) {
	metricsFactory.setProviders.Do(func() {
		metricsFactory.metricsProvider = metricsProvider
	})
}
