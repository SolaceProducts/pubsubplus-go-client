// pubsubplus-go-client
//
// Copyright 2021-2025 Solace Corporation. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package impl

import (
	"bytes"
	"os"
	"testing"

	"solace.dev/go/messaging/internal/impl/core"
	"solace.dev/go/messaging/internal/impl/logging"
	"solace.dev/go/messaging/pkg/solace/metrics"
)

func TestClientMessagingStat(t *testing.T) {
	internalMetricsHandle := &dummyMetricsImpl{}
	metricImpl := &metricsImpl{internalMetricsHandle}
	ret := uint64(1234)
	stat := metrics.ReceivedMessagesBackpressureDiscarded
	internalMetricsHandle.getStat = func(passedStat metrics.Metric) uint64 {
		return ret
	}
	actual := metricImpl.GetValue(stat)
	if ret != actual {
		t.Errorf("expected to get %d back, got %d", ret, actual)
	}
}

func TestStatsReset(t *testing.T) {
	internalMetricsHandle := &dummyMetricsImpl{}
	metricImpl := &metricsImpl{internalMetricsHandle}
	called := false
	internalMetricsHandle.resetStats = func() {
		called = true
	}
	metricImpl.Reset()
	if !called {
		t.Error("metrics handle reset stats was not called on call to reset")
	}
}

func TestGetStatsNoError(t *testing.T) {
	byteBuffer := &bytes.Buffer{}
	logging.Default.SetOutput(byteBuffer)
	defer logging.Default.SetOutput(os.Stderr)
	metricImpl := &metricsImpl{&dummyMetricsImpl{}}
	for i := 0; i < metrics.MetricCount; i++ {
		metricImpl.GetValue(metrics.Metric(i))
	}
	if byteBuffer.Len() > 0 {
		t.Error("did not expect to get a log message when retrieving the stat, this is caused by an unmapped stat")
		t.Error(byteBuffer.String())
	}
}

type dummyMetricsImpl struct {
	getStat         func(metric metrics.Metric) uint64
	incrementMetric func(metric core.NextGenMetric, amount uint64)
	resetStats      func()
}

func (dummy *dummyMetricsImpl) GetStat(metric metrics.Metric) uint64 {
	if dummy.getStat != nil {
		return dummy.getStat(metric)
	}
	return 0
}

func (dummy *dummyMetricsImpl) IncrementMetric(metric core.NextGenMetric, amount uint64) {
	if dummy.incrementMetric != nil {
		dummy.incrementMetric(metric, amount)
	}
}

func (dummy *dummyMetricsImpl) ResetStats() {
	if dummy.resetStats != nil {
		dummy.resetStats()
	}
}
