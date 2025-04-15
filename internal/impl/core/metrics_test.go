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

package core

import (
	"sync/atomic"
	"testing"
)

func TestSolClientMetrics(t *testing.T) {
	metrics := []NextGenMetric{
		MetricPublishMessagesBackpressureDiscarded,
		MetricPublishMessagesTerminationDiscarded,
		MetricReceivedMessagesBackpressureDiscarded,
		MetricReceivedMessagesTerminationDiscarded,
		MetricInternalDiscardNotifications,
	}
	for _, metric := range metrics {
		metricsImpl := newCcsmpMetrics(nil)
		if metricsImpl.getNextGenStat(metric) != 0 {
			t.Error("Expected zero state of metric to be 0")
		}
		metricsImpl.IncrementMetric(metric, 10)
		if metricsImpl.getNextGenStat(metric) != 10 {
			t.Error("Expected incremented metric to be 10")
		}
	}
}

func TestIncrementDuplicateAckCount(t *testing.T) {
	metricsImpl := newCcsmpMetrics(nil)
	duplicateAck := atomic.LoadUint64(&metricsImpl.duplicateAcks)
	if duplicateAck != 0 {
		t.Error("Expected zero state of duplicate Acks counter to be 0")
	}
	metricsImpl.incrementDuplicateAckCount()
	duplicateAck = atomic.LoadUint64(&metricsImpl.duplicateAcks)
	if duplicateAck != 1 {
		t.Error("Expected duplicate Acks to be 1")
	}
	for i := 0; i < 10; i++ {
		// call 10 times
		metricsImpl.incrementDuplicateAckCount()
	}
	duplicateAck = atomic.LoadUint64(&metricsImpl.duplicateAcks)
	if duplicateAck != 11 {
		t.Error("Expected duplicate Acks to be 11")
	}
}
