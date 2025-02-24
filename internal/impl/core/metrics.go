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
	"fmt"
	"sync"
	"sync/atomic"

	"solace.dev/go/messaging/internal/ccsmp"
	"solace.dev/go/messaging/internal/impl/logging"
	"solace.dev/go/messaging/pkg/solace/metrics"
)

var rxMetrics = map[metrics.Metric]ccsmp.SolClientStatsRX{
	metrics.DirectBytesReceived:                       ccsmp.SolClientStatsRXDirectBytes,
	metrics.DirectMessagesReceived:                    ccsmp.SolClientStatsRXDirectMsgs,
	metrics.BrokerDiscardNotificationsReceived:        ccsmp.SolClientStatsRXDiscardInd,
	metrics.UnknownParameterMessagesDiscarded:         ccsmp.SolClientStatsRXDiscardSmfUnknownElement,
	metrics.TooBigMessagesDiscarded:                   ccsmp.SolClientStatsRXDiscardMsgTooBig,
	metrics.PersistentAcknowledgeSent:                 ccsmp.SolClientStatsRXAcked,
	metrics.PersistentDuplicateMessagesDiscarded:      ccsmp.SolClientStatsRXDiscardDuplicate,
	metrics.PersistentNoMatchingFlowMessagesDiscarded: ccsmp.SolClientStatsRXDiscardNoMatchingFlow,
	metrics.PersistentOutOfOrderMessagesDiscarded:     ccsmp.SolClientStatsRXDiscardOutoforder,
	metrics.PersistentBytesReceived:                   ccsmp.SolClientStatsRXPersistentBytes,
	metrics.PersistentMessagesReceived:                ccsmp.SolClientStatsRXPersistentMsgs,
	metrics.ControlMessagesReceived:                   ccsmp.SolClientStatsRXCtlMsgs,
	metrics.ControlBytesReceived:                      ccsmp.SolClientStatsRXCtlBytes,
	metrics.TotalBytesReceived:                        ccsmp.SolClientStatsRXTotalDataBytes,
	metrics.TotalMessagesReceived:                     ccsmp.SolClientStatsRXTotalDataMsgs,
	metrics.CompressedBytesReceived:                   ccsmp.SolClientStatsRXCompressedBytes,
	metrics.PersistentMessagesAccepted:                ccsmp.SolClientStatsRXSettleAccepted,
	metrics.PersistentMessagesFailed:                  ccsmp.SolClientStatsRXSettleFailed,
	metrics.PersistentMessagesRejected:                ccsmp.SolClientStatsRXSettleRejected,
}

var txMetrics = map[metrics.Metric]ccsmp.SolClientStatsTX{
	metrics.TotalBytesSent:                   ccsmp.SolClientStatsTXTotalDataBytes,
	metrics.TotalMessagesSent:                ccsmp.SolClientStatsTXTotalDataMsgs,
	metrics.DirectBytesSent:                  ccsmp.SolClientStatsTXDirectBytes,
	metrics.DirectMessagesSent:               ccsmp.SolClientStatsTXDirectMsgs,
	metrics.PersistentBytesSent:              ccsmp.SolClientStatsTXPersistentBytes,
	metrics.PersistentMessagesSent:           ccsmp.SolClientStatsTXPersistentMsgs,
	metrics.PersistentBytesRedelivered:       ccsmp.SolClientStatsTXPersistentBytesRedelivered,
	metrics.PersistentMessagesRedelivered:    ccsmp.SolClientStatsTXPersistentRedelivered,
	metrics.PublisherAcknowledgementReceived: ccsmp.SolClientStatsTXAcksRxed,
	metrics.PublisherWindowClosed:            ccsmp.SolClientStatsTXWindowClose,
	metrics.PublisherAcknowledgementTimeouts: ccsmp.SolClientStatsTXAckTimeout,
	metrics.ControlMessagesSent:              ccsmp.SolClientStatsTXCtlMsgs,
	metrics.ControlBytesSent:                 ccsmp.SolClientStatsTXCtlBytes,
	metrics.ConnectionAttempts:               ccsmp.SolClientStatsTXTotalConnectionAttempts,
	metrics.PublishedMessagesAcknowledged:    ccsmp.SolClientStatsTXGuaranteedMsgsSentConfirmed,
	metrics.PublishMessagesDiscarded:         ccsmp.SolClientStatsTXDiscardChannelError,
	metrics.PublisherWouldBlock:              ccsmp.SolClientStatsTXWouldBlock,
}

var clientMetrics = map[metrics.Metric]NextGenMetric{
	metrics.ReceivedMessagesTerminationDiscarded:  MetricReceivedMessagesTerminationDiscarded,
	metrics.ReceivedMessagesBackpressureDiscarded: MetricReceivedMessagesBackpressureDiscarded,
	metrics.PublishMessagesTerminationDiscarded:   MetricPublishMessagesTerminationDiscarded,
	metrics.PublishMessagesBackpressureDiscarded:  MetricPublishMessagesBackpressureDiscarded,
	metrics.InternalDiscardNotifications:          MetricInternalDiscardNotifications,
}

// NextGenMetric structure
type NextGenMetric int

const (
	// MetricReceivedMessagesTerminationDiscarded initialized
	MetricReceivedMessagesTerminationDiscarded NextGenMetric = iota

	// MetricReceivedMessagesBackpressureDiscarded initialized
	MetricReceivedMessagesBackpressureDiscarded NextGenMetric = iota

	// MetricPublishMessagesTerminationDiscarded initialized
	MetricPublishMessagesTerminationDiscarded NextGenMetric = iota

	// MetricPublishMessagesBackpressureDiscarded initialized
	MetricPublishMessagesBackpressureDiscarded NextGenMetric = iota

	// MetricInternalDiscardNotifications initialized
	MetricInternalDiscardNotifications NextGenMetric = iota

	// metricCount initialized
	metricCount int = iota
)

// Metrics interface
type Metrics interface {
	GetStat(metric metrics.Metric) uint64
	IncrementMetric(metric NextGenMetric, amount uint64)
	ResetStats()
}

// Implementation
type ccsmpBackedMetrics struct {
	session       *ccsmp.SolClientSession
	metrics       []uint64
	duplicateAcks uint64

	metricLock        sync.RWMutex
	capturedTxMetrics map[ccsmp.SolClientStatsTX]uint64
	capturedRxMetrics map[ccsmp.SolClientStatsRX]uint64
}

func newCcsmpMetrics(session *ccsmp.SolClientSession) *ccsmpBackedMetrics {
	return &ccsmpBackedMetrics{
		metrics:       make([]uint64, metricCount),
		duplicateAcks: uint64(0), // used to track duplicate acks count (auto-acks)
		session:       session,
	}
}

func (backedMetrics *ccsmpBackedMetrics) terminate() {
	backedMetrics.metricLock.Lock()
	defer backedMetrics.metricLock.Unlock()
	backedMetrics.captureTXStats()
	backedMetrics.captureRXStats()
}

func (backedMetrics *ccsmpBackedMetrics) captureTXStats() {
	if backedMetrics.capturedTxMetrics != nil {
		return
	}
	backedMetrics.capturedTxMetrics = make(map[ccsmp.SolClientStatsTX]uint64)
	for _, txStat := range txMetrics {
		backedMetrics.capturedTxMetrics[txStat] = backedMetrics.session.SolClientSessionGetTXStat(txStat)
	}
}

func (backedMetrics *ccsmpBackedMetrics) captureRXStats() {
	if backedMetrics.capturedRxMetrics != nil {
		return
	}
	backedMetrics.capturedRxMetrics = make(map[ccsmp.SolClientStatsRX]uint64)
	for _, rxStat := range rxMetrics {
		backedMetrics.capturedRxMetrics[rxStat] = backedMetrics.session.SolClientSessionGetRXStat(rxStat)
	}
}

func (backedMetrics *ccsmpBackedMetrics) getTXStat(stat ccsmp.SolClientStatsTX) uint64 {
	backedMetrics.metricLock.RLock()
	defer backedMetrics.metricLock.RUnlock()
	if backedMetrics.capturedTxMetrics != nil {
		return backedMetrics.capturedTxMetrics[stat]
	}
	return backedMetrics.session.SolClientSessionGetTXStat(stat)
}

func (backedMetrics *ccsmpBackedMetrics) getRXStat(stat ccsmp.SolClientStatsRX) uint64 {
	backedMetrics.metricLock.RLock()
	defer backedMetrics.metricLock.RUnlock()
	if backedMetrics.capturedRxMetrics != nil {
		return backedMetrics.capturedRxMetrics[stat]
	}
	return backedMetrics.session.SolClientSessionGetRXStat(stat)
}

func (backedMetrics *ccsmpBackedMetrics) getNextGenStat(metric NextGenMetric) uint64 {
	return atomic.LoadUint64(&backedMetrics.metrics[metric])
}

func (backedMetrics *ccsmpBackedMetrics) GetStat(metric metrics.Metric) uint64 {
	if rxMetric, ok := rxMetrics[metric]; ok {
		// check for duplicate counts due to auto-acks and remove from final result
		rxStat := backedMetrics.getRXStat(rxMetric)
		duplicateAck := atomic.LoadUint64(&backedMetrics.duplicateAcks)
		// take the difference and retrun as the settled accepted metric
		if duplicateAck > 0 && metric == metrics.PersistentMessagesAccepted {
			return rxStat - duplicateAck
		}
		return rxStat // return other types of metrics as is
	} else if txMetric, ok := txMetrics[metric]; ok {
		return backedMetrics.getTXStat(txMetric)
	} else if clientMetric, ok := clientMetrics[metric]; ok {
		return backedMetrics.getNextGenStat(clientMetric)
	}
	logging.Default.Warning("Could not find mapping for metric with ID " + fmt.Sprint(metric))
	return 0

}

func (backedMetrics *ccsmpBackedMetrics) ResetStats() {
	for i := 0; i < metricCount; i++ {
		atomic.StoreUint64(&backedMetrics.metrics[i], 0)
	}
	atomic.StoreUint64(&backedMetrics.duplicateAcks, 0) // reset this duplicate acks counter to zero
	backedMetrics.resetNativeStats()
}

func (backedMetrics *ccsmpBackedMetrics) resetNativeStats() {
	backedMetrics.metricLock.Lock()
	defer backedMetrics.metricLock.Unlock()
	if backedMetrics.capturedRxMetrics != nil {
		for key := range backedMetrics.capturedRxMetrics {
			backedMetrics.capturedRxMetrics[key] = 0
		}
		for key := range backedMetrics.capturedTxMetrics {
			backedMetrics.capturedTxMetrics[key] = 0
		}
	} else {
		errorInfo := backedMetrics.session.SolClientSessionClearStats()
		if errorInfo != nil {
			logging.Default.Warning("Could not reset metrics: " + errorInfo.String())
		}
	}
}

func (backedMetrics *ccsmpBackedMetrics) IncrementMetric(metric NextGenMetric, amount uint64) {
	atomic.AddUint64(&backedMetrics.metrics[metric], amount)
}

func (backedMetrics *ccsmpBackedMetrics) incrementDuplicateAckCount() {
	atomic.AddUint64(&backedMetrics.duplicateAcks, uint64(1))
}
