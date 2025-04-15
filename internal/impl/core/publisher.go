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
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"unsafe"

	"solace.dev/go/messaging/internal/ccsmp"
	"solace.dev/go/messaging/internal/impl/logging"
)

// Publisher interface
type Publisher interface {
	// Publish pulishes a message in the form of a SolClientPublishable. Returns any error info from underlying send.
	Publish(message Publishable) ErrorInfo
	// Events returns SolClientEvents
	Events() Events
	// AwaitWritable awaits a writable message. Throws an error if interrupted for termination
	AwaitWritable(terminateSignal chan struct{}) error
	// TaskQueue gets the task queue that can be used to push tasks
	// TaskQueue may be a 0 length queue such that each task is transactioned, ie. no tasks sit in flux
	TaskQueue() chan SendTask
	// checks if the internal publisher is running
	IsRunning() bool
	// Increments a core metric
	IncrementMetric(metric NextGenMetric, amount uint64)
	// Acknowledgements returns the acknowledgement handler
	Acknowledgements() Acknowledgements
	// Requestor returns the reply handler manager
	Requestor() Requestor
}

// Requestor interface
// User for making requests in the request reply model
type Requestor interface {
	// CreateReplyToTopic return a replyto topic created from a publisher id
	CreateReplyToTopic(publisherID string) string
	// registers callback for response message for a returned replyTo topic with correlation id generator
	AddRequestorReplyHandler(replyHandler RequestorReplyHandler) (string, func() (messageId uint64, correlationId string), ErrorInfo)
	// deregisters replyHandler from core publisher
	RemoveRequestorReplyHandler(replyToTopic string) ErrorInfo
}

// Repliable interface, this is Repliable alias, repliable are structs that are received as a part of a reply not a struct that can send a reply
type Repliable = ccsmp.SolClientMessagePt

type RequestorReplyHandler func(message Repliable, correlationId string) bool

var RequestCorrelationPrefix = ccsmp.SolClientGoPropCorrelationPrefix

// Acknowledgements interface
type Acknowledgements interface {
	// Registers a callback for correlation
	AddAcknowledgementHandler(ackHandler AcknowledgementHandler) (uint64, func() (messageId uint64, correlationTag []byte))
	// Deregisters the callback for correlation
	RemoveAcknowledgementHandler(pubID uint64)
}

// AcknowledgementHandler defined
type AcknowledgementHandler func(correlationMsgId uint64, persisted bool, err error)

// SendTask defined
type SendTask func()

// Publishable defined
type Publishable = ccsmp.SolClientMessagePt

// Implementation
type ccsmpReplyCorrelation struct {
	handler      RequestorReplyHandler
	replyToTopic string
}

type RequestorReplyCorrelationEntry = *ccsmpReplyCorrelation

type ccsmpBackedPublisher struct {
	events  *ccsmpBackedEvents
	metrics *ccsmpBackedMetrics
	session *ccsmp.SolClientSession

	taskQueue           chan SendTask
	termination         chan struct{}
	terminationComplete chan struct{}
	canSend             chan bool

	canSendEventID uint

	isRunning int32

	acknowledgementEventID uint
	rejectedEventID        uint

	acknowledgementHandlerID uint64
	acknowledgementMap       sync.Map

	// requestor fields
	rxLock              sync.RWMutex
	replyToPrefix       string
	replyDispatchID     uint64
	publisherRxReplyMap map[uintptr]RequestorReplyCorrelationEntry
}

func newCcsmpPublisher(session *ccsmp.SolClientSession, events *ccsmpBackedEvents, metrics *ccsmpBackedMetrics) *ccsmpBackedPublisher {
	publisher := &ccsmpBackedPublisher{}
	publisher.events = events
	publisher.metrics = metrics
	publisher.session = session
	publisher.taskQueue = make(chan SendTask)
	publisher.termination = make(chan struct{})
	publisher.terminationComplete = make(chan struct{})
	publisher.canSend = make(chan bool, 1)
	publisher.isRunning = 0
	publisher.replyDispatchID = 0
	publisher.replyToPrefix = ""
	publisher.publisherRxReplyMap = make(map[uintptr]RequestorReplyCorrelationEntry)
	return publisher
}

func (publisher *ccsmpBackedPublisher) Publish(message Publishable) ErrorInfo {
	return publisher.session.SolClientSessionPublish(ccsmp.SolClientMessagePt(message))
}

func (publisher *ccsmpBackedPublisher) Requestor() Requestor {
	return publisher
}

// Requestor functions

func (publisher *ccsmpBackedPublisher) CreateReplyToTopic(pubID string) string {
	return publisher.replyToPrefix + pubID
}

func (publisher *ccsmpBackedPublisher) AddRequestorReplyHandler(replyHandler RequestorReplyHandler) (string, func() (messageID uint64, correlationID string), ErrorInfo) {
	// create reply to topic
	// add local dispatch only subscription for reply to topic, note subscribe with local dispatch only is not asynchronous
	// create correlationId generator

	// create reply to topic
	// create unique id to incorperate into replyTo topic
	pubID := atomic.AddUint64(&publisher.replyDispatchID, 1)
	replyToTopic := fmt.Sprintf("%s%016X", publisher.replyToPrefix, pubID)

	// add local dispatch only subscription after adding entry into publisherRxReplyMap to handle reply messages

	// create dispatch entry
	dispatchPointer := ccsmp.NewSessionReplyDispatch(pubID)
	replyEntry := &ccsmpReplyCorrelation{
		handler:      replyHandler,
		replyToTopic: replyToTopic,
	}

	// add dispatch index into rx map
	publisher.rxLock.Lock()

	publisher.publisherRxReplyMap[dispatchPointer] = replyEntry

	publisher.rxLock.Unlock()

	// subscribe using local dispatch only
	errorInfo := publisher.session.SolClientSessionSubscribeReplyTopic(replyToTopic, dispatchPointer, 0)

	// handle subscription error
	if errorInfo != nil {
		// cleanup rx map entry
		publisher.rxLock.Lock()

		delete(publisher.publisherRxReplyMap, dispatchPointer)

		publisher.rxLock.Unlock()

		return "", nil, errorInfo
	}
	// return reply to topic and correlation id generator
	var messageID uint64
	return replyToTopic, func() (nextID uint64, correlationID string) {
		nextID = atomic.AddUint64(&messageID, 1)
		// use correlation prefix independent to avoid ccsmp reply message dispatch discards
		// note the ccsmp prefix is "#SOL"
		correlationID = fmt.Sprintf("%s%016X", RequestCorrelationPrefix, nextID)
		return nextID, correlationID
	}, nil
}

func (publisher *ccsmpBackedPublisher) RemoveRequestorReplyHandler(replyToTopic string) ErrorInfo {
	// convert replyToTopic into index for rx map
	// unsubscribe from local dispatch only replyto subscription
	// remove callback from rx map

	// convert replyToTopic into index for rx map
	// split into parts
	replyTopicParts := strings.Split(replyToTopic, "/")
	// get last part with is the pubID
	pubIDHex := replyTopicParts[len(replyTopicParts)-1]
	// parse into uint64
	pubID, err := strconv.ParseUint(pubIDHex, 16, 64)
	if err != nil {
		return nil
	}
	// convert pubID into pubIndex by creating dispatch used to unsubscribe
	pubIndex := ccsmp.NewSessionReplyDispatch(pubID)

	// call unsubscribe on reply to topic to halt messaging
	errorInfo := publisher.session.SolClientSessionUnsubscribeReplyTopic(replyToTopic, pubIndex, 0)

	if errorInfo != nil {
		return errorInfo
	}

	// remove entry rx map
	publisher.rxLock.Lock()

	delete(publisher.publisherRxReplyMap, pubIndex)

	publisher.rxLock.Unlock()

	return nil
}

func (publisher *ccsmpBackedPublisher) onReplyMessage(msgP Repliable, userP unsafe.Pointer, correlationID string) bool {
	publisher.rxLock.RLock()

	entry, ok := publisher.publisherRxReplyMap[uintptr(userP)]
	if !ok {
		if logging.Default.IsDebugEnabled() {
			logging.Default.Debug(fmt.Sprintf("reply callback called but no reply function is registered for user pointer %v", userP))
		}
		return false
	}
	callback := entry.handler
	publisher.rxLock.RUnlock()
	return callback(msgP, correlationID)
}

func (publisher *ccsmpBackedPublisher) startRequestor() error {
	// get reply to prefix
	var errInfo ErrorInfo
	if publisher.replyToPrefix, errInfo = publisher.session.SolClientSessionGetP2PTopicPrefix(); errInfo != nil {
		return fmt.Errorf("error get client p2p inbox topic prefix. Error:%v", errInfo.String())
	}
	// register with session for reply message callback
	err := publisher.session.SetReplyMessageCallback(func(msgP ccsmp.SolClientMessagePt, userP unsafe.Pointer, correlationID string) bool {
		return publisher.onReplyMessage(msgP, userP, correlationID)
	})
	return err
}

func (publisher *ccsmpBackedPublisher) terminateRequestor() {
	// first deregister from session for reply mesasge callbacks
	publisher.session.SetReplyMessageCallback(nil)

	// cleanup the rx reply map
	publisher.rxLock.Lock()

	// exact entry list to unsubscribe after mutx unlock
	unsubIDList := make([]uint64, len(publisher.publisherRxReplyMap))
	unsubEntryList := make([]RequestorReplyCorrelationEntry, len(publisher.publisherRxReplyMap))
	index := 0

	for id, entry := range publisher.publisherRxReplyMap {
		unsubEntryList[index] = entry
		unsubIDList[index] = uint64(id)
		delete(publisher.publisherRxReplyMap, id)
		index++
	}

	publisher.rxLock.Unlock()

	// call unsubscribe for any outstanding publisher reply to topics
	// note this shuold only occur if there is no call to RemoveRequestorReplyHandler
	for index = 0; index < len(unsubIDList); index++ {
		dispatchID := ccsmp.NewSessionReplyDispatch(unsubIDList[index])
		publisher.session.SolClientSessionUnsubscribeReplyTopic(unsubEntryList[index].replyToTopic, dispatchID, 0)
	}

}

func (publisher *ccsmpBackedPublisher) Events() Events {
	return publisher.events
}

func (publisher *ccsmpBackedPublisher) AwaitWritable(terminateSignal chan struct{}) error {
	select {
	case result := <-publisher.canSend:
		if !result {
			return fmt.Errorf("wait for can send interrupted")
		}
		return nil
	case <-terminateSignal:
		return fmt.Errorf("received terminate signal, stopping wait")
	}
}

func (publisher *ccsmpBackedPublisher) TaskQueue() chan SendTask {
	return publisher.taskQueue
}

func (publisher *ccsmpBackedPublisher) IsRunning() bool {
	return atomic.LoadInt32(&publisher.isRunning) == 1
}

func (publisher *ccsmpBackedPublisher) IncrementMetric(metric NextGenMetric, amount uint64) {
	publisher.metrics.IncrementMetric(metric, amount)
}

func (publisher *ccsmpBackedPublisher) AddAcknowledgementHandler(ackHandler AcknowledgementHandler) (uint64, func() (messageId uint64, correlationTag []byte)) {
	pubID := atomic.AddUint64(&publisher.acknowledgementHandlerID, 1)
	publisher.acknowledgementMap.Store(pubID, ackHandler)
	var messageID uint64
	return pubID, func() (nextId uint64, correlationTag []byte) {
		nextId = atomic.AddUint64(&messageID, 1)
		return nextId, toCorrelationTag(pubID, nextId)
	}
}

func (publisher *ccsmpBackedPublisher) RemoveAcknowledgementHandler(pubID uint64) {
	publisher.acknowledgementMap.Delete(pubID)
}

// For now we will keep eveyrthing as part of the internal publisher. Interface design breaks this out if needed in the future
func (publisher *ccsmpBackedPublisher) Acknowledgements() Acknowledgements {
	return publisher
}

const correlationIDLength = 8
const correlationTagLength = correlationIDLength * 2

func toCorrelationTag(pubID, messageID uint64) []uint8 {
	correlationTag := make([]uint8, correlationTagLength)
	// encode pub id and message id into correlationTag using little endian
	// correlation tag structure will be as follows:
	// pubId = b3b2b1b0, messageId=b7b6b5b4
	// correlationTag = |b0|b1|b2|b3|b4|b5|b6|b7|
	var shift, mask uint64 = 0, 0b11111111
	for i := 0; i < correlationIDLength; i, shift, mask = i+1, shift+8, mask<<8 {
		correlationTag[i] = uint8(pubID & mask >> shift)
		correlationTag[i+correlationIDLength] = uint8(messageID & mask >> shift)
	}
	return correlationTag
}

func fromCorrelationTag(bytes []uint8) (pubID, messageID uint64, ok bool) {
	if len(bytes) != correlationTagLength {
		return 0, 0, false
	}
	var shift uint64 = 0
	for i := 0; i < correlationIDLength; i, shift = i+1, shift+8 {
		pubID |= uint64(bytes[i]) << shift
		messageID |= uint64(bytes[i+correlationIDLength]) << shift
	}
	return pubID, messageID, true
}

func (publisher *ccsmpBackedPublisher) onAcknowledgement(correlationP unsafe.Pointer, persisted bool, err error) {
	if correlationP != nil {
		correlationTag := ccsmp.ToGoBytes(correlationP, correlationTagLength)
		pubID, msgID, ok := fromCorrelationTag(correlationTag)
		if ok {
			callbackPtr, ok := publisher.acknowledgementMap.Load(pubID)
			if ok {
				callback := callbackPtr.(AcknowledgementHandler)
				callback(msgID, persisted, err)
			} else if logging.Default.IsDebugEnabled() {
				// This is expected if we have terminated the publisher. We may still receive acks
				logging.Default.Debug("Received acknowledgement missing publisher callback with ID " + fmt.Sprint(pubID))
			}
		}
	}
}

func (publisher *ccsmpBackedPublisher) onCanSend() {
	select {
	case publisher.canSend <- true:
	default:
		// a can send is already queued
	}
}

func (publisher *ccsmpBackedPublisher) start() {
	if !atomic.CompareAndSwapInt32(&publisher.isRunning, 0, 1) {
		return
	}
	publisher.canSendEventID = publisher.Events().AddEventHandler(SolClientEventCanSend, func(scei SessionEventInfo) {
		publisher.onCanSend()
	})
	publisher.acknowledgementEventID = publisher.Events().AddEventHandler(SolClientEventAcknowledgement, func(ei SessionEventInfo) {
		publisher.onAcknowledgement(ei.GetCorrelationPointer(), true, ei.GetError())
	})
	publisher.rejectedEventID = publisher.Events().AddEventHandler(SolClientEventRejected, func(ei SessionEventInfo) {
		publisher.onAcknowledgement(ei.GetCorrelationPointer(), false, ei.GetError())
	})
	publisher.startRequestor()
	go publisher.publishLoop()
}

// blocking call to terminate
// we don't need a grace period as this will terminate immediately
func (publisher *ccsmpBackedPublisher) terminate() {
	if !atomic.CompareAndSwapInt32(&publisher.isRunning, 1, 2) {
		return
	}
	// interrupt the AwaitWritable function
	select {
	case publisher.canSend <- false:
	default:
		// do nothing, a can send event is already queued
	}

	// blocking terminate awaiting the shutdown of the publish loop
	close(publisher.termination)
	// wait for termination to complete
	<-publisher.terminationComplete
	// Any additional cleanup can go here
	publisher.events.RemoveEventHandler(publisher.canSendEventID)
	publisher.events.RemoveEventHandler(publisher.acknowledgementEventID)
	publisher.events.RemoveEventHandler(publisher.rejectedEventID)
	// cleanup requestor
	publisher.terminateRequestor()
}

func (publisher *ccsmpBackedPublisher) publishLoop() {
loop:
	for {
		select {
		// check for termination first as the below block will be arbitrary when queues are saturated
		case <-publisher.termination:
			break loop
		// no termination event waiting for us, we block on both the task queue and the termination queue
		default:
			// wait for a new task, or wait for a termination signal
			select {
			// get a task from the task queue and execute the task
			case task := <-publisher.taskQueue:
				task() // block until can sends and successful publish
			// if blocking while waiting for a task, wait for termination
			case <-publisher.termination:
				break loop
			}
		}
	}
	close(publisher.terminationComplete)
}
