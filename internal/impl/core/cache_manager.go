// pubsubplus-go-client
//
// Copyright 2025 Solace Corporation. All rights reserved.
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
	"sync/atomic"

	"solace.dev/go/messaging/internal/ccsmp"
	"solace.dev/go/messaging/internal/impl/constants"
	"solace.dev/go/messaging/internal/impl/logging"
	"solace.dev/go/messaging/pkg/solace"
	apimessage "solace.dev/go/messaging/pkg/solace/message"
	"solace.dev/go/messaging/pkg/solace/resource"
)

// PollAndProcessCacheResponseChannel is intended to be run as a go routine.
func (receiver *ccsmpBackedReceiver) PollAndProcessCacheResponseChannel() {
	receiver.setCachePollingRunning(true)
	var cacheEventInfo ccsmp.CacheEventInfo
	channelIsOpen := true
	/* poll cacheventinfo channel */
	for channelIsOpen {
		cacheEventInfo, channelIsOpen = <-receiver.cacheResponseChan
		atomic.AddInt32(&receiver.cacheResponseChanCounter, -1)
		if !channelIsOpen {
			// If channel is closed, we can stop polling. In this case we don't need to handle
			// the cacheEventInfo since there won't be a menaingful one left on the queue.
			// Any function that closes the channel must guarantee this.
			if receiver.cacheLogger.IsDebugEnabled() {
				receiver.cacheLogger.Debug("cacheResponseChan was closed, exiting PollAndProcessCacheResponseChannel loop.")
			}
			break
		}
		/* We decrement the counter first, since as soon as we pop the CacheEventInfo
		 * off the channel, CCSMP is able to put another on. If CCSMP is able resume processing the
		 * cache responses, we should unblock the application by allowing it to submit more cache
		 * requests ASAP.*/
		receiver.ProcessCacheEvent(cacheEventInfo)
	}
	// Indicate that this function has stopped running.
	receiver.setCachePollingRunning(false)
}

// ProcessCacheEvent is intended to be run by any agent trying to process a cache response. This can be run from a polling go routine, or during termination to cleanup remaining resources, and possibly by other agents as well.
func (receiver *ccsmpBackedReceiver) ProcessCacheEvent(cacheEventInfo ccsmp.CacheEventInfo) {
	if receiver.cacheLogger.IsDebugEnabled() {
		receiver.cacheLogger.Debug(fmt.Sprintf("ProcessCacheEvent::cacheEventInfo is:\n%s\n", cacheEventInfo.String()))
	}
	cacheSessionP := cacheEventInfo.GetCacheSessionPointer()
	foundCacheResponseHolder, found := receiver.cacheSessionMap.Load(cacheSessionP)
	cacheResponseHolder := foundCacheResponseHolder.(CacheResponseProcessor)
	if !found {
		if receiver.cacheLogger.IsDebugEnabled() {
			/* NOTE: This can occur when there has been a duplicate event, where for some reason CCSMP was able
			 * produce an event, but PSPGo thought CCSMP was not, so PSPGo generated an event on CCSMP's
			 * behalf, but after CCSMP's event was put on the channel. This would result in the CCSMP-
			 * generated event being processed and its cache session pointer being removed from the tabel
			 * and the duplicate event that was processed afterwards having the same cache session pointer,
			 * but no matching entry in the table since it was already removed by the original entry. This
			 * is not a bug, and the application doesn't need to be concerned about this, so we log it as
			 * debug. */
			receiver.cacheLogger.Debug(constants.UnableToProcessCacheResponse + constants.InvalidCacheSession)
		}
	} else {
		cacheResponse := solace.CacheResponse{}
		cacheResponseHolder.ProcessCacheResponse(cacheResponse)
	}
	/* Lifecycle management of cache sessions */
	/* NOTE: In the event of a duplicate event in the receiver.cacheResponseChan channel, the following deletion
	 * will not panic. */
	receiver.cacheSessionMap.Delete(cacheSessionP)
	if errorInfo := ccsmp.DestroyCacheSession(cacheSessionP); errorInfo != nil {
		/* NOTE: If we can't destroy the cache session, there is no follow up action that can be taken, so
		 * there is no point in returning an error. We just log it and move on. */
		receiver.cacheLogger.Error(fmt.Sprintf("%s %s and %s. ErrorInfo is: [%s]", constants.FailedToDestroyCacheSession, constants.WithCacheSessionPointer, constants.WithCacheRequestID, errorInfo.GetMessageAsString()))
	}
}

// CacheRequestInfo holds the original information that was used to send the cache request.
// This is useful for comparing a received cache response during processing, or for adding
// information to logging or error messages when this information cannot be retrieved from the
// cache response.
/* NOTE: This is actually most useful in generating cache response stubs for cache sessions that somehow got lost and
 * that still need to be cleaned up during termination.*/
type CacheRequestInfo struct {
	/* NOTE: we don't need to include the cache session pointer in this struct since it is only ever stored
	 * in a map where the cache session pointer is used as the key.*/
	cacheRequestID apimessage.CacheRequestID
	topic          string
}

func NewCacheRequestInfo(cacheRequestID apimessage.CacheRequestID, topic string) CacheRequestInfo {
	return CacheRequestInfo{
		cacheRequestID: cacheRequestID,
		topic:          topic,
	}
}

func (i *CacheRequestInfo) GetTopic() string {
	return i.topic
}

func (i *CacheRequestInfo) GetCacheRequestID() apimessage.CacheRequestID {
	return i.cacheRequestID
}

// CacheResponseProcessor provides an interface through which the information necessary to process a cache response
// that is passed from CCSMP can be acquired.
type CacheResponseProcessor interface {
	/* This model of having a common interface be implemented by multiple concrete types so that we can have a
	 * heterogeneous set of value types in the map is useful, but would become tedious as more implementing
	 * types were added since every type would have to implement a nil accessor for each other type. This is fine
	 * while there are only two implementing types, and more types are not expected. If the number of implementing
	 * types should increase, this design pattern should be revisited.
	 */

	// GetChannel returns the channel that is used to pass the CacheResponse back to the application if such a
	// channel exists or will otherwise be nil. If the channel exists, the bool will return as true. If the channel
	// does not exist, the bool will return false.
	GetChannel() (chan solace.CacheResponse, bool)

	// GetCallback returns the callback that is used to post-process the CacheResponse if such a callback exists.
	// If the callback exists, the bool will return as true. If the callback does not exist, the bool will return
	// false.
	GetCallback() (func(solace.CacheResponse), bool)

	// ProcessCacheResponse processes the cache response according to the implementation
	ProcessCacheResponse(solace.CacheResponse)

	// GetCacheRequestInfo returns the original information that was used to send the cache request.
	// This is useful for comparing a received cache response during processing, or for adding
	// information to logging or error messages when this information cannot be retrieved from the
	// cache response.
	GetCacheRequestInfo() *CacheRequestInfo
}

// CacheResponseCallbackHolder holds an application-provided callback that is responsible for post-processing the cache
// response. CacheResponseCallbackHolder implements the CacheResponseProcessor interface to allow safe access of this
// callback when being retrieved from a map of heterogeneous values.
type CacheResponseCallbackHolder struct {
	CacheResponseProcessor
	cacheRequestInfo CacheRequestInfo
	callback         func(solace.CacheResponse)
}

func NewCacheResponseCallbackHolder(callback func(solace.CacheResponse), cacheRequestInfo CacheRequestInfo) CacheResponseCallbackHolder {
	return CacheResponseCallbackHolder{
		cacheRequestInfo: cacheRequestInfo,
		callback:         callback,
	}
}

func (cbHolder CacheResponseCallbackHolder) GetCallback() (func(solace.CacheResponse), bool) {
	return cbHolder.callback, true
}

func (cbHolder CacheResponseCallbackHolder) GetChannel() (chan solace.CacheResponse, bool) {
	return nil, false
}

func (cbHolder CacheResponseCallbackHolder) ProcessCacheResponse(cacheResponse solace.CacheResponse) {
	if callback, found := cbHolder.GetCallback(); found {
		callback(cacheResponse)
	} else {
		logging.Default.Error(constants.UnableToPassCacheResponseToApplication + constants.NoCacheCallbackAvailable)
	}
}

func (cbHolder CacheResponseCallbackHolder) GetCacheRequestInfo() *CacheRequestInfo {
	return &cbHolder.cacheRequestInfo
}

// CacheResponseChannelHolder holds a API-provided channel to which the cache reponse will be pushed.
// CacheResponseChannelHolder implements the CacheResponseProcessor interface to allow safe access of this callback
// when being retrieved from a map of heterogeneous values.
type CacheResponseChannelHolder struct {
	CacheResponseProcessor
	cacheRequestInfo CacheRequestInfo
	channel          chan solace.CacheResponse
}

func NewCacheResponseChannelHolder(channel chan solace.CacheResponse, cacheRequestInfo CacheRequestInfo) CacheResponseChannelHolder {
	return CacheResponseChannelHolder{
		cacheRequestInfo: cacheRequestInfo,
		channel:          channel,
	}
}

func (chHolder CacheResponseChannelHolder) GetCallback() (func(solace.CacheResponse), bool) {
	return nil, false
}

func (chHolder CacheResponseChannelHolder) GetChannel() (chan solace.CacheResponse, bool) {
	return chHolder.channel, true
}

func (chHolder CacheResponseChannelHolder) ProcessCacheResponse(cacheResponse solace.CacheResponse) {
	/* Because function pointers and channels are both pointer types, they could be nil. So, we should
	 * check to make sure that they are not. There could be an error where the API did not create the
	 * correct holder type, which would cause the holder's value to be nil and the API would panic.*/
	if channel, found := chHolder.GetChannel(); found {
		/* This will not block because the channel is created with a buffer size of 1 in RequestCachedAsync() */
		channel <- cacheResponse
		close(channel)
	} else {
		/* This is an error log because it is the API's responsiblity to create and manage the channel. */
		logging.Default.Error(constants.UnableToPassCacheResponseToApplication + constants.NoCacheChannelAvailable)
	}
}

func (chHolder CacheResponseChannelHolder) GetCacheRequestInfo() *CacheRequestInfo {
	return &chHolder.cacheRequestInfo
}

// addCacheSessionToMapIfNotPresent adds a cache session to the map and associates it with a CacheResponseProcessor if
// it is not already present. If the cache session is already present, this function returns an IllegalStateError.
func (receiver *ccsmpBackedReceiver) addCacheSessionToMapIfNotPresent(holder CacheResponseProcessor, cacheSessionP ccsmp.SolClientCacheSessionPt) error {
	/* FFC: There is a race condition in the function where we read one state of the map, and then
	 * update the state after the map has been mutated. This is because the lock is managed by the map accessor
	 * functions. This should not happen, since it would require duplicate pointers in CCSMP. The alternative is
	 * code duplication that IMO is not worth it to avoid a race condition that would only occur because of a bug
	 * in CCSMP. This sort of bug would have other obvious impacts on the application anyways, so we don't need
	 * to rely on this path as the only one to notify the application of such a problem.
	 */
	var err error
	err = nil
	if _, found := receiver.cacheSessionMap.Load(cacheSessionP); found {
		/* Pre-existing cache session found. This error is fatal to the operation but not to the API since we can
		 * this does not block other activities like subscribing or trying to send a distint cache request, but does
		 * prevent the API from indexing the cache session which is necessary for tracking cache request lifecycles.
		 */
		err = solace.NewError(&solace.IllegalStateError{},
			fmt.Sprintf("%s [0x%x] %s", constants.ApplicationTriedToCreateCacheRequest, cacheSessionP, constants.AnotherCacheSessionAlreadyExists), nil)
		return err
	}
	/* No pre-existing cache session found, we can index the current one and continue. */
	receiver.cacheSessionMap.Store(cacheSessionP, holder)
	return err
}

// isAvailableForCache returns nil if the receiver is ready to send a cache request, or an error if it is not.
func (receiver *ccsmpBackedReceiver) checkStateForCacheRequest() error {
	var err error
	var errorString string = ""
	if !receiver.IsRunning() {
		/* NOTE: it would be great if we could provide a more detailed error string here, but
		 * ccsmpBackedReceiver.IsRunning() only returns a boolean, so we can't say more than we already have.
		 */
		errorString = "Could not perform cache operations because the receiver was not running."
		err = solace.NewError(&solace.IllegalStateError{}, errorString, nil)
	} else if atomic.LoadInt32(&receiver.cacheResponseChanCounter) >= cacheResponseChannelMaxSize {
		errorString = fmt.Sprintf("Could not perform cache operations because more than %d cache responses are still waiting to be processed by the application.", cacheResponseChannelMaxSize)
		err = solace.NewError(&solace.IllegalStateError{}, errorString, nil)
	}
	if errorString != "" {
		/* Warn log because application tried to conduct operation without properly configuring the object. */
		receiver.cacheLogger.Warning(errorString)
	}
	return err
}

// StartAndInitCacheManagerIfNotDoneAlready allocates whatever resources are required for managing cache requests.
// This setup is done only once, and is intended to be done after the first cache request has been submitted to the API
// by the application but before the cache request is passed from Go to C. This is because the CacheManager is not a
// standalone object, but is rather a trait of the its implementor, which may or may not exclusively conduct cache
// operations. In the case that the CacheManager's implementor does not exclusively implement cache operations, e.g. a
// direct receiver, unless that implementor is directed by the application to conduct cache operations, the resources
// required for those operations are not needed. In this case, pre-allocating the resources on receiver start would be
// a waste of time and memory. Only if the implementor is directed to conduct a cache operation are the relevant
// resources actually required and so allocated.
func (receiver *ccsmpBackedReceiver) StartAndInitCacheManagerIfNotDoneAlready() {
	if receiver.cacheResponseChan == nil {
		receiver.cacheResponseChan = make(chan ccsmp.CacheEventInfo, cacheResponseChannelMaxSize)
	}
	if !receiver.isCachePollingRunning() {
		go receiver.PollAndProcessCacheResponseChannel()
		if receiver.cacheLogger.IsDebugEnabled() {
			receiver.cacheLogger.Debug(constants.StartedCachePolling)
		}
	} else {
		if receiver.cacheLogger.IsDebugEnabled() {
			receiver.cacheLogger.Debug(constants.DidntStartCachePolling)
		}
	}
}

func (receiver *ccsmpBackedReceiver) generateCacheRequestCancellationNotice(cacheSessionP ccsmp.SolClientCacheSessionPt, cacheResponseProcessor CacheResponseProcessor, errorInfo *ccsmp.SolClientErrorInfoWrapper) ccsmp.CacheEventInfo {
	if receiver.cacheLogger.IsDebugEnabled() {
		receiver.cacheLogger.Debug(constants.AttemptingCancellationNoticeGeneration)
	}
	cacheEventInfo := ccsmp.NewCacheEventInfoForCancellation(cacheSessionP, cacheResponseProcessor.GetCacheRequestInfo().GetCacheRequestID(), cacheResponseProcessor.GetCacheRequestInfo().GetTopic(), ToNativeError(errorInfo, constants.FailedToCancelCacheRequest))
	return cacheEventInfo
}

// cancelAllPendingCacheRequests will cancel all pending cache requests and potentially block until all cancellations
// are pushed to the cacheResponse channel.
func (receiver *ccsmpBackedReceiver) cancelAllPendingCacheRequests() {
	receiver.cacheSessionMap.Range(func(key, value interface{}) bool {
		cacheSessionP := key.(ccsmp.SolClientCacheSessionPt)
		cacheResponseProcessor := value.(CacheResponseProcessor)
		errorInfo := ccsmp.CancelCacheRequest(cacheSessionP)
		if errorInfo != nil {
			if errorInfo.ReturnCode != ccsmp.SolClientReturnCodeOk {
				/* FFC: might need to add additional checking for subcodes or error strings specific to a CCSMP
				 * trying to cancel cache requests on an invalid cache session. */
				/* There was a failure in cancelling the cache request, but we still
				 * have a cache session pointer, so we generate a cache response to notify
				 * the application that something went wrong and defer destroying the cache
				 * session to a later point.*/
				receiver.cacheLogger.Info(fmt.Sprintf("%s %s %d and %s %d.", constants.FailedToCancelCacheRequest, constants.WithCacheRequestID, cacheResponseProcessor.GetCacheRequestInfo().GetCacheRequestID(), constants.WithCacheSessionPointer, cacheSessionP))
				generatedEvent := receiver.generateCacheRequestCancellationNotice(cacheSessionP, cacheResponseProcessor, errorInfo)
				/* WARNING: This will block if the next cache response in the channel is associated with a
				 * cache request that the application elected to process their cache responses through
				 * a callback and the channel is full, until the application finishes processing the event
				 * through that callback.*/
				atomic.AddInt32(&receiver.cacheResponseChanCounter, 1)
				receiver.cacheResponseChan <- generatedEvent
			}
		}
		return true
	})
}

// Teardown is used to clean up cache-related resources as a part of termination. This method assumes
// that terminate has already been called and that we don't need to run state checks, since the caller or one
// of its parents should hold the state for it.
/* WARNING: If the application has submitted any cache requests with a callback for processing, this function will
 * block until all the callbacks are processed. */
func (receiver *ccsmpBackedReceiver) Teardown() {
	if running := receiver.isCachePollingRunning(); !running {
		/* We can return early since either the resources and shutdown are being handled by a
		 * different thread right now, or it's already been done before. */
		return
	}

	/* For all cache sessions remaining in the map, issue CCSMP cancellation. Possibly on go routine? check python */
	receiver.cancelAllPendingCacheRequests()
	/* For all cache sessions remaining in the map, generate cancellation events for them and put them
	 * on the cacheResponseChan. These should get to the chan after the actual CCSMP cancellations. Edit
	 * ProcessCacheEvent if necessary to ignore events without a corresponding cache session so that if
	 * any duplicates are generated, they are ignored by ProcessCacheEvent and not passed to the
	 * application.*/
	/* Release the mutex and then shutdown the PollAndProcessCacheResponseChannel goroutine and
	 * cacheResponseChan. */
	close(receiver.cacheResponseChan)
	for len(receiver.cacheResponseChan) > 0 {
		receiver.ProcessCacheEvent(<-receiver.cacheResponseChan)
	}
}

const cachePollingRunningTrue uint32 = 1
const cachePollingRunningFalse uint32 = 0

func (receiver *ccsmpBackedReceiver) isCachePollingRunning() bool {
	return atomic.LoadUint32(&receiver.cachePollingRunning) == cachePollingRunningTrue
}

func (receiver *ccsmpBackedReceiver) setCachePollingRunning(running bool) {
	if running {
		atomic.StoreUint32(&receiver.cachePollingRunning, cachePollingRunningTrue)
	} else {
		atomic.StoreUint32(&receiver.cachePollingRunning, cachePollingRunningFalse)
	}
}

// CacheManager interface
type CacheManager interface {
	// Cleanup cache resources
	Teardown()
	// SendCacheRequest
	SendCacheRequest(cachedMessageSubscriptionRequest resource.CachedMessageSubscriptionRequest, cacheRequestID apimessage.CacheRequestID, cacheResponseHandler CacheResponseProcessor) error
	// StartAndInitCacheManagerIfNotDoneAlready
	StartAndInitCacheManagerIfNotDoneAlready()
}

func (receiver *ccsmpBackedReceiver) SendCacheRequest(cachedMessageSubscriptionRequest resource.CachedMessageSubscriptionRequest, cacheRequestID apimessage.CacheRequestID, cacheResponseHandler CacheResponseProcessor) error {
	receiver.cacheLogger = logging.For(receiver)

	var err error

	if err = receiver.checkStateForCacheRequest(); err != nil {
		/* NOTE: We relay on checkStateForCacheRequest to log dicta. */
		return err
	}

	/* init cache resources if not present (tables, etc.) */
	receiver.StartAndInitCacheManagerIfNotDoneAlready()

	/* create cache session */
	propsList := []string{
		ccsmp.SolClientCacheSessionPropCacheName, cachedMessageSubscriptionRequest.GetCacheName(),
		ccsmp.SolClientCacheSessionPropMaxAge, strconv.FormatInt(int64(cachedMessageSubscriptionRequest.GetCachedMessageAge()), 10),
		ccsmp.SolClientCacheSessionPropMaxMsgs, strconv.FormatInt(int64(cachedMessageSubscriptionRequest.GetMaxCachedMessages()), 10),
		ccsmp.SolClientCacheSessionPropRequestreplyTimeoutMs, strconv.FormatInt(int64(cachedMessageSubscriptionRequest.GetCacheAccessTimeout()), 10),
	}
	/* We don't need to release the mutex on calls to CreateCacheSession because there are no callbacks. */
	cacheSessionP, errInfo := ccsmp.CreateCacheSession(propsList, receiver.session.GetPointer())
	if receiver.cacheLogger.IsDebugEnabled() {
		receiver.cacheLogger.Debug(fmt.Sprintf("Created cache session 0x%x", cacheSessionP))
	}

	if errInfo != nil {
		errorString := constants.FailedToCreateCacheSession + constants.WithCacheRequestID + fmt.Sprintf("%d", cacheRequestID)
		receiver.cacheLogger.Warning(errorString)
		return ToNativeError(errInfo, errorString)
	}

	/* store cache session in table with channel */
	if err = receiver.addCacheSessionToMapIfNotPresent(cacheResponseHandler, cacheSessionP); err != nil {
		return err
	}

	/* Run go routine that sends cache request */
	var cacheEventCallback ccsmp.SolClientCacheEventCallback = func(cacheEventInfo ccsmp.CacheEventInfo) {
		atomic.AddInt32(&receiver.cacheResponseChanCounter, 1)
		receiver.cacheResponseChan <- cacheEventInfo
	}
	cacheStrategy := cachedMessageSubscriptionRequest.GetCachedMessageSubscriptionRequestStrategy()
	if cacheStrategy == nil {
		errorString := fmt.Sprintf("%s %s %d and %s %d because %s", constants.FailedToSendCacheRequest, constants.WithCacheRequestID, cacheRequestID, constants.WithCacheSessionPointer, cacheSessionP, constants.InvalidCachedMessageSubscriptionStrategyPassed)
		receiver.cacheLogger.Warning(errorString)
		return solace.NewError(&solace.IllegalArgumentError{}, errorString, nil)
	}
	if receiver.cacheLogger.IsDebugEnabled() {
		receiver.cacheLogger.Debug(fmt.Sprintf("Sending cache request with cache request ID %d on cache session 0x%x", cacheRequestID, cacheSessionP))
	}
	errInfo = ccsmp.SendCacheRequest(uintptr(receiver.dispatchID),
		cacheSessionP,
		cachedMessageSubscriptionRequest.GetName(),
		cacheRequestID,
		ccsmp.CachedMessageSubscriptionRequestStrategyMappingToCCSMPCacheRequestFlags[*cacheStrategy],
		ccsmp.CachedMessageSubscriptionRequestStrategyMappingToCCSMPSubscribeFlags[*cacheStrategy],
		cacheEventCallback)
	if errInfo != nil {
		errorString := fmt.Sprintf("%s %s %d and %s 0x%x", constants.FailedToSendCacheRequest, constants.WithCacheRequestID, cacheRequestID, constants.WithCacheSessionPointer, cacheSessionP)
		receiver.cacheLogger.Warning(errorString)
		return ToNativeError(errInfo, errorString)
	}

	receiver.cacheLogger.Debug("Sent cache request")
	return err
}
