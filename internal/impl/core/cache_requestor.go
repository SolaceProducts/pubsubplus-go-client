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

	"solace.dev/go/messaging/internal/ccsmp"
	"solace.dev/go/messaging/internal/impl/constants"
	"solace.dev/go/messaging/internal/impl/logging"
	"solace.dev/go/messaging/pkg/solace"
	apimessage "solace.dev/go/messaging/pkg/solace/message"
	"solace.dev/go/messaging/pkg/solace/resource"
)

// maxOutstandingCacheRequests indicates the maximum number of cache responses that can be buffered by the API without
// being processed by the application.
const maxOutstandingCacheRequests int = 1024

// ProcessCacheEvent is intended to be run by any agent trying to process a cache response. This can be run from a polling go routine, or during termination to cleanup remaining resources, and possibly by other agents as well.
func (receiver *ccsmpBackedReceiver) ProcessCacheEvent(cacheEventInfo ccsmp.CacheEventInfo) {
	if logging.Default.IsDebugEnabled() {
		logging.Default.Debug(fmt.Sprintf("ProcessCacheEvent::cacheEventInfo is:\n%s\n", cacheEventInfo.String()))
	}
	cacheSessionP := cacheEventInfo.GetCacheSessionPointer()
	cacheSession := ccsmp.NewSolClientCacheSession(cacheSessionP)
	foundCacheResponseHolder, found := receiver.cacheSessionMap.Load(cacheSession)
	cacheResponseHolder := foundCacheResponseHolder.(CacheResponseProcessor)
	if !found {
		if logging.Default.IsDebugEnabled() {
			/* NOTE: This can occur when there has been a duplicate event, where for some reason CCSMP was able
			 * produce an event, but PSPGo thought CCSMP was not, so PSPGo generated an event on CCSMP's
			 * behalf, but after CCSMP's event was put on the channel. This would result in the CCSMP-
			 * generated event being processed and its cache session pointer being removed from the tabel
			 * and the duplicate event that was processed afterwards having the same cache session pointer,
			 * but no matching entry in the table since it was already removed by the original entry. This
			 * is not a bug, and the application doesn't need to be concerned about this, so we log it as
			 * debug. */
			logging.Default.Debug(constants.UnableToProcessCacheResponse + constants.InvalidCacheSession)
		}
	} else {
		cacheResponse := solace.CacheResponse{}
		cacheResponseHolder.ProcessCacheResponse(cacheResponse)
	}
	/* Lifecycle management of cache sessions */
	/* NOTE: In the event of a duplicate event in the receiver.cacheResponseChan channel, the following deletion
	 * will not panic. */
	receiver.cacheSessionMap.Delete(cacheSession)
	if errorInfo := cacheSession.DestroyCacheSession(); errorInfo != nil {
		/* NOTE: If we can't destroy the cache session, there is no follow up action that can be taken, so
		 * there is no point in returning an error. We just log it and move on. */
		logging.Default.Error(fmt.Sprintf("%s %s and %s. ErrorInfo is: [%s]", constants.FailedToDestroyCacheSession, constants.WithCacheSessionPointer, constants.WithCacheRequestID, errorInfo.GetMessageAsString()))
	}
}

// addCacheSessionToMapIfNotPresent adds a cache session to the map and associates it with a CacheResponseProcessor if
// it is not already present. If the cache session is already present, this function returns an IllegalStateError.
func (receiver *ccsmpBackedReceiver) addCacheSessionToMapIfNotPresent(holder CacheResponseProcessor, cacheSession ccsmp.SolClientCacheSession) error {
	/* FFC: There is a race condition in the function where we read one state of the map, and then
	 * update the state after the map has been mutated. This is because the lock is managed by the map accessor
	 * functions. This should not happen, since it would require duplicate pointers in CCSMP. The alternative is
	 * code duplication that IMO is not worth it to avoid a race condition that would only occur because of a bug
	 * in CCSMP. This sort of bug would have other obvious impacts on the application anyways, so we don't need
	 * to rely on this path as the only one to notify the application of such a problem.
	 */
	var err error
	err = nil
	if _, found := receiver.cacheSessionMap.Load(cacheSession); found {
		/* Pre-existing cache session found. This error is fatal to the operation but not to the API since we can
		 * this does not block other activities like subscribing or trying to send a distint cache request, but does
		 * prevent the API from indexing the cache session which is necessary for tracking cache request lifecycles.
		 */
		err = solace.NewError(&solace.IllegalStateError{},
			fmt.Sprintf("%s [0x%x] %s", constants.ApplicationTriedToCreateCacheRequest, cacheSession, constants.AnotherCacheSessionAlreadyExists), nil)
		return err
	}
	/* No pre-existing cache session found, we can index the current one and continue. */
	receiver.cacheSessionMap.Store(cacheSession, holder)
	return err
}

// CancelAllPendingCacheRequests will cancel all pending cache requests and potentially block until all cancellations
// are pushed to the cacheResponse channel.
func (receiver *ccsmpBackedReceiver) CancelAllPendingCacheRequests() {
	receiver.cacheSessionMap.Range(func(key, value interface{}) bool {
		cacheSession := key.(ccsmp.SolClientCacheSession)
		cacheResponseProcessor := value.(CacheResponseProcessor)
		errorInfo := cacheSession.CancelCacheRequest()
		if errorInfo != nil {
			if errorInfo.ReturnCode != ccsmp.SolClientReturnCodeOk {
				/* FFC: might need to add additional checking for subcodes or error strings specific to a CCSMP
				 * trying to cancel cache requests on an invalid cache session. */
				/* There was a failure in cancelling the cache request, but we still
				 * have a cache session pointer, so we generate a cache response to notify
				 * the application that something went wrong and defer destroying the cache
				 * session to a later point.*/
				logging.Default.Info(fmt.Sprintf("Failed to cancel cache request %s %d and %s %s.", constants.WithCacheRequestID, cacheResponseProcessor.GetCacheRequestInfo().GetCacheRequestID(), constants.WithCacheSessionPointer, cacheSession.String()))
				if logging.Default.IsDebugEnabled() {
					logging.Default.Debug(constants.AttemptingCancellationNoticeGeneration)
				}

				generatedEvent := ccsmp.NewCacheEventInfoForCancellation(cacheSession, cacheResponseProcessor.GetCacheRequestInfo().GetCacheRequestID(), cacheResponseProcessor.GetCacheRequestInfo().GetTopic(), ToNativeError(errorInfo, "Failed to cancel cache request."))
				/* WARNING: This will block if the next cache response in the channel is associated with a
				 * cache request that the application elected to process their cache responses through
				 * a callback and the channel is full, until the application finishes processing the event
				 * through that callback.*/
				receiver.cacheResponseChan <- generatedEvent
			}
		}
		return true
	})
}

func (receiver *ccsmpBackedReceiver) InitCacheRequestorResourcesIfNotDoneAlready() {
	if receiver.cacheResponseChan == nil {
		receiver.cacheResponseChan = make(chan ccsmp.CacheEventInfo, maxOutstandingCacheRequests)
	}
}

func (receiver *ccsmpBackedReceiver) IsCacheRequestorReady() bool {
	ret := len(receiver.cacheResponseChan) >= maxOutstandingCacheRequests
	logging.Default.Warning(fmt.Sprintf("Could not perform cache operations because more than %d cache responses are still waiting to be processed by the application.", maxOutstandingCacheRequests))
	return ret
}

// CacheRequestor interface
type CacheRequestor interface {
	// SendCacheRequest
	SendCacheRequest(cachedMessageSubscriptionRequest resource.CachedMessageSubscriptionRequest, cacheRequestID apimessage.CacheRequestID, cacheResponseHandler CacheResponseProcessor) error
	// StartAndInitCacheRequestorIfNotDoneAlready
	//StartAndInitCacheRequestorIfNotDoneAlready()

	//ProcessCacheEvent
	ProcessCacheEvent(ccsmp.CacheEventInfo)

	CancelAllPendingCacheRequests()
	CacheResponseChan() chan ccsmp.CacheEventInfo
	InitCacheRequestorResourcesIfNotDoneAlready()
	IsCacheRequestorReady() bool
}

// SendCacheRequest sends a creates a cache session and sends a cache request on that session. This method
// assumes the receiver is in the proper state (running). The caller must guarantee this state before
// attempting to send a cache request. Failing to do so will result in undefined behaviour.
func (receiver *ccsmpBackedReceiver) SendCacheRequest(cachedMessageSubscriptionRequest resource.CachedMessageSubscriptionRequest, cacheRequestID apimessage.CacheRequestID, cacheResponseHandler CacheResponseProcessor) error {
	var err error

	/* create cache session */
	propsList := ccsmp.ConvertCachedMessageSubscriptionRequestToCcsmpPropsList(cachedMessageSubscriptionRequest)

	cacheSession, errInfo := receiver.session.CreateCacheSession(propsList)
	if logging.Default.IsDebugEnabled() {
		logging.Default.Debug(fmt.Sprintf("Created cache session %s", cacheSession.String()))
	}

	if errInfo != nil {
		errorString := constants.FailedToCreateCacheSession + constants.WithCacheRequestID + fmt.Sprintf("%d", cacheRequestID)
		logging.Default.Warning(errorString)
		return ToNativeError(errInfo, errorString)
	}

	/* store cache session in table with channel */
	if err = receiver.addCacheSessionToMapIfNotPresent(cacheResponseHandler, cacheSession); err != nil {
		return err
	}

	/* Run go routine that sends cache request */
	var cacheEventCallback ccsmp.SolClientCacheEventCallback = func(cacheEventInfo ccsmp.CacheEventInfo) {
		receiver.cacheResponseChan <- cacheEventInfo
	}
	cacheStrategy := cachedMessageSubscriptionRequest.GetCachedMessageSubscriptionRequestStrategy()
	if cacheStrategy == nil {
		errorString := fmt.Sprintf("%s %s %d and %s %s because %s", constants.FailedToSendCacheRequest, constants.WithCacheRequestID, cacheRequestID, constants.WithCacheSessionPointer, cacheSession.String(), constants.InvalidCachedMessageSubscriptionStrategyPassed)
		logging.Default.Warning(errorString)
		return solace.NewError(&solace.IllegalArgumentError{}, errorString, nil)
	}
	if logging.Default.IsDebugEnabled() {
		logging.Default.Debug(fmt.Sprintf("Sending cache request with cache request ID %d on cache session %s", cacheRequestID, cacheSession.String()))
	}
	errInfo = cacheSession.SendCacheRequest(uintptr(receiver.dispatchID),
		cachedMessageSubscriptionRequest.GetName(),
		cacheRequestID,
		ccsmp.CachedMessageSubscriptionRequestStrategyMappingToCCSMPCacheRequestFlags[*cacheStrategy],
		ccsmp.CachedMessageSubscriptionRequestStrategyMappingToCCSMPSubscribeFlags[*cacheStrategy],
		cacheEventCallback)
	if errInfo != nil {
		errorString := fmt.Sprintf("%s %s %d and %s 0x%x", constants.FailedToSendCacheRequest, constants.WithCacheRequestID, cacheRequestID, constants.WithCacheSessionPointer, cacheSession.String())
		logging.Default.Warning(errorString)
		return ToNativeError(errInfo, errorString)
	}

	logging.Default.Debug("Sent cache request")
	return err
}

func (receiver *ccsmpBackedReceiver) CacheResponseChan() chan ccsmp.CacheEventInfo {
	return receiver.cacheResponseChan
}
