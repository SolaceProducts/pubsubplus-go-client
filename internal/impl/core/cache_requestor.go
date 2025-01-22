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
	"sync"

	"solace.dev/go/messaging/internal/ccsmp"
	"solace.dev/go/messaging/internal/impl/constants"
	"solace.dev/go/messaging/internal/impl/logging"
	"solace.dev/go/messaging/pkg/solace"
	apimessage "solace.dev/go/messaging/pkg/solace/message"
	"solace.dev/go/messaging/pkg/solace/resource"
)

// ProcessCacheEvent is intended to be run by any agent trying to process a cache response. This can be run from a polling go routine, or during termination to cleanup remaining resources, and possibly by other agents as well.
func (receiver *ccsmpBackedReceiver) ProcessCacheEvent(cacheRequestMap *sync.Map, cacheEventInfo ccsmp.CacheEventInfo) {
	if logging.Default.IsDebugEnabled() {
		logging.Default.Debug(fmt.Sprintf("ProcessCacheEvent::cacheEventInfo is:\n%s\n", cacheEventInfo.String()))
	}
	cacheSessionP := cacheEventInfo.GetCacheSessionPointer()
	cacheSession := ccsmp.NewSolClientCacheSession(cacheSessionP)
	cacheRequestIndex := GetCacheRequestMapIndexFromCacheSession(cacheSession)
	foundCacheResponseHolder, found := cacheRequestMap.Load(cacheRequestIndex)
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
		cacheResponseHolder := foundCacheResponseHolder.(CacheResponseProcessor)
		cacheResponse := solace.CacheResponse{}
		cacheResponseHolder.ProcessCacheResponse(cacheResponse)
	}
	/* Lifecycle management of cache sessions */
	/* NOTE: In the event of a duplicate event in the cacheRequestMap channel, the following deletion
	 * will not panic. */
	cacheRequestMap.Delete(cacheSession)
	if errorInfo := cacheSession.DestroyCacheSession(); errorInfo != nil {
		/* NOTE: If we can't destroy the cache session, there is no follow up action that can be taken, so
		 * there is no point in returning an error. We just log it and move on. */
		logging.Default.Error(fmt.Sprintf("%s %s and %s. ErrorInfo is: [%s]", constants.FailedToDestroyCacheSession, constants.WithCacheSessionPointer, constants.WithCacheRequestID, errorInfo.GetMessageAsString()))
	}
}

// CancelAllPendingCacheRequests will cancel all pending cache requests and potentially block until all cancellations
// are pushed to the cacheResponse channel.
func (receiver *ccsmpBackedReceiver) CancelAllPendingCacheRequests(cacheRequestIndex uintptr, cacheResponseProcessor CacheResponseProcessor) *ccsmp.CacheEventInfo {
	//func (receiver *ccsmpBackedReceiver) CancelAllPendingCacheRequests(cacheSession ccsmp.SolClientCacheSession, cacheResponseProcessor CacheResponseProcessor) *ccsmp.CacheEventInfo {
	var generatedEvent ccsmp.CacheEventInfo
	cacheSession := GetCacheSessionFromCacheRequestIndex(cacheRequestIndex)
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

			generatedEvent = ccsmp.NewCacheEventInfoForCancellation(cacheSession, cacheResponseProcessor.GetCacheRequestInfo().GetCacheRequestID(), cacheResponseProcessor.GetCacheRequestInfo().GetTopic(), ToNativeError(errorInfo, "Failed to cancel cache request."))
		}
	}
	return &generatedEvent
}

// CacheRequestMapIndex is a type alias to uintptr, which is the backing type of SolClientCacheSessionPt.
// CacheRequestMapIndex is used as the index in the map between cache sessions and cache response processors in the
// receiver.
type CacheRequestMapIndex = uintptr

type CacheRequest interface {
	RequestConfig() resource.CachedMessageSubscriptionRequest
	ID() apimessage.CacheRequestID
	Processor() CacheResponseProcessor
	CacheSession() ccsmp.SolClientCacheSession
	Index() CacheRequestMapIndex
}

type CacheRequestImpl struct {
	cachedMessageSubscriptionRequest resource.CachedMessageSubscriptionRequest
	cacheRequestID                   apimessage.CacheRequestID
	cacheResponseProcessor           CacheResponseProcessor
	cacheSession                     ccsmp.SolClientCacheSession
	index                            CacheRequestMapIndex
}

func GetCacheSessionFromCacheRequestIndex(cacheRequestMapIndex CacheRequestMapIndex) ccsmp.SolClientCacheSession {
	return ccsmp.NewSolClientCacheSession(ccsmp.SolClientCacheSessionPt(cacheRequestMapIndex))
}

func GetCacheRequestMapIndexFromCacheSession(cacheSession ccsmp.SolClientCacheSession) CacheRequestMapIndex {
	return CacheRequestMapIndex(cacheSession.ConvertPointerToInt())
}

func (cacheRequest *CacheRequestImpl) RequestConfig() resource.CachedMessageSubscriptionRequest {
	return cacheRequest.cachedMessageSubscriptionRequest
}

func (cacheRequest *CacheRequestImpl) ID() apimessage.CacheRequestID {
	return cacheRequest.cacheRequestID
}

func (cacheRequest *CacheRequestImpl) Processor() CacheResponseProcessor {
	return cacheRequest.cacheResponseProcessor
}

func (cacheRequest *CacheRequestImpl) CacheSession() ccsmp.SolClientCacheSession {
	return cacheRequest.cacheSession
}

func (cacheRequest *CacheRequestImpl) Index() CacheRequestMapIndex {
	return cacheRequest.index
}

func NewCacheRequest(cachedMessageSubscriptionRequest resource.CachedMessageSubscriptionRequest, cacheRequestID apimessage.CacheRequestID, cacheResponseHandler CacheResponseProcessor, cacheSession ccsmp.SolClientCacheSession) CacheRequest {
	return &CacheRequestImpl{
		cachedMessageSubscriptionRequest: cachedMessageSubscriptionRequest,
		cacheRequestID:                   cacheRequestID,
		cacheResponseProcessor:           cacheResponseHandler,
		cacheSession:                     cacheSession,
		index:                            GetCacheRequestMapIndexFromCacheSession(cacheSession),
	}
}

func (receiver *ccsmpBackedReceiver) CreateCacheRequest(cachedMessageSubscriptionRequest resource.CachedMessageSubscriptionRequest, cacheRequestID apimessage.CacheRequestID, cacheResponseHandler CacheResponseProcessor) (CacheRequest, error) {
	/* create cache session */
	propsList := ccsmp.ConvertCachedMessageSubscriptionRequestToCcsmpPropsList(cachedMessageSubscriptionRequest)

	cacheSession, errInfo := receiver.session.CreateCacheSession(propsList)

	if errInfo != nil {
		errorString := constants.FailedToCreateCacheSession + constants.WithCacheRequestID + fmt.Sprintf("%d", cacheRequestID)
		logging.Default.Warning(errorString)
		return nil, ToNativeError(errInfo, errorString)
	}
	if logging.Default.IsDebugEnabled() {
		logging.Default.Debug(fmt.Sprintf("Created cache session %s", cacheSession.String()))
	}
	return NewCacheRequest(cachedMessageSubscriptionRequest, cacheRequestID, cacheResponseHandler, cacheSession), nil

}

func (receiver *ccsmpBackedReceiver) DestroyCacheRequest(cacheRequest CacheRequest) error {
	var err error
	/* NOTE: We do not cancel any cache requests here in constrast to other failure paths that clean up the cache
	 * session, such as in Terminate/teardownCache, because this method is intended to be used to clean up the
	 * cache session and related resources only after a failed call to SendCacheRequest().
	 */
	cacheSession := cacheRequest.CacheSession()
	if errorInfo := cacheSession.DestroyCacheSession(); errorInfo != nil {
		/* NOTE: If we can't destroy the cache session, there is no follow up action that can be taken, so
		 * there is no point in returning an error. We just log it and move on. */
		errorString := fmt.Sprintf("%s %s and %s. ErrorInfo is: [%s]", constants.FailedToDestroyCacheSession, constants.WithCacheSessionPointer, constants.WithCacheRequestID, errorInfo.GetMessageAsString())
		logging.Default.Error(errorString)
		err = ToNativeError(errorInfo, errorString)
	}
	return err
}

// CacheRequestor interface
type CacheRequestor interface {
	// CreateCacheRequest
	CreateCacheRequest(resource.CachedMessageSubscriptionRequest, apimessage.CacheRequestID, CacheResponseProcessor) (CacheRequest, error)
	// DestroyCacheRequest is used to clean up a cache request object when SendCacheRequestFails
	DestroyCacheRequest(CacheRequest) error
	// SendCacheRequest
	SendCacheRequest(CacheRequest, ccsmp.SolClientCacheEventCallback) error
	//SendCacheRequest(cachedMessageSubscriptionRequest resource.CachedMessageSubscriptionRequest, cacheRequestID apimessage.CacheRequestID, cacheResponseHandler CacheResponseProcessor) error
	// StartAndInitCacheRequestorIfNotDoneAlready
	//StartAndInitCacheRequestorIfNotDoneAlready()

	//ProcessCacheEvent
	ProcessCacheEvent(*sync.Map, ccsmp.CacheEventInfo)

	CancelAllPendingCacheRequests(uintptr, CacheResponseProcessor) *ccsmp.CacheEventInfo
	//CancelAllPendingCacheRequests(ccsmp.SolClientCacheSession, CacheResponseProcessor) *ccsmp.CacheEventInfo
}

// SendCacheRequest sends a creates a cache session and sends a cache request on that session. This method
// assumes the receiver is in the proper state (running). The caller must guarantee this state before
// attempting to send a cache request. Failing to do so will result in undefined behaviour.
func (receiver *ccsmpBackedReceiver) SendCacheRequest(cacheRequest CacheRequest, cacheEventCallback ccsmp.SolClientCacheEventCallback) error {
	var err error

	cacheSession := cacheRequest.CacheSession()
	cacheStrategy := cacheRequest.RequestConfig().GetCachedMessageSubscriptionRequestStrategy()
	if cacheStrategy == nil {
		errorString := fmt.Sprintf("%s %s %d and %s %s because %s", constants.FailedToSendCacheRequest, constants.WithCacheRequestID, cacheRequest.ID(), constants.WithCacheSessionPointer, cacheSession.String(), constants.InvalidCachedMessageSubscriptionStrategyPassed)
		logging.Default.Warning(errorString)
		return solace.NewError(&solace.IllegalArgumentError{}, errorString, nil)
	}
	if logging.Default.IsDebugEnabled() {
		logging.Default.Debug(fmt.Sprintf("Sending cache request with cache request ID %d on cache session %s", cacheRequest.ID(), cacheSession.String()))
	}
	errInfo := cacheSession.SendCacheRequest(uintptr(receiver.dispatchID),
		cacheRequest.RequestConfig().GetName(),
		cacheRequest.ID(),
		ccsmp.CachedMessageSubscriptionRequestStrategyMappingToCCSMPCacheRequestFlags[*cacheStrategy],
		ccsmp.CachedMessageSubscriptionRequestStrategyMappingToCCSMPSubscribeFlags[*cacheStrategy],
		cacheEventCallback)
	if errInfo != nil {
		errorString := fmt.Sprintf("%s %s %d and %s 0x%x", constants.FailedToSendCacheRequest, constants.WithCacheRequestID, cacheRequest.ID(), constants.WithCacheSessionPointer, cacheSession.String())
		logging.Default.Warning(errorString)
		return ToNativeError(errInfo, errorString)
	}

	logging.Default.Debug("Sent cache request")
	return err
}
