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
	"unsafe"

	"solace.dev/go/messaging/internal/ccsmp"
	"solace.dev/go/messaging/internal/impl/constants"
	"solace.dev/go/messaging/internal/impl/logging"
	"solace.dev/go/messaging/pkg/solace"
	"solace.dev/go/messaging/pkg/solace/message"
	apimessage "solace.dev/go/messaging/pkg/solace/message"
	"solace.dev/go/messaging/pkg/solace/resource"
	"solace.dev/go/messaging/pkg/solace/subcode"
)

type SubscribeFlags = ccsmp.SolClientSubscribeFlags
// CacheResponse provides information about the response received from the cache.
type cacheResponse struct {
	cacheRequestOutcome solace.CacheRequestOutcome
	cacheRequestID      apimessage.CacheRequestID
	err                 error
}

// GetCacheRequestOutcome retrieves the cache request outcome for the cache response
func (cacheResp *cacheResponse) GetCacheRequestOutcome() solace.CacheRequestOutcome {
	return cacheResp.cacheRequestOutcome
}

// GetCacheRequestID retrieves the cache request ID that generated the cache response
func (cacheResp *cacheResponse) GetCacheRequestID() apimessage.CacheRequestID {
	return cacheResp.cacheRequestID
}

// GetError retrieves the error field, will be nil if the cache request
// was successful, and will be not nil if a problem was encountered.
func (cacheResp *cacheResponse) GetError() error {
	return cacheResp.err
}

// ProcessCacheEvent is intended to be run by any agent trying to process a cache response. This can be run from a polling go routine, or during termination to cleanup remaining resources, and possibly by other agents as well.
func (receiver *ccsmpBackedReceiver) ProcessCacheEvent(cacheRequestMap *sync.Map, cacheEventInfo CoreCacheEventInfo) {
	if logging.Default.IsDebugEnabled() {
		logging.Default.Debug(fmt.Sprintf("ProcessCacheEvent::cacheEventInfo is:\n%s\n", cacheEventInfo.String()))
	}
	cacheSessionP := cacheEventInfo.GetCacheSessionPointer()
	cacheSession := ccsmp.WrapSolClientCacheSessionPt(cacheSessionP)
	cacheRequestIndex := GetCacheRequestMapIndexFromCacheSession(cacheSession)
	foundCacheRequest, found := cacheRequestMap.Load(cacheRequestIndex)
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
			logging.Default.Debug("Unable to process cache response because: The cache session associated with the given cache request/response was invalid")
		}
	} else {
		cacheRequest := foundCacheRequest.(CacheRequest)

		var cacheRespError error = nil
		var cacheRequestOutcome solace.CacheRequestOutcome

		if cacheEventInfo.GetReturnCode() == ccsmp.SolClientReturnCodeOk {
			// request was successful, no need to check subcode
			cacheRequestOutcome = solace.CacheRequestOutcomeOk
			// cacheRespError should be nil since returncode is OK
		} else if cacheEventInfo.GetReturnCode() == ccsmp.SolClientReturnCodeIncomplete {
			// request still in progress, check subcode for actual outcome of cache request
			if cacheEventInfo.GetSubCode() == ccsmp.SolClientSubCodeCacheSuspectData {
				// suspect data
				cacheRequestOutcome = solace.CacheRequestOutcomeSuspectData
				// cacheRespError should be nil
			} else if cacheEventInfo.GetSubCode() == ccsmp.SolClientSubCodeCacheNoData {
				// no data
				cacheRequestOutcome = solace.CacheRequestOutcomeNoData
				// cacheRespError should be nil
			} else if cacheEventInfo.GetSubCode() == ccsmp.SolClientSubCodeCacheTimeout {
				// request timed out, failed
				cacheRequestOutcome = solace.CacheRequestOutcomeFailed
				// cacheRespError should be a timeout error
				cacheRespError = solace.NewError(&solace.TimeoutError{}, "the cache request timed out", nil)
				if logging.Default.IsDebugEnabled() {
					logging.Default.Debug(
						fmt.Sprintf(
							"ProcessCacheEvent: The cache request timed out.\nReturnCode is: %d\nSubCode is: %d\n",
							cacheEventInfo.GetReturnCode(),
							cacheEventInfo.GetSubCode()))
				}
			} else {
				// failed
				cacheRequestOutcome = solace.CacheRequestOutcomeFailed
				// cacheRespError should be nil
			}
		} else {
			// request failed, no need to check subcode
			cacheRequestOutcome = solace.CacheRequestOutcomeFailed
			// set cacheRespError based on the returned subcode
			if cacheEventInfo.GetSubCode() != ccsmp.SolClientSubCodeCacheSuspectData && cacheEventInfo.GetSubCode() != ccsmp.SolClientSubCodeCacheNoData {
				// cacheRespError should be a solaceError
				cacheRespError = solace.NewNativeError("the cache request failed", subcode.Code(cacheEventInfo.GetSubCode()))
				if logging.Default.IsDebugEnabled() {
					logging.Default.Debug(
						fmt.Sprintf(
							"ProcessCacheEvent: The cache request failed.\nReturnCode is: %d\nSubCode is: %d\n",
							cacheEventInfo.GetReturnCode(),
							cacheEventInfo.GetSubCode()))
				}
			}
		}

		cacheResponse := &cacheResponse{
			cacheRequestOutcome: cacheRequestOutcome,
			cacheRequestID:      cacheRequest.ID(),
			err:                 cacheRespError,
		}

		cacheRequest.Processor().ProcessCacheResponse(cacheResponse)
		receiver.CleanupCacheRequestSubscriptions(cacheRequest)
		if messageFilter := cacheRequest.MessageFilter(); messageFilter != nil {
			/* NOTE: Not all cache requests require filtering. So, we only cleanup resources associated with
			 * message filtering if the cache request was configured to require that they be allocated.
			 */
			(*messageFilter).CleanupFiltering()
		}
	}
	/* Lifecycle management of cache sessions */
	/* NOTE: In the event of a duplicate event in the cacheRequestMap channel, the following deletion
	 * will not panic. */
	cacheRequestMap.Delete(cacheRequestIndex)
	if errorInfo := cacheSession.DestroyCacheSession(); errorInfo != nil {
		/* NOTE: If we can't destroy the cache session, there is no follow up action that can be taken, so
		 * there is no point in returning an error. We just log it and move on. */
		logging.Default.Error(fmt.Sprintf("%s %s %s and %s 0x%x. ErrorInfo is: [%s]", constants.FailedToDestroyCacheSession, constants.WithCacheSessionPointer, cacheSession.String(), constants.WithCacheRequestID, cacheRequestIndex, errorInfo.GetMessageAsString()))
	}
}

// CancelPendingCacheRequests will cancel all pending cache requests for a given cache session and potentially block
// until all cancellations are pushed to the cacheResponse channel.
// func (receiver *ccsmpBackedReceiver) CancelPendingCacheRequests(cacheRequestIndex CacheRequestMapIndex, cacheResponseProcessor CacheResponseProcessor) *CoreCacheEventInfo {
func (receiver *ccsmpBackedReceiver) CancelPendingCacheRequests(cacheRequestIndex CacheRequestMapIndex, cacheRequest CacheRequest) *CoreCacheEventInfo {
	var generatedEvent CoreCacheEventInfo
	cacheSession := GetCacheSessionFromCacheRequestIndex(cacheRequestIndex)
	errorInfo := cacheSession.CancelCacheRequest()
	if errorInfo != nil {
		if errorInfo.ReturnCode != ccsmp.SolClientReturnCodeOk {
			/* There was a failure in cancelling the cache request, but we still
			 * have a cache session pointer, so we generate a cache response to notify
			 * the application that something went wrong and defer destroying the cache
			 * session to a later point.*/
			logging.Default.Info(fmt.Sprintf("Failed to cancel cache request %s %d and %s %s.", constants.WithCacheRequestID, cacheRequest.ID(), constants.WithCacheSessionPointer, cacheSession.String()))
			//logging.Default.Info(fmt.Sprintf("Failed to cancel cache request %s %d and %s %s.", constants.WithCacheRequestID, cacheResponseProcessor.GetCacheRequestInfo().GetCacheRequestID(), constants.WithCacheSessionPointer, cacheSession.String()))
			if logging.Default.IsDebugEnabled() {
				logging.Default.Debug("Attempting to generate a cache request cancellation event now.")
			}

			generatedEvent = ccsmp.NewCacheEventInfoForCancellation(cacheSession, cacheRequest.ID(), cacheRequest.RequestConfig().GetName(), ToNativeError(errorInfo, "Failed to cancel cache request."))
			//generatedEvent = ccsmp.NewCacheEventInfoForCancellation(cacheSession, cacheResponseProcessor.GetCacheRequestInfo().GetCacheRequestID(), cacheResponseProcessor.GetCacheRequestInfo().GetTopic(), ToNativeError(errorInfo, "Failed to cancel cache request."))
		}
	}
	return &generatedEvent
}

// CacheRequestMapIndex is used as the index in the map between cache sessions and cache response processors in the
// receiver.
type CacheRequestMapIndex = ccsmp.SolClientCacheSessionPt

// CoreCacheEventInfo is a type alias for the Go representation of the cache event info returned to the API from CCSMP
// in response to a cache request concluding.
type CoreCacheEventInfo = ccsmp.CacheEventInfo

type CoreCacheSession = ccsmp.SolClientCacheSession

// CoreCacheEventCallback is a type alias for the callback that CCSMP will call on the context thread to pass the
// cache event info corresponding to a cache response to the Go API.
type CoreCacheEventCallback = ccsmp.SolClientCacheEventCallback

type CacheRequest interface {
	// RequestConfig returns the [resource.CachedMessageSubscriptionRequest] that was configured by the application
	// for this cache request.
	RequestConfig() resource.CachedMessageSubscriptionRequest
	// ID returns the [CacheRequestID] that was specified for this cache request by the application.
	ID() apimessage.CacheRequestID
	// Processor returns the method through which the application decided to handle the cache response that will result
	// from this cache request.
	Processor() CacheResponseProcessor
	// CacheSession returns the [CoreCacheSession] that was created to service this cache request.
	CacheSession() CoreCacheSession
	// Index returns the [CacheRequestMapIndex] used to associate this cache request with its processor in the
	// receiver's internal map.
	Index() CacheRequestMapIndex
	// UsesLocalDispatch returns whether or not the [CacheRequest] uses local dispatch for subscription management.
	UsesLocalDispatch() bool
	// DispatchId returns the dispatchId used to send this [CacheRequest]
	DispatchId() uintptr
	// MessageFilter returns the filter used to filter received messages. If the cache request has not configured a filter,
	// this method returns nil.
	MessageFilter() *ReceivedMessageFilter
}

type MessageFilterConfig = ccsmp.SolClientMessageFilteringConfigPt

var defaultMessageFilterConfigValue MessageFilterConfig = nil

type cacheRequestMessageFilter struct {
	pointer        MessageFilterConfig
	cacheRequestId message.CacheRequestID
	dispatchId     uintptr
	newField       uintptr
}

func (filter *cacheRequestMessageFilter) Filter() MessageFilterConfig {
	fmt.Printf("Filter() called, &filter is 0x%x\n", &filter)
	fmt.Printf("Filter() called, filter.pointer is 0x%x\n", filter.pointer)
	fmt.Printf("Filter() called, filter.newField is 0x%x\n", filter.newField)
	return filter.pointer
}

func (filter *cacheRequestMessageFilter) SetupFiltering() {
	fmt.Printf("SetupFiltering::filter.pointer before init is 0x%x\n", uintptr(unsafe.Pointer(filter.pointer)))
	filter.pointer = ccsmp.AllocMessageFilteringConfigForCacheRequests(filter.pointer, filter.dispatchId, filter.cacheRequestId)
	filter.newField = uintptr(unsafe.Pointer(filter.pointer))
	fmt.Printf("SetupFiltering::filter.newField after init is 0x%x\n", filter.newField)
	fmt.Printf("SetupFiltering::filter.pointer after init is 0x%x\n", uintptr(unsafe.Pointer(filter.pointer)))
	fmt.Printf("SetupFiltering::&filter.pointer after init is 0x%x\n", uintptr(unsafe.Pointer(&filter.pointer)))
	//fmt.Printf("SetupFiltering::ret after init is 0x%x\n", uintptr(unsafe.Pointer(ret)))
}

func (filter *cacheRequestMessageFilter) CleanupFiltering() {
	ccsmp.CleanMessageFilteringConfigForCacheRequests(filter.pointer)
}

func newCacheRequestMessageFilter(cacheRequestId message.CacheRequestID, dispatchId uintptr) *cacheRequestMessageFilter {
	fmt.Printf("Creating new cache request message filter\n")
	return &cacheRequestMessageFilter{
		pointer:        defaultMessageFilterConfigValue,
		cacheRequestId: cacheRequestId,
		dispatchId:     dispatchId,
	}
}

type ReceivedMessageFilter interface {
	// SetupFiltering allocates and configures any resources associated with filtering received messages.
	SetupFiltering()
	// CleanupFiltering cleans any resources associated with filtering received messages.
	CleanupFiltering()
	// Filter() returns the configured filter that is applied to received messages
	Filter() MessageFilterConfig
}

type CacheRequestImpl struct {
	cachedMessageSubscriptionRequest resource.CachedMessageSubscriptionRequest
	cacheRequestID                   apimessage.CacheRequestID
	cacheResponseProcessor           CacheResponseProcessor
	cacheSession                     ccsmp.SolClientCacheSession
	index                            CacheRequestMapIndex
	isLocalDispatch                  bool
	dispatchId                       uintptr
	messageFilter                    ReceivedMessageFilter
}

func GetCacheSessionFromCacheRequestIndex(cacheRequestMapIndex CacheRequestMapIndex) CoreCacheSession {
	return ccsmp.WrapSolClientCacheSessionPt(ccsmp.SolClientCacheSessionPt(cacheRequestMapIndex))
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

func (cacheRequest *CacheRequestImpl) UsesLocalDispatch() bool {
	strategy := cacheRequest.cachedMessageSubscriptionRequest.GetCachedMessageSubscriptionRequestStrategy()
	return ccsmp.CachedMessageSubscriptionRequestStrategyMappingToCCSMPSubscribeFlags[*strategy] == ccsmp.LocalDispatchFlags()
}

func (cacheRequest *CacheRequestImpl) DispatchId() uintptr {
	return cacheRequest.dispatchId
}

func (cacheRequest *CacheRequestImpl) MessageFilter() *ReceivedMessageFilter {
	if cacheRequest.messageFilter != nil {
		return &cacheRequest.messageFilter
	}
	return nil
}

func NewCacheRequest(cachedMessageSubscriptionRequest resource.CachedMessageSubscriptionRequest, cacheRequestID apimessage.CacheRequestID, cacheResponseHandler CacheResponseProcessor, cacheSession ccsmp.SolClientCacheSession, dispatchId uintptr) CacheRequest {
	cacheRequestImpl := CacheRequestImpl{
		cachedMessageSubscriptionRequest: cachedMessageSubscriptionRequest,
		cacheRequestID:                   cacheRequestID,
		cacheResponseProcessor:           cacheResponseHandler,
		cacheSession:                     cacheSession,
		index:                            GetCacheRequestMapIndexFromCacheSession(cacheSession),
		dispatchId:                       dispatchId,
	}
	if cacheRequestImpl.UsesLocalDispatch() {
		fmt.Printf("Using local dispatch, creating message filter\n")
		cacheRequestImpl.messageFilter = newCacheRequestMessageFilter(cacheRequestID, dispatchId)
		fmt.Printf("Using local dispatch, configuring filter.\n")
		cacheRequestImpl.messageFilter.SetupFiltering()
		fmt.Printf("NewCacheRequest::&messageFilter is 0x%x\n", &cacheRequestImpl.messageFilter)
		fmt.Printf("Using local dispatch, filterConfig after setup is 0x%x\n", (*cacheRequestImpl.MessageFilter()).Filter())
	}
	return &cacheRequestImpl
}

func (receiver *ccsmpBackedReceiver) CreateCacheRequest(cachedMessageSubscriptionRequest resource.CachedMessageSubscriptionRequest, cacheRequestID apimessage.CacheRequestID, cacheResponseHandler CacheResponseProcessor, dispatchId uintptr) (CacheRequest, error) {
	/* create cache session */
	propsList := ccsmp.ConvertCachedMessageSubscriptionRequestToCcsmpPropsList(cachedMessageSubscriptionRequest)

	cacheSession, errInfo := receiver.session.CreateCacheSession(propsList)

	if errInfo != nil {
		errorString := fmt.Sprintf("Failed to create cache session %s %d", constants.WithCacheRequestID, cacheRequestID)
		logging.Default.Warning(errorString)
		return nil, ToNativeError(errInfo, errorString)
	}
	if logging.Default.IsDebugEnabled() {
		logging.Default.Debug(fmt.Sprintf("Created cache session %s", cacheSession.String()))
	}
	cacheRequest := NewCacheRequest(cachedMessageSubscriptionRequest, cacheRequestID, cacheResponseHandler, cacheSession, dispatchId)
	//cacheRequestReceivedMessageFilter := cacheRequest.(ReceivedMessageFilter)
	//cacheRequestReceivedMessageFilter.SetupFiltering()
	return cacheRequest, nil

}

func (receiver *ccsmpBackedReceiver) DestroyCacheRequest(cacheRequest CacheRequest) error {
	var err error
	/* NOTE: We do not cancel any cache requests here in constrast to other failure paths that clean up the cache
	 * session, such as in Terminate/teardownCache, because this method is intended to be used to clean up the
	 * cache session and related resources only after a failed call to SendCacheRequest().
	 */
	cacheSession := cacheRequest.CacheSession()
	if messageFilter := cacheRequest.MessageFilter(); messageFilter != nil {
		/* NOTE: Not all cache requests require filtering. So, we only cleanup resources associated with
		 * message filtering if the cache request was configured to require that they be allocated.
		 */
		(*messageFilter).CleanupFiltering()
	}
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
	// CreateCacheRequest creates a cache session and a CacheRequest object which contains the new session along with
	// whatever information is required to send the cache request.
	CreateCacheRequest(resource.CachedMessageSubscriptionRequest, apimessage.CacheRequestID, CacheResponseProcessor, uintptr) (CacheRequest, error)
	// DestroyCacheRequest is used to clean up a cache request object when SendCacheRequestFails
	DestroyCacheRequest(CacheRequest) error
	// SendCacheRequest sends the given cache request object, and configures CCSMP to use the given callback to handle
	// the resulting cache event/response
	SendCacheRequest(CacheRequest, CoreCacheEventCallback, uintptr) error
	// ProcessCacheEvent creates a cache response from the cache event that was asynchronously returned by CCSMP, and
	// gives this response to the application for post-processing using the method configured by the application during
	// the call to RequestCachedAsync or RequestCachedAsyncWithCallback.
	ProcessCacheEvent(*sync.Map, CoreCacheEventInfo)
	// CleanupCacheRequestSubscriptions cleans up subscriptions that are intended to persist only for the lifetime of
	// cache request. Currently, this applies only to CachedOnly cache requests, which use only local dispatch to
	// forward messages to the appropriate consumer callback.
	CleanupCacheRequestSubscriptions(CacheRequest) error
	// CancelPendingCacheRequests Cancels all the cache requests for the cache session associated with the given
	// CacheRequestMapIndex
	CancelPendingCacheRequests(CacheRequestMapIndex, CacheRequest) *CoreCacheEventInfo
	//CancelPendingCacheRequests(CacheRequestMapIndex, CacheResponseProcessor) *CoreCacheEventInfo
}

func (receiver *ccsmpBackedReceiver) CleanupCacheRequestSubscriptions(cacheRequest CacheRequest) error {
	if cacheRequest.UsesLocalDispatch() {
		var messageFilter MessageFilterConfig
		if filter := cacheRequest.MessageFilter(); filter != nil {
			messageFilter = (*filter).Filter()
		} else {
			errorString := "API tried to clean up subscription for cache request that was configured for local dispatch but that did not have a configured message filter. Unable to cleanup subscription so exiting."
			logging.Default.Info(errorString)
			return solace.NewError(&solace.InvalidConfigurationError{}, errorString, nil)
		}
		errInfo := receiver.session.UnsubscribeFromCacheRequestTopic(cacheRequest.RequestConfig().GetCacheName(),
			ccsmp.CachedMessageSubscriptionRequestStrategyMappingToCCSMPSubscribeFlags[*cacheRequest.RequestConfig().GetCachedMessageSubscriptionRequestStrategy()],
			uintptr(unsafe.Pointer(messageFilter)),
			uintptr(0))
		if errInfo != nil {
			if logging.Default.IsDebugEnabled() {
				logging.Default.Debug(fmt.Sprintf("Got error [%s] when trying to unsubscribe from cache topic [%s]", errInfo.String(), cacheRequest.RequestConfig().GetCacheName()))
			}
			return ToNativeError(errInfo)
		}
	}
	return nil
}

// SendCacheRequest sends a creates a cache session and sends a cache request on that session. This method
// assumes the receiver is in the proper state (running). The caller must guarantee this state before
// attempting to send a cache request. Failing to do so will result in undefined behaviour.
// dispatchID needs to be passed because the [DirectMessageReceiver] posesses a dispatch ID that can be
// different from the one maintained by the [ccsmpBackedReceiver]. When the [DirectMessageReceiver] is started,
// it increments the service-wide dispatchID that is maintained by the [ccsmpBackedReceiver], and maintains a
// copy of that dispatchID for the remainder of its lifecycle. It is not documented that this copy of the
// dispatchID is required to be immutable, and it is the [DirectMessageReceiver]'s responsibility to
// maintain it across network operations. If the dispatchID were not passed, and the
// [ccsmpBackedReceiver.dispatchID] was used instead, then the data messages from the cache response would be
// forwarded to the last created [DirectMessageReceiver] instead of to the one which sent this cache request.
func (receiver *ccsmpBackedReceiver) SendCacheRequest(cacheRequest CacheRequest, cacheEventCallback CoreCacheEventCallback, dispatchID uintptr) error {
	var err error

	cacheSession := cacheRequest.CacheSession()
	cacheStrategy := cacheRequest.RequestConfig().GetCachedMessageSubscriptionRequestStrategy()
	if cacheStrategy == nil {
		errorString := fmt.Sprintf("%s %s %d and %s %s because an invalid CachedMessageSubscriptionStrategy was passed", constants.FailedToSendCacheRequest, constants.WithCacheRequestID, cacheRequest.ID(), constants.WithCacheSessionPointer, cacheSession.String())
		logging.Default.Warning(errorString)
		return solace.NewError(&solace.IllegalArgumentError{}, errorString, nil)
	}
	if logging.Default.IsDebugEnabled() {
		logging.Default.Debug(fmt.Sprintf("Sending cache request with cache request ID %d and dispatchID 0x%x on cache session %s", cacheRequest.ID(), dispatchID, cacheSession.String()))
	}

	var filterConfig MessageFilterConfig = defaultMessageFilterConfigValue
	if messageFilter := cacheRequest.MessageFilter(); messageFilter != nil {
		fmt.Printf("messageFilter is 0x%x\n", messageFilter)
		filterConfig = (*messageFilter).Filter()
	}
	fmt.Printf("filterConfig is 0x%x\n", uintptr(unsafe.Pointer(filterConfig)))

	errInfo := cacheSession.SendCacheRequest(uintptr(dispatchID),
		cacheRequest.RequestConfig().GetName(),
		cacheRequest.ID(),
		ccsmp.CachedMessageSubscriptionRequestStrategyMappingToCCSMPCacheRequestFlags[*cacheStrategy],
		ccsmp.CachedMessageSubscriptionRequestStrategyMappingToCCSMPSubscribeFlags[*cacheStrategy],
		cacheEventCallback,
		filterConfig)
	if errInfo != nil {
		errorString := fmt.Sprintf("%s %s %d and %s %s. Related errInfo was %s", constants.FailedToSendCacheRequest, constants.WithCacheRequestID, cacheRequest.ID(), constants.WithCacheSessionPointer, cacheSession.String(), errInfo.String())
		logging.Default.Warning(errorString)
		return ToNativeError(errInfo, errorString)
	}

	logging.Default.Debug("Sent cache request")
	return err
}
