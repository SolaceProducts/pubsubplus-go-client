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
	apimessage "solace.dev/go/messaging/pkg/solace/message"
	"solace.dev/go/messaging/pkg/solace/resource"
)

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

// CancelPendingCacheRequests will cancel all pending cache requests for a given cache session and potentially block
// until all cancellations are pushed to the cacheResponse channel.
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
			if logging.Default.IsDebugEnabled() {
				logging.Default.Debug("Attempting to generate a cache request cancellation event now.")
			}

			generatedEvent = ccsmp.NewCacheEventInfoForCancellation(cacheSession, cacheRequest.ID(), cacheRequest.RequestConfig().GetName(), ToNativeError(errorInfo, "Failed to cancel cache request."))
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

func (receiver *ccsmpBackedReceiver) CreateCacheRequest(cachedMessageSubscriptionRequest resource.CachedMessageSubscriptionRequest, cacheRequestID apimessage.CacheRequestID, cacheResponseHandler CacheResponseProcessor, dispatchID uintptr) (CacheRequest, error) {
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
	cacheRequest := NewCacheRequest(cachedMessageSubscriptionRequest, cacheRequestID, cacheResponseHandler, cacheSession, dispatchID)
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
		filterConfig = (*messageFilter).Filter()
	}

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
