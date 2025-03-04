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
	"solace.dev/go/messaging/pkg/solace/subcode"
)

// CacheResponseProcessor provides an interface through which the information necessary to process a cache response
// that is passed from CCSMP can be acquired.
type CacheResponseProcessor interface {
	// ProcessCacheResponse processes the cache response according to the implementation
	ProcessCacheResponse(solace.CacheResponse)
}

// cacheResponseProcessor holds an application-provided callback that is responsible for post-processing the cache
// response. cacheResponseProcessor implements the CacheResponseProcessor interface to allow safe access of this
// callback when being retrieved from a map of heterogeneous values.
type cacheResponseProcessor struct {
	CacheResponseProcessor
	callback func(solace.CacheResponse)
}

func NewCacheResponseProcessor(callback func(solace.CacheResponse)) cacheResponseProcessor {
	return cacheResponseProcessor{
		callback: callback,
	}
}

func (crp cacheResponseProcessor) ProcessCacheResponse(cacheResponse solace.CacheResponse) {
	crp.callback(cacheResponse)
}

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
