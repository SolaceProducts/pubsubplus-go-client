// pubsubplus-go-client
//
// Copyright 2021-2024 Solace Corporation. All rights reserved.
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

package receiver

import (
    "fmt"

	"solace.dev/go/messaging/internal/ccsmp"
	"solace.dev/go/messaging/internal/impl/constants"
	"solace.dev/go/messaging/internal/impl/logging"
	"solace.dev/go/messaging/pkg/solace"
	"solace.dev/go/messaging/pkg/solace/message"
)

const UnableToPassCacheResponseToApplication = "Unable to pass cache response to application because: "
const NoCacheChannelAvailable = "The API failed to retrieve the configured channel that was intended for the application"
const UnableToRunApplicationCacheCallback = "Unable to run the cache response callback given by the application because: "
const NoCacheCallbackAvailable = "The application did not provide a callback that could be used to process the cache response."
const UnableToProcessCacheResponse = "Unable to process cache response because: "
const InvalidCacheSession = "The cache session associated with the given cache request/response was invalid"

// PollAndProcessCacheResponseChannel is intended to be run as a go routine.
func (receiver * directMessageReceiverImpl) PollAndProcessCacheResponseChannel() {
        receiver.cachePollingRunning.Store(true)
        var cacheEventInfo ccsmp.SolClientCacheEventInfo
        channelIsOpen := true
        /* poll cacheventinfo channel */
        for channelIsOpen {
                cacheEventInfo, channelIsOpen = <- receiver.cacheResponseChan
                if !channelIsOpen {
                        // If channel is closed, we can stop polling. In this case we don't need to handle
                        // the cacheEventInfo since there won't be a menaingful one left on the queue.
                        // Any function that closes the channel must guarantee this.
                        /* TODO: This may need to be reworked? */
                        break
                }
                /* We decrement the counter first, since as soon as we pop the SolClientCacheEventInfo
                 * off the channel, CCSMP is able to put another on. If CCSMP is able resume processing the
                 * cache responses, we should unblock the application by allowing it to submit more cache
                 * requests ASAP.*/
                receiver.cacheResponseChanCounter.Add(-1)
                receiver.ProcessCacheEvent(cacheEventInfo)
        }
        // Indicate that this function has stopped running.
        receiver.cachePollingRunning.Store(false)
}

/* TODO: This should probably be refactored to the direct_message_receiver_impl module */
// ProcessCacheEvent is intended to be run by any agent trying to process a cache response. This can be run from a polling go routine, or during termination to cleanup remaining resources, and possibly by other agents as well.
func (receiver *directMessageReceiverImpl) ProcessCacheEvent(cacheEventInfo ccsmp.SolClientCacheEventInfo) {
        cacheSessionP := cacheEventInfo.GetCacheSessionPointer()
        /* TODO: Make sure there are no mutations to the map during this read.*/
        if cacheResponseHolder, found := receiver.cacheSessionMap.Load(cacheSessionP); !found {
                if receiver.logger.IsDebugEnabled() {
                        /* TODO: refactor consts to separate file */
                        /* This can occur when there has been a duplicate event, where for some reason CCSMP was able
                         * produce an event, but PSPGo thought CCSMP was not, so PSPGo generated an event on CCSMP's
                         * behalf, but after CCSMP's event was put on the channel. This would result in the CCSMP-
                         * generated event being processed and its cache session pointer being removed from the tabel
                         * and the duplicate event that was processed afterwards having the same cache session pointer,
                         * but no matching entry in the table since it was already removed by the original entry. This
                         * is not a bug, and the application doesn't need to be concerned about this, so we log it as
                         * debug. */
                        receiver.logger.Debug(UnableToProcessCacheResponse + InvalidCacheSession)
                }
        } else {
                cacheResponseHolder.(CacheResponseProcessor).ProcessCacheResponse()
        }
        /* Lifecycle management of cache sessions */
        /* NOTE: In the event of a duplicate event in the receiver.cacheResponseChan channel, the following deletion
         * will not panic. */
        receiver.cacheSessionMap.Delete(cacheSessionP)
        if errorInfo := ccsmp.DestroyCacheSession(cacheSessionP); errorInfo != nil {
                /* NOTE: If we can't destroy the cache session, there is no follow up action that can be taken, so
                 * there is no point in returning an error. We just log it and move on. */
                 receiver.logger.Error(fmt.Sprintf("%s %s and %s. ErrorInfo is: [%s]", constants.FailedToDestroyCacheSession, constants.WithCacheSessionPointer, constants.WithCacheRequestID, errorInfo.GetMessageAsString()))
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
        cacheRequestID message.CacheRequestID
        topic string
}

func NewCacheRequestInfo(cacheRequestID message.CacheRequestID, topic string) CacheRequestInfo {
        return CacheRequestInfo{
                cacheRequestID: cacheRequestID,
                topic: topic,
        }
}

func (i * CacheRequestInfo) GetTopic() string {
        return i.topic
}

func (i * CacheRequestInfo) GetCacheRequestID() message.CacheRequestID {
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
        ProcessCacheResponse()

        // GetCacheRequestInfo returns the original information that was used to send the cache request.
        // This is useful for comparing a received cache response during processing, or for adding
        // information to logging or error messages when this information cannot be retrieved from the
        // cache response.
        GetCacheRequestInfo() * CacheRequestInfo
}

// CacheResponseCallbackHolder holds an application-provided callback that is responsible for post-processing the cache
// response. CacheResponseCallbackHolder implements the CacheResponseProcessor interface to allow safe access of this
// callback when being retrieved from a map of heterogeneous values.
type CacheResponseCallbackHolder struct {
        CacheResponseProcessor
        cacheRequestInfo CacheRequestInfo
        callback func(solace.CacheResponse)
}

func NewCacheResponseCallbackHolder(callback func(solace.CacheResponse), cacheRequestInfo CacheRequestInfo) CacheResponseCallbackHolder {
        return CacheResponseCallbackHolder{
                cacheRequestInfo: cacheRequestInfo,
                callback: callback,
        }
}

func (cbHolder CacheResponseCallbackHolder) GetCallback() (func(solace.CacheResponse), bool) {
        return cbHolder.callback, true
}

func (cbHolder CacheResponseCallbackHolder) GetChannel() (chan solace.CacheResponse, bool) {
        return nil, false
}

func (cbHolder CacheResponseCallbackHolder) ProcessCacheResponse() {
        if callback, found := cbHolder.GetCallback(); found {
                cacheResponse := solace.CacheResponse{}
                callback(cacheResponse)
        } else {
                logging.Default.Error(UnableToPassCacheResponseToApplication + NoCacheCallbackAvailable)
        }
}

func (cbHolder CacheResponseCallbackHolder) GetCacheRequestInfo() * CacheRequestInfo {
        return &cbHolder.cacheRequestInfo
}

// CacheResponseChannelHolder holds a API-provided channel to which the cache reponse will be pushed.
// CacheResponseChannelHolder implements the CacheResponseProcessor interface to allow safe access of this callback
// when being retrieved from a map of heterogeneous values.
type CacheResponseChannelHolder struct {
        CacheResponseProcessor
        cacheRequestInfo CacheRequestInfo
        channel chan solace.CacheResponse
}

func NewCacheResponseChannelHolder(channel chan solace.CacheResponse, cacheRequestInfo CacheRequestInfo) CacheResponseChannelHolder {
        return CacheResponseChannelHolder{
                cacheRequestInfo: cacheRequestInfo,
                channel: channel,
        }
}

func (chHolder CacheResponseChannelHolder) GetCallback() (func(solace.CacheResponse), bool) {
        return nil, false
}

func (chHolder CacheResponseChannelHolder) GetChannel() (chan solace.CacheResponse, bool) {
        return chHolder.channel, true
}

func (chHolder CacheResponseChannelHolder) ProcessCacheResponse() {
        /* Because function pointers and channels are both pointer types, they could be nil. So, we should
         * check to make sure that they are not. There could be an error where the API did not create the
         * correct holder type, which would cause the holder's value to be nil and the API would panic.*/
         if channel, found := chHolder.GetChannel(); found {
                cacheResponse := solace.CacheResponse{}
                /* This will not block because the channel is created with a buffer size of 1 in RequestCachedAsync() */
                channel <- cacheResponse
                close(channel)
        } else {
                /* This is an error log because it is the API's responsiblity to create and manage the channel. */
                logging.Default.Error(UnableToPassCacheResponseToApplication + NoCacheChannelAvailable)
        }
}

func (chHolder CacheResponseChannelHolder) GetCacheRequestInfo() * CacheRequestInfo {
        return &chHolder.cacheRequestInfo
}
