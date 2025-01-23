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
	"solace.dev/go/messaging/internal/impl/logging"
	"solace.dev/go/messaging/pkg/solace"
	apimessage "solace.dev/go/messaging/pkg/solace/message"
)

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
		logging.Default.Error("Unable to pass cache response to application because the application did not provide a callback that could be used to process the cache response.")
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
		logging.Default.Error("The API failed to retrieve the configured channel that was intended for the application because no cache channel was available.")
	}
}

func (chHolder CacheResponseChannelHolder) GetCacheRequestInfo() *CacheRequestInfo {
	return &chHolder.cacheRequestInfo
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
