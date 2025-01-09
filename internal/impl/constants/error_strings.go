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

package constants

// Constants used for various error messages should go here
// Note that errors in golang by convention start with a lowercase letter and do not end in punctuation

// UnableToModifyPropertyOfDisconnectedService error string
const UnableToModifyPropertyOfDisconnectedService = "unable to modify a property of a service that is disconnected"

// UnableToModifyNonModifiableGivenServiceProperty error string
const UnableToModifyNonModifiableGivenServiceProperty = "unable to modify the non-modifiable given service property: %s"

// UnableToConnectAlreadyDisconnectedService error string
const UnableToConnectAlreadyDisconnectedService = "unable to connect messaging service in state %s"

// UnableToConnectAlreadyDisposedService error string
const UnableToConnectAlreadyDisposedService = "unable to connect messaging service after it has already been disposed of"

// UnableToDisconnectUnstartedService error string
const UnableToDisconnectUnstartedService = "unable to disconnect messaging service in state %s"

// UnableToPublishAlreadyTerminated error string
const UnableToPublishAlreadyTerminated = "unable to publish message: message publisher has been terminated"

// UnableToPublishNotStarted error string
const UnableToPublishNotStarted = "unable to publish message: message publisher is not started. current state: "

// UnableToTerminatePublisher error string
const UnableToTerminatePublisher = "cannot terminate the publisher as it has not been started"

// UnableToStartPublisher error string
const UnableToStartPublisher = "cannot start the publisher as it has already been terminated"

// RequestReplyPublisherCannotReceiveReplyAlreadyTerminated error string
const RequestReplyPublisherCannotReceiveReplyAlreadyTerminated = "publisher has been terminated, no reply messages to receive"

// RequestReplyPublisherTimedOutWaitingForReply error string
const RequestReplyPublisherTimedOutWaitingForReply = "timed out waiting for reply message for request publish"

// UnableToStartPublisherParentServiceNotStarted error string
const UnableToStartPublisherParentServiceNotStarted = "cannot start publisher unless parent MessagingService is connected"

// UnableToTerminateReceiver error string
const UnableToTerminateReceiver = "cannot terminate the receiver as it has not been started"

// UnableToStartReceiver error string
const UnableToStartReceiver = "cannot start the receiver as it has already been terminated"

// UnableToStartReceiverParentServiceNotStarted error string
const UnableToStartReceiverParentServiceNotStarted = "cannot start receiver unless parent MessagingService is connected"

// UnableToSettleAlreadyTerminated error string
const UnableToSettleAlreadyTerminated = "unable to settle message: message receiver has been terminated"

// UnableToSettleNotStarted error string
const UnableToSettleNotStarted = "unable to settle meessage: message receiver is not yet started"

// InvalidMessageSettlementOutcome error string
const InvalidMessageSettlementOutcome = "invalid message settlement outcome used to settle message"

// UnableToModifySubscriptionBadState error string
const UnableToModifySubscriptionBadState = "unable to modify subscriptions in state %s"

// DirectReceiverUnsupportedSubscriptionType error string
const DirectReceiverUnsupportedSubscriptionType = "DirectMessageReceiver does not support subscriptions of type %T"

// ReceiverTimedOutWaitingForMessage error string
const ReceiverTimedOutWaitingForMessage = "timed out waiting for message on call to Receive"

// ReceiverCannotReceiveNotStarted error string
const ReceiverCannotReceiveNotStarted = "receiver has not yet been started, no messages to receive"

// ReceiverCannotReceiveAlreadyTerminated error string
const ReceiverCannotReceiveAlreadyTerminated = "receiver has been terminated, no messages to receive"

// PersistentReceiverUnsupportedSubscriptionType error string
const PersistentReceiverUnsupportedSubscriptionType = "PersistentMessageReceiver does not support subscriptions of type %T"

// PersistentReceiverMissingQueue error string
const PersistentReceiverMissingQueue = "queue must be provided when building a new PersistentMessageReceiver"

// PersistentReceiverCannotPauseBadState error string
const PersistentReceiverCannotPauseBadState = "cannot pause message receiption when not in started state"

// PersistentReceiverCannotUnpauseBadState error string
const PersistentReceiverCannotUnpauseBadState = "cannot resume message receiption when not in started state"

// PersistentReceiverMustSpecifyRGMID error string
const PersistentReceiverMustSpecifyRGMID = "must specify ReceiverPropertyPersistentMessageReplayStrategyIDBasedReplicationGroupMessageID when replay from message ID is selected"

// PersistentReceiverMustSpecifyTime error string
const PersistentReceiverMustSpecifyTime = "must specify ReceiverPropertyPersistentMessageReplayStrategyTimeBasedStartTime when replay from time is selected"

// WouldBlock error string
const WouldBlock = "buffer is full, cannot queue additional messages"

// UnableToRetrieveMessageID error string
const UnableToRetrieveMessageID = "unable to retrieve message ID from message, was the message received by a persistent receiver?"

// IncompleteMessageDeliveryMessage error string
const IncompleteMessageDeliveryMessage = "failed to publish messages, publisher terminated with %d undelivered messages"

// IncompleteMessageDeliveryMessageWithUnacked error string
const IncompleteMessageDeliveryMessageWithUnacked = "failed to publish messages, publisher terminated with %d undelivered messages and %d unacknowledged messages"

// IncompleteMessageReceptionMessage error string
const IncompleteMessageReceptionMessage = "failed to dispatch messages, receiver terminated with %d undelivered messages"

// LoginFailure error string
const LoginFailure = "failed to authenticate with the broker: "

// UnresolvedSession error string
const UnresolvedSession = "requested service is unreachable: "

// DirectReceiverBackpressureMustBeGreaterThan0 error string
const DirectReceiverBackpressureMustBeGreaterThan0 = "direct receiver backpressure buffer size must be > 0"

// UnableToRegisterCallbackReceiverTerminating error string
const UnableToRegisterCallbackReceiverTerminating = "cannot register message handler, receiver is not running"

// FailedToAddSubscription error string
const FailedToAddSubscription = "failed to add subscription to receiver: "

// InvalidUserPropertyDataType error string
const InvalidUserPropertyDataType = "type %T at key %s is not supported for user data"

// InvalidOutboundMessageType error string
const InvalidOutboundMessageType = "got an invalid OutboundMessage, was it built by OutboundMessageBuilder? backing type %T"

// InvalidInboundMessageType error string
const InvalidInboundMessageType = "got an invalid InboundMessage, was it received with a receiver? backing type %T"

// TerminatedOnMessagingServiceShutdown error string
const TerminatedOnMessagingServiceShutdown = "terminated due to MessagingService disconnect"

// ShareNameMustNotBeEmpty error string
const ShareNameMustNotBeEmpty = "share name must not be empty"

// ShareNameMustNotContainInvalidCharacters error string
const ShareNameMustNotContainInvalidCharacters = "share name must not contain literals '*' or '>'"

// MissingServiceProperty error string
const MissingServiceProperty = "property %s is required to build a messaging service"

// MissingServicePropertyGivenScheme error string
const MissingServicePropertyGivenScheme = "property %s is required when using authentication scheme %s"

// MissingServicePropertiesGivenScheme error string
const MissingServicePropertiesGivenScheme = "one of properties %s is required when using authentication scheme %s"

// CouldNotCreateRGMID error string
const CouldNotCreateRGMID = "could not create ReplicationGroupMessageID from string: %s"

// CouldNotCompareRGMID error string
const CouldNotCompareRGMID = "could not compare ReplicationGroupMessageIDs: %s"

// CouldNotCompareRGMIDBadType error string
const CouldNotCompareRGMIDBadType = "could not compare with ReplicationGroupMessageID of type %T"

// CouldNotConfirmSubscriptionServiceUnavailable error string
const CouldNotConfirmSubscriptionServiceUnavailable = "could not confirm subscription, the messaging service was terminated"

// InvalidConfiguration error string
const InvalidConfiguration = "invalid configuration provided: "

// MissingReplyMessageHandler error string
const MissingReplyMessageHandler = "got nil ReplyMessageHandler, ReplyMessageHandler is required for Publish"

// ReplierFailureToPublishReply error string
const ReplierFailureToPublishReply = "Publish Reply Error: "

// FailedToProvisionEndpoint error string
const FailedToProvisionEndpoint = "failed to provision endpoint: "

// FailedToDeprovisionEndpoint error string
const FailedToDeprovisionEndpoint = "failed to deprovision endpoint: "

// UnableToProvisionParentServiceNotStarted error string
const UnableToProvisionParentServiceNotStarted = "cannot provision endpoint unless parent MessagingService is connected"

// UnableToDeprovisionParentServiceNotStarted error string
const UnableToDeprovisionParentServiceNotStarted = "cannot deprovision endpoint unless parent MessagingService is connected"

// CouldNotConfirmProvisionDeprovisionServiceUnavailable error string
const CouldNotConfirmProvisionDeprovisionServiceUnavailable = "could not confirm provision/deprovision, the messaging service was terminated"

// FailedToCreateCacheSession error string
const FailedToCreateCacheSession = "Failed to create cache session"

// WithCacheRequestID error string
const WithCacheRequestID = "with cache request ID:"

// FailedToSendCacheRequest error string
const FailedToSendCacheRequest = "Failed to send cache request"

// FailedToRetrieveChannel error string
const FailedToRetrieveChannel = "Failed to retrieve channel"

// FailedToRetrieveCallback error string
const FailedToRetrieveCallback = "Failed to retrieve callback"

// FailedToCancelCacheRequest error String
const FailedToCancelCacheRequest = "Failed to cancel cache request"

// WithCacheSessionPointer error string
const WithCacheSessionPointer = "with cache session pointer:"

// AttemptingCancellationNoticeGeneration error string
const AttemptingCancellationNoticeGeneration = "Attempting to generate a cache request cancellation event now."

// FailedToDestroyCacheSession error string
const FailedToDestroyCacheSession = "Failed to destroy cache session"

// InvalidCachedMessageSubscriptionStrategyPassed error string
const InvalidCachedMessageSubscriptionStrategyPassed = "an invalid CachedMessageSubscriptionStrategy was passed"

// UnableToPassCacheResponseToApplication error string
const UnableToPassCacheResponseToApplication = "Unable to pass cache response to application because: "

// NoCacheChannelAvailable error string
const NoCacheChannelAvailable = "The API failed to retrieve the configured channel that was intended for the application"

// UnableToRunApplicationCacheCallback error string
const UnableToRunApplicationCacheCallback = "Unable to run the cache response callback given by the application because: "

// NoCacheCallbackAvailable error string
const NoCacheCallbackAvailable = "The application did not provide a callback that could be used to process the cache response."

// UnableToProcessCacheResponse error string
const UnableToProcessCacheResponse = "Unable to process cache response because: "

// InvalidCacheSession error string
const InvalidCacheSession = "The cache session associated with the given cache request/response was invalid"

// FailedToRetrieveCacheResponseProcessor error string
const FailedToRetrieveCacheResponseProcessor = "Tried to retrieve CacheResponseProcessor from cacheSessionMap, but none existed for the given cacheSessionP:"

// ApplicationTriedToCreateCacheRequest error string
const ApplicationTriedToCreateCacheRequest = "The application API to create a new cache request using cache session pointer"

// AnotherCacheSessionAlreadyExists error string
const AnotherCacheSessionAlreadyExists = "but another cache request's cache session under that pointer already exists."

// StartedCachePolling error string
const StartedCachePolling = "Started go routine for polling cache response channel."

// DidntStartCachePolling error string
const DidntStartCachePolling = "Didn't start go routine for polling cache response channel again because it is already running."

// InitializedReceiverCacheSessionMap error string
const InitializedReceiverCacheSessionMap = "Initialized receiver cacheSessionMap"
