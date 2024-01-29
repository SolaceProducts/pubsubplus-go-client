package solace

import (
	"time"

	"solace.dev/go/messaging/pkg/solace/config"
	"solace.dev/go/messaging/pkg/solace/message"
	"solace.dev/go/messaging/pkg/solace/resource"
)

// RequestReplyMessagePublisher allows for publishing of request-reply messages
// with handling for reply messages.
type RequestReplyMessagePublisher interface {
	MessagePublisher
	MessagePublisherHealthCheck

	// StartAsyncCallback will start the RequestReplyMessagePublisher asynchronously.
	// Before this function is called, the service is considered
	// off-duty. To operate normally, this function must be called on
	// the RequestReplyMessageReceiver instance. This function is idempotent.
	// Returns immediately and will call the callback function when ready
	// passing the started RequestReplyMessageReceiver instance, or nil and
	// an error if one occurred. Subsequent calls will register additional
	// callbacks that will be called immediately if already started.
	StartAsyncCallback(callback func(RequestReplyMessagePublisher, error))

	// TerminateAsyncCallback will terminate the message publisher asynchronously.
	// This function is idempotent. The only way to resume operation
	// after this function is called is to create a new instance.
	// Any attempt to call this function renders the instance
	// permanently terminated, even if this function completes.
	// A graceful shutdown will be attempted within the grace period.
	// A grace period of 0 implies a non-graceful shutdown that ignores
	// unfinished tasks or in-flight messages.
	// Returns immediately and registers a callback that will receive an
	// error if one occurred or nil if successfully and gracefully terminated.
	// If gracePeriod is less than 0, the function will wait indefinitely.
	TerminateAsyncCallback(gracePeriod time.Duration, callback func(error))

	// PublishBytes sends a request for a reply of type byte array to the specified destination.
	// The API will handle correlation of messages so no additional work is requried.
	// Takes a requestMessage to send, a replyMessageHandler function to handle the
	// response, a requestsDestination to deliver the requestMessage to, a replyTimeout
	// indicating the maximum wait time for a response message and an optional
	// userContext object given to the replyMessageHandler (may be nil).
	// Returns an error if one occurred. If replyTimeout is less than 0, the function
	// will wait indefinitely. Possible errors include:
	// - solace/errors.*PubSubPlusClientError - If the message could not be sent and all retry attempts failed.
	// - solace/errors.*PublisherOverflowError - If messages are published faster than publisher's I/O
	//   capabilities allow. When publishing can be resumed, the registered PublisherReadinessListeners
	//   are called.
	PublishBytes(message []byte, replyMessageHandler ReplyMessageHandler, destination *resource.Topic, replyTimeout time.Duration, userContext interface{}) error

	// PublishString sends a request for a reply of type string to the specified destination.
	// The API will handle correlation of messages so no additional work is requried.
	// Takes a requestMessage to send, a replyMessageHandler function to handle the
	// response, a requestsDestination to deliver the requestMessage to, a replyTimeout
	// indicating the maximum wait time for a response message and an optional
	// userContext object given to the replyMessageHandler (may be nil).
	// Returns an error if one occurred. If replyTimeout is less than 0, the function
	// will wait indefinitely. Possible errors include:
	// - solace/errors.*PubSubPlusClientError - If the message could not be sent and all retry attempts failed.
	// - solace/errors.*PublisherOverflowError - If messages are published faster than publisher's I/O
	//   capabilities allow. When publishing can be resumed, the registered PublisherReadinessListeners
	//   are called.
	PublishString(message string, replyMessageHandler ReplyMessageHandler, destination *resource.Topic, replyTimeout time.Duration, userContext interface{}) error

	// Publish sends a request for a reply non-blocking with optional user context.
	// The API will handle correlation of messages so no additional work is requried.
	// Takes a requestMessage to send, a replyMessageHandler function to handle the
	// response, a requestsDestination to deliver the requestMessage to, a replyTimeout
	// indicating the maximum wait time for a response message and an optional
	// userContext object given to the replyMessageHandler (may be nil).
	// Returns an error if one occurred. If replyTimeout is less than 0, the function
	// will wait indefinitely. Possible errors include:
	// - solace/errors.*PubSubPlusClientError if the message could not be sent and all retry attempts failed.
	// - solace/errors.*PublisherOverflowError if publishing messages faster than publisher's I/O
	// capabilities allow. When publishing can be resumed, registered PublisherReadinessListeners
	// will be called.
	Publish(requestMessage message.OutboundMessage, replyMessageHandler ReplyMessageHandler,
		requestsDestination *resource.Topic, replyTimeout time.Duration,
		properties config.MessagePropertiesConfigurationProvider, userContext interface{}) error

	// PublishAwaitResponse will send a request for a reply blocking until a response is
	// received. The API will handle correlation of messages so no additional work is required.
	// Takes a requestMessage to send, a requestDestination to deliver the requestMessage to,
	// and a replyTimeout indicating the maximum wait time for a response message.
	// Will return the response and an error if one occurred. If replyTimeout is less than 0,
	// the function will wait indefinitely. Possible errors include:
	// - solace/errors.*PubSubPlusClientError if the message could not be sent and all retry attempts failed.
	// - solace/errors.*PublisherOverflowError if publishing messages faster than publisher's I/O
	// capabilities allow. When publishing can be resumed, registered PublisherReadinessListeners
	// will be called.
	PublishAwaitResponse(requestMessage message.OutboundMessage, requestDestination *resource.Topic,
		replyTimeout time.Duration, properties config.MessagePropertiesConfigurationProvider) (message.InboundMessage, error)
}

// ReplyMessageHandler is a callback to handle a reply message. The function will be
// called with a message received or nil, the user context if it was set when calling
// RequestReplyMessagePublisher.Publish, and an error if one was thrown.
type ReplyMessageHandler func(message message.InboundMessage, userContext interface{}, err error)

// RequestReplyMessagePublisherBuilder allows for configuration of request reply message publisher instances
type RequestReplyMessagePublisherBuilder interface {
	// Build will build a new RequestReplyMessagePublisher instance based on the configured properties.
	// Returns solace/errors.*InvalidConfigurationError if an invalid configuration is provided.
	Build() (messagePublisher RequestReplyMessagePublisher, err error)
	// OnBackPressureReject will set the publisher backpressure strategy to reject
	// where publish attempts will be rejected once the bufferSize, in number of messages, is reached.
	// If bufferSize is 0, an error will be thrown when the transport is full when publishing.
	// Valid bufferSize is >= 0.
	OnBackPressureReject(bufferSize uint) RequestReplyMessagePublisherBuilder
	// OnBackPressureWait will set the publisher backpressure strategy to wait where publish
	// attempts will block until there is space in the buffer of size bufferSize in number of messages.
	// Valid bufferSize is >= 1.
	OnBackPressureWait(bufferSize uint, waitTime time.Duration) RequestReplyMessagePublisherBuilder
	// FromConfigurationProvider will configure the persistent publisher with the given properties.
	// Built in PublisherPropertiesConfigurationProvider implementations include:
	//   PublisherPropertyMap, a map of PublisherProperty keys to values
	FromConfigurationProvider(provider config.PublisherPropertiesConfigurationProvider) RequestReplyMessagePublisherBuilder
}
