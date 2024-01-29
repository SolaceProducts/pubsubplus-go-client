package solace

import (
	"time"

	"solace.dev/go/messaging/pkg/solace/config"
	"solace.dev/go/messaging/pkg/solace/message"
	"solace.dev/go/messaging/pkg/solace/resource"
)

// Replier allows for received request-reply messages to be replied to. The
// destination of these messages is automatically determined by the
// InboundMessage passed to a RequestMessageHandler.
type Replier interface {
	// Reply publishes a reply or response message.
	Reply(message message.OutboundMessage) error
}

// RequestMessageHandler is a callback called when a message is received.
// It is passed the request message as well as a replier allowing for the
// publishing of a reply message. The replier argument may be nil indicating
// that a NON-Request-Reply message has been received on the topic subscription
// given when building the RequestReplyMessageReceiver instance.
type RequestMessageHandler func(message message.InboundMessage, replier Replier)

// RequestReplyMessageReceiver allows receiving of request-reply messages
// with handling for sending reply messages.
type RequestReplyMessageReceiver interface {
	MessageReceiver

	// StartAsyncCallback will start the message receiver asynchronously.
	// Before this function is called, the service is considered
	// off-duty. To operate normally, this function must be called on
	// the RequestReplyMessageReceiver instance. This function is idempotent.
	// Returns immediately and will call the callback function when ready
	// passing the started RequestReplyMessageReceiver instance, or nil and
	// an error if one occurred. Subsequent calls will register additional
	// callbacks that will be called immediately if already started.
	StartAsyncCallback(callback func(RequestReplyMessageReceiver, error))

	// TerminateAsyncCallback will terminate the message receiver asynchronously.
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

	// ReceiveAsync registers an asynchronous message handler. The given
	// messageHandler will handle an ordered sequence of inbound request messages.
	// This function is mutually exclusive to ReceiveMessage.
	// Returns an error one occurred while registering the callback.
	// If a callback is already registered, it will be replaced by the given
	// callback.
	ReceiveAsync(messageHandler RequestMessageHandler) error

	// ReceiveMessage receives a message and replier synchronously from the receiver.
	// Returns a nil replier if the message can not be replied to.
	// Returns an error if the receiver is not started or already terminated.
	// This function waits until the specified timeout to receive a message or waits
	// forever if timeout value is negative. If a timeout occurs, a solace.TimeoutError
	// is returned.
	ReceiveMessage(timeout time.Duration) (message.InboundMessage, Replier, error)
}

// RequestReplyMessageReceiverBuilder allows for configuration of RequestReplyMessageReceiver instances
type RequestReplyMessageReceiverBuilder interface {
	// Build will build a new RequestReplyMessageReceiver with the given properties.
	// The message receiver will subscribe to the specified topic subscription.
	// Accepts TopicSubscription instances as Subscriptions. See solace.TopicSubscriptionOf.
	// Returns solace/errors.*InvalidConfigurationError if an invalid configuration is provided.
	Build(requestTopicSubscription resource.Subscription) (messageReceiver RequestReplyMessageReceiver, err error)
	// BuildWithSharedSubscription will build a new RequestReplyMessageReceiver with
	// the given properties using a shared topic subscription and the shared name.
	BuildWithSharedSubscription(requestTopicSubscription resource.Subscription, shareName resource.ShareName) (messageReceiver RequestReplyMessageReceiver, err error)
	// FromConfigurationProvider will configure the request reply receiver with the given properties.
	// Built in ReceiverPropertiesConfigurationProvider implementations include:
	//   ReceiverPropertyMap, a map of ReceiverProperty keys to values
	FromConfigurationProvider(provider config.PublisherPropertiesConfigurationProvider) RequestReplyMessageReceiverBuilder
}
