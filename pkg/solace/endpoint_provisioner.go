package solace

import (
	"solace.dev/go/messaging/pkg/solace/config"
)

// EndpointProvisioner aids the type-safe collection of queue properties,
// and can provision multiple queues with different names (but identical properties) on the broker.
// Warning: This is a mutable object. The fluent builder style setters modify and return the original object. Make copies explicitly.
type EndpointProvisioner interface {
	// Provision a queue with the specified name on the broker bearing
	// all the properties configured on the Provisioner.
	// Properties left unconfigured will be set to broker defaults.
	// Accepts a boolean parameter to ignore a specific error response from the broker which indicates
	// that a queue with the same name and properties already exists.
	// Blocks until the operation is finished on the broker, returns the provision outcome.
	Provision(queueName string, ignoreExists bool) ProvisionOutcome

	// ProvisionAsync will asynchronously provision a queue with the specified name on
	// the broker bearing all the properties configured on the Provisioner.
	// Accepts a boolean parameter to ignore a specific error response from the broker which indicates
	// that a queue with the same name and properties already exists.
	// Properties left unconfigured will be set to broker defaults.
	// This function is idempotent. The only way to resume configuration operation
	// after this function is called is to create a new instance.
	// Any attempt to call this function will provision the queue
	// on the broker, even if this function completes.
	// Returns a channel immediately that receives the endpoint provision outcome when completed.
	ProvisionAsync(queueName string, ignoreExists bool) <-chan ProvisionOutcome

	// ProvisionAsyncWithCallback will asynchronously provision a queue with the specified name on
	// the broker bearing all the properties configured on the Provisioner.
	// Accepts a boolean parameter to ignore a specific error response from the broker which indicates
	// that a queue with the same name and properties already exists.
	// Properties left unconfigured will be set to broker defaults.
	// This function is idempotent. The only way to resume configuration operation
	// after this function is called is to create a new instance.
	// Any attempt to call this function will provision the queue
	// on the broker, even if this function completes.
	// Returns immediately and registers a callback that will receive an
	// outcome for the endpoint provision.
	ProvisionAsyncWithCallback(queueName string, ignoreExists bool, callback func(ProvisionOutcome))

	// Deprovision (deletes) the queue with the given name from the broker.
	// Ignores all queue properties accumulated in the EndpointProvisioner.
	// Accepts the ignoreMissing boolean property, which, if set to true,
	// turns the "no such queue" error into nil.
	// Blocks until the operation is finished on the broker, returns the nil or an error
	Deprovision(queueName string, ignoreMissing bool) error

	// DeprovisionAsync will asynchronously deprovision (deletes) the queue with the given
	// name from the broker. Returns immediately.
	// Ignores all queue properties accumulated in the EndpointProvisioner.
	// Accepts the ignoreMissing boolean property, which, if set to true,
	// turns the "no such queue" error into nil.
	// Any error (or nil if successful) is reported through the returned channel.
	// Returns a channel immediately that receives nil or an error.
	DeprovisionAsync(queueName string, ignoreMissing bool) <-chan error

	// DeprovisionAsyncWithCallback will asynchronously deprovision (deletes) the queue with the
	// given name on the broker.
	// Ignores all queue properties accumulated in the EndpointProvisioner.
	// Accepts the ignoreMissing boolean property, which, if set to true,
	// turns the "no such queue" error into nil.
	// Returns immediately and registers a callback that will receive an
	// error if deprovision on the broker fails.
	DeprovisionAsyncWithCallback(queueName string, ignoreMissing bool, callback func(err error))

	// FromConfigurationProvider sets the configuration based on the specified configuration provider.
	// The following are built in configuration providers:
	// - EndpointPropertyMap - This can be used to set an EndpointProperty to a value programatically.
	//
	// The EndpointPropertiesConfigurationProvider interface can also be implemented by a type
	// to have it act as a configuration factory by implementing the following:
	//
	//   func (type MyType) GetConfiguration() EndpointPropertyMap {...}
	//
	// Any properties provided by the configuration provider are layered over top of any
	// previously set properties, including those set by specifying various strategies.
	// Can be used to clone a EndpointProvisioner object.
	FromConfigurationProvider(properties config.EndpointPropertiesConfigurationProvider) EndpointProvisioner

	// Returns a copy of the current configuration held.
	GetConfiguration() config.EndpointPropertyMap

	// WithProperty will set an individual queue property by name. Does not perform type checking.
	WithProperty(propertyName config.EndpointProperty, propertyValue interface{}) EndpointProvisioner

	// WithDurability will set the durability property for the endpoint.
	// True for durable, false for non-durable.
	WithDurability(durable bool) EndpointProvisioner

	// WithExclusiveAccess will set the endpoint access type.
	// True for exclusive, false for non-exclusive.
	WithExclusiveAccess(exclusive bool) EndpointProvisioner

	// WithDiscardNotification will set the notification behaviour on message discards.
	// True to notify senders about discards, false not to.
	WithDiscardNotification(notifySender bool) EndpointProvisioner

	// WithMaxMessageRedelivery will sets the number of times messages from the
	// queue will be redelivered before being diverted to the DMQ.
	WithMaxMessageRedelivery(count int) EndpointProvisioner

	// WithMaxMessageSize will set the maximum message size in bytes the queue will accept.
	WithMaxMessageSize(count int) EndpointProvisioner

	// WithPermission will set the queue's permission level for others.
	// The levels are supersets of each other, can not be combined and the last one set will take effect.
	WithPermission(permission config.EndpointPermission) EndpointProvisioner

	// WithQuotaMB will set the overall size limit of the queue in MegaBytes.
	WithQuotaMB(quota int) EndpointProvisioner

	// WithTTLPolicy will set how the queue will handle the TTL value in messages.
	// True to respect it, false to ignore it.
	WithTTLPolicy(respect bool) EndpointProvisioner
}

// The EndpointProvisioner.Provision operations return this structure to indicate the success, the underlying error code,
// and when available, the properties of the queue on the broker.
// It is possible for the outcome to be successful and yet contain a non-nil error when the queue already exists on the broker,
// and the Provision function was invoked with the ignoreExists flag set.
type ProvisionOutcome interface {
	// GetError returns the low level error object if any.
	GetError() error

	// GetStatus retrives the actual outcome: true means success, false means failure.
	GetStatus() bool

	// GetEndpointProperties retrieves the peoperties of the provisioned endpoint.
	// Initially empty, but once CCSMP supports returning the on-broker queue properties, this is where they will go.
	GetEndpointProperties() config.EndpointPropertyMap
}
