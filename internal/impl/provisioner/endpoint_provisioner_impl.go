// pubsubplus-go-client
//
// Copyright 2021-2025 Solace Corporation. All rights reserved.
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

// Package provisioner is defined below
package provisioner

import (
	"fmt"

	"solace.dev/go/messaging/internal/ccsmp"

	"solace.dev/go/messaging/internal/impl/constants"
	"solace.dev/go/messaging/internal/impl/core"
	"solace.dev/go/messaging/internal/impl/logging"
	"solace.dev/go/messaging/internal/impl/validation"

	"solace.dev/go/messaging/pkg/solace"
	"solace.dev/go/messaging/pkg/solace/config"
)

// endpointProvisionerImpl structure
type endpointProvisionerImpl struct {
	properties                  config.EndpointPropertyMap
	logger                      logging.LogLevelLogger
	internalEndpointProvisioner core.EndpointProvisioner
}

// NewEndpointProvisionerImpl function
func NewEndpointProvisionerImpl(internalEndpointProvisioner core.EndpointProvisioner) solace.EndpointProvisioner {
	return &endpointProvisionerImpl{
		// default properties
		properties:                  constants.DefaultEndpointProperties.GetConfiguration(),
		logger:                      logging.For(endpointProvisionerImpl{}),
		internalEndpointProvisioner: internalEndpointProvisioner,
	}
}

func mapEndpointPermissionToCcsmpProp(propValue string) string {
	// the ccsmp permissions map
	ccsmpPermissionProperties := map[string]string{
		string(config.EndpointPermissionNone):        string(ccsmp.SolClientEndpointPermissionNone),
		string(config.EndpointPermissionReadOnly):    string(ccsmp.SolClientEndpointPermissionReadOnly),
		string(config.EndpointPermissionConsume):     string(ccsmp.SolClientEndpointPermissionConsume),
		string(config.EndpointPermissionModifyTopic): string(ccsmp.SolClientEndpointPermissionModifyTopic),
		string(config.EndpointPermissionDelete):      string(ccsmp.SolClientEndpointPermissionDelete),
	}

	return ccsmpPermissionProperties[propValue]
}

// validateEndpointProperties function
func validateEndpointProperties(properties config.EndpointPropertyMap) ([]string, error) {
	propertiesList := []string{}
	// We can only provision Queue Endpoints for now; should support Topic Endpoints in the future
	propertiesList = append(propertiesList, ccsmp.SolClientEndpointPropID, ccsmp.SolClientEndpointPropQueue)

	for property, value := range properties {
		switch property {
		case config.EndpointPropertyDurable:
			isDurable, present, err := validation.BooleanPropertyValidation(string(config.EndpointPropertyDurable), value)
			if present {
				if err != nil {
					// return an error if the durability is not a bool
					return nil, err
				}
				// if durability is true
				if isDurable {
					propertiesList = append(propertiesList, ccsmp.SolClientEndpointPropDurable, ccsmp.SolClientPropEnableVal)
				} else {
					propertiesList = append(propertiesList, ccsmp.SolClientEndpointPropDurable, ccsmp.SolClientPropDisableVal)
				}
			}
		case config.EndpointPropertyExclusive:
			isExclusive, present, err := validation.BooleanPropertyValidation(string(config.EndpointPropertyExclusive), value)
			if present {
				if err != nil {
					return nil, err
				}
				// set access type for the endpoint (queues only)
				propertiesList = append(propertiesList, ccsmp.SolClientEndpointPropAccesstype)
				if isExclusive {
					propertiesList = append(propertiesList, ccsmp.SolClientEndpointPropAccesstypeExclusive)
				} else {
					propertiesList = append(propertiesList, ccsmp.SolClientEndpointPropAccesstypeNonexclusive)
				}
			}
		case config.EndpointPropertyNotifySender:
			propValue, present, err := validation.BooleanPropertyValidation(string(config.EndpointPropertyNotifySender), value)
			if present {
				if err != nil {
					return nil, err
				}
				// set the discard behaviour - notify Sender ON or OFF
				propertiesList = append(propertiesList, ccsmp.SolClientEndpointPropDiscardBehavior)
				if propValue {
					propertiesList = append(propertiesList, ccsmp.SolClientEndpointPropDiscardNotifySenderOn)
				} else {
					propertiesList = append(propertiesList, ccsmp.SolClientEndpointPropDiscardNotifySenderOff)
				}
			}
		case config.EndpointPropertyMaxMessageRedelivery:
			propValue, present, err := validation.IntegerPropertyValidationWithRange(string(config.EndpointPropertyMaxMessageRedelivery), value, 0, 255)
			if present {
				if err != nil {
					return nil, err
				}
				propertiesList = append(propertiesList, ccsmp.SolClientEndpointPropMaxmsgRedelivery, fmt.Sprint(propValue))
			}
		case config.EndpointPropertyMaxMessageSize:
			propValue, present, err := validation.Uint64PropertyValidation(string(config.EndpointPropertyMaxMessageSize), value)
			if present {
				if err != nil {
					return nil, err
				}
				propertiesList = append(propertiesList, ccsmp.SolClientEndpointPropMaxmsgSize, fmt.Sprint(propValue))
			}
		case config.EndpointPropertyPermission:
			propValue, present, err := validation.StringPropertyValidation(
				string(config.EndpointPropertyPermission),
				fmt.Sprintf("%v", value),
				string(config.EndpointPermissionNone),
				string(config.EndpointPermissionReadOnly),
				string(config.EndpointPermissionConsume),
				string(config.EndpointPermissionModifyTopic),
				string(config.EndpointPermissionDelete),
			)
			if present {
				if err != nil {
					return nil, err
				}
				propertiesList = append(propertiesList, ccsmp.SolClientEndpointPropPermission, mapEndpointPermissionToCcsmpProp(propValue))
			}
		case config.EndpointPropertyQuotaMB:
			propValue, present, err := validation.Uint64PropertyValidation(string(config.EndpointPropertyQuotaMB), value)
			if present {
				if err != nil {
					return nil, err
				}
				propertiesList = append(propertiesList, ccsmp.SolClientEndpointPropQuotaMb, fmt.Sprint(propValue))
			}
		case config.EndpointPropertyRespectsTTL:
			shouldRespectTTL, present, err := validation.BooleanPropertyValidation(string(config.EndpointPropertyRespectsTTL), value)
			if present {
				if err != nil {
					return nil, err
				}
				// if should respect Ttl
				if shouldRespectTTL {
					propertiesList = append(propertiesList, ccsmp.SolClientEndpointPropRespectsMsgTTL, ccsmp.SolClientPropEnableVal)
				}
			}
		}
	}

	return propertiesList, nil
}

// Provision a queue with the specified name on the broker bearing
// all the properties configured on the Provisioner.
// Properties left unconfigured will be set to broker defaults.
// Accepts a boolean parameter to ignore a specific error response from the broker which indicates
// that a queue with the same name and properties already exists.
// Blocks until the operation is finished on the broker, returns the provision outcome.
func (provisioner *endpointProvisionerImpl) Provision(queueName string, ignoreExists bool) solace.ProvisionOutcome {
	// Implementation here
	provisionOutcome := provisionOutcome{
		ok:                 false,
		err:                nil,
		endpointProperties: nil,
	}
	// check that provisioner is running
	if !provisioner.internalEndpointProvisioner.IsRunning() {
		// we error if the provisioner is not running
		provisionOutcome.err = solace.NewError(&solace.IllegalStateError{}, constants.UnableToProvisionParentServiceNotStarted, nil)
		return &provisionOutcome
	}

	properties := provisioner.properties.GetConfiguration()

	endpointProperties, err := validateEndpointProperties(properties)
	if err != nil {
		// return provision outcome with error
		provisionOutcome.err = err
		return &provisionOutcome
	}

	// We want to provision a queue
	endpointProperties = append(endpointProperties, ccsmp.SolClientEndpointPropName, queueName)

	// continue to provision the queue here
	correlationID, result, errInfo := provisioner.internalEndpointProvisioner.Provision(endpointProperties, ignoreExists)
	if errInfo != nil {
		provisionErr := core.ToNativeError(errInfo, constants.FailedToProvisionEndpoint)
		provisionOutcome.err = provisionErr
		return &provisionOutcome // return the error
	}

	if provisioner.logger.IsDebugEnabled() {
		provisioner.logger.
			Debug(fmt.Sprintf("Provision awaiting confirm on provision outcome for queue: '%s' with CorrelationID: %v", queueName, correlationID))
	}
	// result channel should not be nil
	if result == nil {
		provisionOutcome.ok = false
		provisionOutcome.err = solace.NewError(&solace.IllegalStateError{}, fmt.Sprintf("%sinvalid provision outcome channel for queue '%s' with CorrelationID: %v", constants.FailedToProvisionEndpoint, queueName, correlationID), nil)
		return &provisionOutcome
	}

	// block until we get provision outcome
	// should timeout from ccsmp if waiting for timeout exceeds
	event := <-result
	if provisioner.logger.IsDebugEnabled() {
		if event.GetError() != nil {
			provisioner.logger.Debug(fmt.Sprintf("Provision received error for queue: '%s' with CorrelationID: %v. Error: %s", queueName, correlationID, event.GetError().Error()))
		} else {
			provisioner.logger.Debug(fmt.Sprintf("Provision received confirm for queue: '%s' with CorrelationID: %v", queueName, correlationID))
		}
	}

	// an error occurred, return it here
	if event.GetError() != nil {
		provisionOutcome.ok = false
		provisionOutcome.err = event.GetError()
		return &provisionOutcome
	}

	// no errors here, so set status (ok) to True
	provisionOutcome.ok = true
	return &provisionOutcome
}

// ProvisionAsync will asynchronously provision a queue with the specified name on
// the broker bearing all the properties configured on the Provisioner.
// Accepts a boolean parameter to ignore a specific error response from the broker which indicates
// that a queue with the same name and properties already exists.
// Properties left unconfigured will be set to broker defaults.
// This function is idempotent. The only way to resume configuration operation
// after this function is called is to create a new instance.
// Any attempt to call this function will provision the queue
// on the broker, even if this function completes.
// The maximum number of outstanding requests for provision is set to 32.
// This function will return an error when this limit is reached or exceeded.
// Returns a channel immediately that receives the endpoint provision outcome when completed.
func (provisioner *endpointProvisionerImpl) ProvisionAsync(queueName string, ignoreExists bool) <-chan solace.ProvisionOutcome {
	// Implementation here
	result := make(chan solace.ProvisionOutcome, 1)
	go func() {
		result <- provisioner.Provision(queueName, ignoreExists)
		close(result)
	}()
	return result
}

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
// Please note that the callback may not be executed in network order from the broker
func (provisioner *endpointProvisionerImpl) ProvisionAsyncWithCallback(queueName string, ignoreExists bool, callback func(solace.ProvisionOutcome)) {
	// Implementation here
	go func() {
		callback(provisioner.Provision(queueName, ignoreExists))
	}()
}

// Deprovision (deletes) the queue with the given name from the broker.
// Ignores all queue properties accumulated in the EndpointProvisioner.
// Accepts the ignoreMissing boolean property, which, if set to true,
// turns the "no such queue" error into nil.
// Blocks until the operation is finished on the broker, returns the nil or an error
func (provisioner *endpointProvisionerImpl) Deprovision(queueName string, ignoreMissing bool) error {
	// Implementation here
	if !provisioner.internalEndpointProvisioner.IsRunning() {
		// we error if the provisioner is not running
		return solace.NewError(&solace.IllegalStateError{}, constants.UnableToDeprovisionParentServiceNotStarted, nil)
	}

	properties := provisioner.properties.GetConfiguration() // we don't need all these properties for deprov

	// Override defaults durability to be True since we currently only support durable
	// properties[config.EndpointPropertyDurable] = true

	endpointProperties, err := validateEndpointProperties(properties)
	if err != nil {
		// return deprovision error
		return err
	}

	// We want to provision a queue
	endpointProperties = append(endpointProperties, ccsmp.SolClientEndpointPropName, queueName)

	// continue to provision the queue here
	correlationID, result, errInfo := provisioner.internalEndpointProvisioner.Deprovision(endpointProperties, ignoreMissing)
	if errInfo != nil {
		return core.ToNativeError(errInfo, constants.FailedToDeprovisionEndpoint)
	}
	if provisioner.logger.IsDebugEnabled() {
		provisioner.logger.Debug(fmt.Sprintf("Deprovision awaiting confirm on deprovision outcome for queue: '%s' with CorrelationID: %v", queueName, correlationID))
	}

	// result channel should not be nil
	if result == nil {
		return solace.NewError(&solace.IllegalStateError{}, fmt.Sprintf("%sinvalid deprovision result channel for queue '%s' with CorrelationID: %v", constants.FailedToDeprovisionEndpoint, queueName, correlationID), nil)
	}

	// block until we get provision outcome
	// should timeout from ccsmp if waiting for timeout exceeds
	event := <-result
	if provisioner.logger.IsDebugEnabled() {
		if event.GetError() != nil {
			provisioner.logger.Debug(fmt.Sprintf("Deprovision received error for queue: '%s' with CorrelationID: %v. Error: %s", queueName, correlationID, event.GetError().Error()))
		} else {
			provisioner.logger.Debug(fmt.Sprintf("Deprovision received confirm for queue: '%s' with CorrelationID: %v", queueName, correlationID))
		}
	}

	// an error occurred while deprovisioning queue
	if event.GetError() != nil {
		return event.GetError()
	}

	return nil
}

// DeprovisionAsync will asynchronously deprovision (deletes) the queue with the given
// name from the broker. Returns immediately.
// Ignores all queue properties accumulated in the EndpointProvisioner.
// Accepts the ignoreMissing boolean property, which, if set to true,
// turns the "no such queue" error into nil.
// Any error (or nil if successful) is reported through the returned channel.
// Returns a channel immediately that receives nil or an error.
func (provisioner *endpointProvisionerImpl) DeprovisionAsync(queueName string, ignoreMissing bool) <-chan error {
	// Implementation here
	result := make(chan error, 1)
	go func() {
		result <- provisioner.Deprovision(queueName, ignoreMissing)
		close(result)
	}()
	return result
}

// DeprovisionAsyncWithCallback will asynchronously deprovision (deletes) the queue with the
// given name on the broker.
// Ignores all queue properties accumulated in the EndpointProvisioner.
// Accepts the ignoreMissing boolean property, which, if set to true,
// turns the "no such queue" error into nil.
// Returns immediately and registers a callback that will receive an
// error if deprovision on the broker fails.
// Please note that the callback may not be executed in network order from the broker
func (provisioner *endpointProvisionerImpl) DeprovisionAsyncWithCallback(queueName string, ignoreMissing bool, callback func(err error)) {
	// Implementation here
	go func() {
		callback(provisioner.Deprovision(queueName, ignoreMissing))
	}()
}

// FromConfigurationProvider will set the given properties to the resulting message.
func (provisioner *endpointProvisionerImpl) FromConfigurationProvider(properties config.EndpointPropertiesConfigurationProvider) solace.EndpointProvisioner {
	mergeEndpointPropertyMap(provisioner.properties, properties)
	return provisioner
}

// GetConfiguration will returns a copy of the current configuration held.
func (provisioner *endpointProvisionerImpl) GetConfiguration() config.EndpointPropertyMap {
	return provisioner.properties // TODO: make a copy of this before returning
}

// WithProperty sets an individual property on a message.
func (provisioner *endpointProvisionerImpl) WithProperty(propertyName config.EndpointProperty, propertyValue interface{}) solace.EndpointProvisioner {
	provisioner.properties[config.EndpointProperty(propertyName)] = propertyValue
	return provisioner
}

// WithDurability will set the durability property for the endpoint.
func (provisioner *endpointProvisionerImpl) WithDurability(durable bool) solace.EndpointProvisioner {
	provisioner.properties[config.EndpointPropertyDurable] = durable
	return provisioner
}

// WithExclusiveAccess will set the endpoint access type.
func (provisioner *endpointProvisionerImpl) WithExclusiveAccess(exclusive bool) solace.EndpointProvisioner {
	provisioner.properties[config.EndpointPropertyExclusive] = exclusive
	return provisioner
}

// WithDiscardNotification will set the notification behaviour on message discards.
func (provisioner *endpointProvisionerImpl) WithDiscardNotification(notifySender bool) solace.EndpointProvisioner {
	provisioner.properties[config.EndpointPropertyNotifySender] = notifySender
	return provisioner
}

// WithMaxMessageRedelivery will sets the number of times messages from the
// queue will be redelivered before being diverted to the DMQ.
func (provisioner *endpointProvisionerImpl) WithMaxMessageRedelivery(count uint) solace.EndpointProvisioner {
	provisioner.properties[config.EndpointPropertyMaxMessageRedelivery] = count
	return provisioner
}

// WithMaxMessageSize will set the maximum message size in bytes the queue will accept.
func (provisioner *endpointProvisionerImpl) WithMaxMessageSize(count uint) solace.EndpointProvisioner {
	provisioner.properties[config.EndpointPropertyMaxMessageSize] = count
	return provisioner
}

// WithPermission will set the queue's permission level for others.
// The levels are supersets of each other, can not be combined and the last one set will take effect.
func (provisioner *endpointProvisionerImpl) WithPermission(permission config.EndpointPermission) solace.EndpointProvisioner {
	provisioner.properties[config.EndpointPropertyPermission] = permission
	return provisioner
}

// WithQuotaMB will set the overall size limit of the queue in MegaBytes.
func (provisioner *endpointProvisionerImpl) WithQuotaMB(quota uint) solace.EndpointProvisioner {
	provisioner.properties[config.EndpointPropertyQuotaMB] = quota
	return provisioner
}

// WithTTLPolicy will set how the queue will handle the TTL value in messages.
// True to respect it, false to ignore it.
func (provisioner *endpointProvisionerImpl) WithTTLPolicy(respect bool) solace.EndpointProvisioner {
	provisioner.properties[config.EndpointPropertyRespectsTTL] = respect
	return provisioner
}

func (provisioner *endpointProvisionerImpl) String() string {
	return fmt.Sprintf("solace.EndpointProvisioner at %p", provisioner)
}

func mergeEndpointPropertyMap(original config.EndpointPropertyMap, new config.EndpointPropertiesConfigurationProvider) {
	if new == nil {
		return
	}
	props := new.GetConfiguration()
	for key, value := range props {
		original[key] = value
	}
}

// ProvisionOutcome
type provisionOutcome struct {
	// The low level error object if any.
	err error
	// Actual outcome: true means success, false means failure.
	ok bool
	// Initially empty, but when we support returning the on-broker queue properties, this is where they will go.
	endpointProperties config.EndpointPropertyMap
}

func (outcome *provisionOutcome) GetError() error {
	return outcome.err
}

func (outcome *provisionOutcome) GetStatus() bool {
	return outcome.ok
}
