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

package config

// EndpointProperty - when provisioning queues on the broker, these properties of the queue can be supplied.
type EndpointProperty string

// EndpointPropertyMap is a map of EndpointProperty keys to values of respective types.
// Best handled via the type-safe EndpointProvisioner builder methods.
type EndpointPropertyMap map[EndpointProperty]interface{}

// EndpointPropertiesConfigurationProvider describes the behavior of a configuration provider
// that provides the queue properties for queue provisioning.
type EndpointPropertiesConfigurationProvider interface {
	GetConfiguration() EndpointPropertyMap
}

// GetConfiguration returns a copy of the EndpointPropertyMap
func (endpointPropertyMap EndpointPropertyMap) GetConfiguration() EndpointPropertyMap {
	ret := make(EndpointPropertyMap)
	for key, value := range endpointPropertyMap {
		ret[key] = value
	}
	return ret
}

// MarshalJSON implements the json.Marshaler interface.
func (endpointPropertyMap EndpointPropertyMap) MarshalJSON() ([]byte, error) {
	m := make(map[string]interface{})
	for k, v := range endpointPropertyMap {
		m[string(k)] = v
	}
	return nestJSON(m)
}

// UnmarshalJSON implements the json.Unmarshaler interface.
func (endpointPropertyMap EndpointPropertyMap) UnmarshalJSON(b []byte) error {
	m, err := flattenJSON(b)
	if err != nil {
		return err
	}
	for key, val := range m {
		endpointPropertyMap[EndpointProperty(key)] = val
	}
	return nil
}

// These constants are used as keys in a EndpointPropertyMap to provision queues on the broker.
const (
	// EndpointPropertyDurable boolean property specifying durability of the queue being provisioned.
	// True means durable, false means non-durable.
	EndpointPropertyDurable EndpointProperty = "solace.messaging.endpoint-property.durable"

	// EndpointPropertyExclusive boolean property specifying the access type of the queue being provisioned.
	// True means exclusive, false means non-exclusive.
	EndpointPropertyExclusive EndpointProperty = "solace.messaging.endpoint-property.exclusive"

	// EndpointPropertyNotifySender boolean property specifying whether the queue will notify senders on discards.
	// True means to notify senders, false means no notification.
	EndpointPropertyNotifySender EndpointProperty = "solace.messaging.endpoint-property.notify-sender"

	// EndpointPropertyMaxMessageRedelivery integer property specifying how many times the broker
	// will try to re-deliver messages from the queue being provisioned.
	EndpointPropertyMaxMessageRedelivery EndpointProperty = "solace.messaging.endpoint-property.max-message-redelivery"

	// EndpointPropertyMaxMessageSize integer property specifying how big (in bytes) each message
	// can be in the queue being provisioned.
	EndpointPropertyMaxMessageSize EndpointProperty = "solace.messaging.endpoint-property.max-message-size"

	// EndpointPropertyPermission EndpointPermission enum type property specifying the permission level granted to others
	// (relative to the clientusername that established the messagingService session) on the queue being provisioned.
	EndpointPropertyPermission EndpointProperty = "solace.messaging.endpoint-property.permission"

	// EndpointPropertyQuotaMB integer property specifying (in MegaBytes) how much storage
	// the queue being provisioned is allowed to take up on the broker.
	EndpointPropertyQuotaMB EndpointProperty = "solace.messaging.endpoint-property.quota-mb"

	// EndpointPropertyRespectsTTL boolean property specifying how the queue being provisioned
	// treats the TTL value in messages.
	// True means respect the TTL, false means ignore the TTL.
	EndpointPropertyRespectsTTL EndpointProperty = "solace.messaging.endpoint-property.respects-ttl"
)

// EndpointPermission - these permissions can be supplied when provisioning queues on the broker.
type EndpointPermission string

// The different permission levels that can be granted to other clients over the queue being provisioned.
// These are increasingly broad levels, each encompassing all lower permissions.
const (
	// EndpointPermissionNone specifies no access at all to others.
	EndpointPermissionNone EndpointPermission = "solace.messaging.endpoint-permission.none"

	// EndpointPermissionReadOnly specifies others may read (browse) the queue.
	EndpointPermissionReadOnly EndpointPermission = "solace.messaging.endpoint-permission.read-only"

	// EndpointPermissionConsume specifies others may browse and consume from the queue.
	EndpointPermissionConsume EndpointPermission = "solace.messaging.endpoint-permission.consume"

	// EndpointPermissionModifyTopic specifies others may browse and consume from the queue, and alter its subscription(s).
	EndpointPermissionModifyTopic EndpointPermission = "solace.messaging.endpoint-permission.modify-topic"

	// EndpointPermissionDelete specifies others may do anything to the queue without restriction.
	EndpointPermissionDelete EndpointPermission = "solace.messaging.endpoint-permission.delete"
)
