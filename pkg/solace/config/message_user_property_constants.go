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
// An interface for constant property values to define user message properties that have a special
// reserved meaning or behaviour.

package config

// MessageUserPropertyConstant is a property that can be set on a messages.
type MessageUserPropertyConstant = MessageProperty

const (
	// A standard property key that clients should use if they want to group messages. It is used to
	// specify a partition queue name, when supported by a PubSub+ messaging broker. Expected value
	// is UTF-8 encoded up to 255 bytes long string. This constant can be passed as the property
	// string to any generic property setter on the OutboundMessageBuilder that takes properties from
	// message_properties.go as a parameter, such as
	// OutboundMessage.WithProperty().
	QueuePartitionKey = "JMSXGroupID"
)
