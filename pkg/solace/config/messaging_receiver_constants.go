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

// MessageSettlementOutcome - represents the type for supported message settlement outcome on a PersistentMessageReceiver.
type MessageSettlementOutcome string

// The various message settlement outcomes available for use when configuring a PersistentMessageReceiver.
const (
	// Settles the message with a positive acknowledgement, removing it from the queue.
	// Same as calling Ack() on the message.
	PersistentReceiverAcceptedOutcome MessageSettlementOutcome = "ACCEPTED"

	// Settles the message with a negative acknowledgement without removing it from the queue.
	// This may or may not make the message eligible for redelivery or eventually the DMQ, depending on the queue configuration.
	PersistentReceiverFailedOutcome MessageSettlementOutcome = "FAILED"

	// Settles the message with a negative acknowledgement, removing it from the queue.
	PersistentReceiverRejectedOutcome MessageSettlementOutcome = "REJECTED"
)
