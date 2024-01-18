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

package message

import (
	"testing"

	"solace.dev/go/messaging/internal/ccsmp"
)

func TestOutboundMessageFree(t *testing.T) {
	msg, err := NewOutboundMessage()
	if err != nil {
		t.Error("did not expect error, got " + err.Error())
	}
	if msg.messagePointer == nil {
		t.Error("expected message pointer to not be nil")
	}
	if msg.IsDisposed() {
		t.Error("message is disposed before disposed called")
	}
	msg.Dispose()
	if !msg.IsDisposed() {
		t.Error("IsDisposed returned false, expected true")
	}
	if msg.messagePointer != nil {
		t.Error("expected MessagePointer to be freed and set to nil, it was not")
	}
}

func TestInboundMessageFree(t *testing.T) {
	msgP, ccsmpErr := ccsmp.SolClientMessageAlloc()
	if ccsmpErr != nil {
		t.Error("did not expect error, got " + ccsmpErr.GetMessageAsString())
	}
	msg := NewInboundMessage(msgP, false)
	if msg.messagePointer == nil {
		t.Error("expected message pointer to not be nil")
	}
	if msg.IsDisposed() {
		t.Error("message is disposed before disposed called")
	}
	msg.Dispose()
	if !msg.IsDisposed() {
		t.Error("IsDisposed returned false, expected true")
	}
	if msg.messagePointer != nil {
		t.Error("expected MessagePointer to be freed and set to nil, it was not")
	}
}
