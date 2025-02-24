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

package future

import (
	"fmt"
	"testing"
	"time"
)

func TestFutureError(t *testing.T) {
	future := NewFutureError()
	c1 := make(chan error)
	c2 := make(chan error)
	go func() {
		err := future.Get()
		c1 <- err
	}()
	go func() {
		err := future.Get()
		c2 <- err
	}()
	time.After(100 * time.Millisecond)
	// make sure that none of the futures completed
	select {
	case <-c1:
		t.Error("expected c1 to not receive a signal until after future was completed")
	default:
		// success
	}
	select {
	case <-c2:
		t.Error("expected c1 to not receive a signal until after future was completed")
	default:
		// success
	}
	myError := fmt.Errorf("Hello World")
	future.Complete(myError)
	select {
	case r1 := <-c1:
		if r1 != myError {
			t.Errorf("expected to receive the same error from future, but it was different: %s != %s", r1, myError)
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("timed out waiting for channel c1 to receive value from completed future")
	}
	select {
	case r2 := <-c2:
		if r2 != myError {
			t.Errorf("expected to receive the same error from future, but it was different: %s != %s", r2, myError)
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("timed out waiting for channel c1 to receive value from completed future")
	}
	// success!
}
