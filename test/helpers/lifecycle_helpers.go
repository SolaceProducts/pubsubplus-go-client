// pubsubplus-go-client
//
// Copyright 2021-2025 Solace Corporation. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package helpers

import (
	"fmt"

	//lint:ignore ST1001 dot import is fine for tests
	. "github.com/onsi/gomega"
)

// LifecycleControl interface
type LifecycleControl interface {
	IsRunning() bool
	IsTerminating() bool
	IsTerminated() bool
}

// LifecycleControlWithReadyness interface
type LifecycleControlWithReadyness interface {
	LifecycleControl
	IsReady() bool
}

// ValidateState function
func ValidateState(lc LifecycleControl, isRunning, isTerminating, isTerminated bool) {
	ExpectWithOffset(1, lc.IsRunning()).To(Equal(isRunning), fmt.Sprintf("Expected IsRunning to return %t", isRunning))
	ExpectWithOffset(1, lc.IsTerminating()).To(Equal(isTerminating), fmt.Sprintf("Expected IsTerminating to return %t", isTerminating))
	ExpectWithOffset(1, lc.IsTerminated()).To(Equal(isTerminated), fmt.Sprintf("Expected IsTerminated to return %t", isTerminated))
}

// ValidateReadyState function
func ValidateReadyState(lc LifecycleControlWithReadyness, isReady, isRunning, isTerminating, isTerminated bool) {
	ExpectWithOffset(1, lc.IsReady()).To(Equal(isReady), fmt.Sprintf("Expected IsReady to return %t", isReady))
	ExpectWithOffset(1, lc.IsRunning()).To(Equal(isRunning), fmt.Sprintf("Expected IsRunning to return %t", isRunning))
	ExpectWithOffset(1, lc.IsTerminating()).To(Equal(isTerminating), fmt.Sprintf("Expected IsTerminating to return %t", isTerminating))
	ExpectWithOffset(1, lc.IsTerminated()).To(Equal(isTerminated), fmt.Sprintf("Expected IsTerminated to return %t", isTerminated))
}
