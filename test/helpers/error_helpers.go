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
	"encoding/json"
	"errors"
	"fmt"

	"solace.dev/go/messaging/pkg/solace"
	"solace.dev/go/messaging/pkg/solace/subcode"
	"solace.dev/go/messaging/test/sempclient/action"
	"solace.dev/go/messaging/test/sempclient/config"
	"solace.dev/go/messaging/test/sempclient/monitor"

	//lint:ignore ST1001 dot import is fine for tests
	. "github.com/onsi/gomega"
)

// Note that in all helper functions, we must expect with offset

// ValidateError function
func ValidateError(err error, expected error, substrings ...string) {
	validateError(2, err, expected, substrings...)
}

// ValidateChannelError function
func ValidateChannelError(errChan <-chan error, expected error, substrings ...string) {
	var err error
	EventuallyWithOffset(1, errChan).Should(Receive(&err))
	validateError(2, err, expected, substrings...)
}

func validateError(offset int, err error, expected interface{}, substrings ...string) {
	ExpectWithOffset(offset, err).To(HaveOccurred(), "Expected error to have occurred")
	ExpectWithOffset(offset, err).To(BeAssignableToTypeOf(expected), fmt.Sprintf("Expected error of type %T to be assignable of type %T", err, expected))
	if len(substrings) > 0 {
		for _, str := range substrings {
			ExpectWithOffset(offset, err.Error()).To(ContainSubstring(str), fmt.Sprintf("Expected error with string '%s' to contain substring '%s'", err.Error(), str))
		}
	}
}

// ValidateNativeError function
func ValidateNativeError(err error, codes ...subcode.Code) {
	validateNativeError(2, err, codes...)
}

func validateNativeError(offset int, err error, codes ...subcode.Code) {
	ExpectWithOffset(offset, err).To(Not(BeNil()), "Expected native error to have occurred")
	var pubSubPlusError *solace.NativeError
	ExpectWithOffset(offset, errors.As(err, &pubSubPlusError)).To(BeTrue(), fmt.Sprintf("Expected error to be or wrap PubSubPlusNativeError, was %T", err))
	ExpectWithOffset(offset, subcode.Is(pubSubPlusError.SubCode(), codes...)).To(BeTrue(),
		fmt.Sprintf("Expected error code %d (%s) to be in %v", pubSubPlusError.SubCode(), pubSubPlusError.SubCode().String(), codes))
}

// DecodeActionSwaggerError function
func DecodeActionSwaggerError(err error, response interface{}) {
	decodeActionSwaggerError(err, response, 2)
}

func decodeActionSwaggerError(err error, response interface{}, offset int) {
	ExpectWithOffset(offset, err).To(HaveOccurred(), "Expected action swagger error to have occurred")
	var swaggerError action.GenericSwaggerError
	ExpectWithOffset(offset, errors.As(err, &swaggerError)).To(BeTrue(), "Expected action swagger error to be of type action.GenericSwaggerError")
	jsonErr := json.Unmarshal(swaggerError.Body(), response)
	ExpectWithOffset(offset, jsonErr).ToNot(HaveOccurred(), "Expected to be able to decode response body to response object. Data: '"+string(swaggerError.Body())+"'")
}

// DecodeConfigSwaggerError function
func DecodeConfigSwaggerError(err error, response interface{}) {
	decodeConfigSwaggerError(err, response, 2)
}

func decodeConfigSwaggerError(err error, response interface{}, offset int) {
	ExpectWithOffset(offset, err).To(HaveOccurred(), "Expected config swagger error to have occurred")
	var swaggerError config.GenericSwaggerError
	ExpectWithOffset(offset, errors.As(err, &swaggerError)).To(BeTrue(), "Expected config swagger error to be of type config.GenericSwaggerError")
	jsonErr := json.Unmarshal(swaggerError.Body(), response)
	ExpectWithOffset(offset, jsonErr).ToNot(HaveOccurred(), "Expected to be able to decode response body to response object. Data: '"+string(swaggerError.Body())+"'")
}

// DecodeMonitorSwaggerError function
func DecodeMonitorSwaggerError(err error, response interface{}) {
	decodeMonitorSwaggerError(err, response, 2)
}

func decodeMonitorSwaggerError(err error, response interface{}, offset int) {
	ExpectWithOffset(offset, err).To(HaveOccurred(), "Expected monitor swagger error to have occurred")
	var swaggerError monitor.GenericSwaggerError
	ExpectWithOffset(offset, errors.As(err, &swaggerError)).To(BeTrue(), "Expected monitor swagger error to be of type monitor.GenericSwaggerError")
	jsonErr := json.Unmarshal(swaggerError.Body(), response)
	ExpectWithOffset(offset, jsonErr).ToNot(HaveOccurred(), "Expected to be able to decode response body to response object. Data: '"+string(swaggerError.Body())+"'")
}
