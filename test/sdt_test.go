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

package test

import (
	"fmt"
	"reflect"

	"solace.dev/go/messaging/pkg/solace/message/sdt"
	"solace.dev/go/messaging/pkg/solace/resource"
	"solace.dev/go/messaging/test/helpers"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("SDT Container Tests", func() {

	type testCase struct {
		input  interface{}
		output interface{}
		err    bool
	}

	type typeCase struct {
		typeName  string
		name      string
		testCases []testCase
	}

	var intTypes = []reflect.Type{
		reflect.TypeOf(int(0)),
		reflect.TypeOf(int8(0)),
		reflect.TypeOf(int16(0)),
		reflect.TypeOf(int32(0)),
		reflect.TypeOf(int64(0)),
	}

	var uintTypes = []reflect.Type{
		reflect.TypeOf(uint(0)),
		reflect.TypeOf(byte(0)),
		reflect.TypeOf(uint8(0)),
		reflect.TypeOf(uint16(0)),
		reflect.TypeOf(uint32(0)),
		reflect.TypeOf(uint64(0)),
	}

	// for one to one mappings
	generateBaseCaseTestCase := func(validValue interface{}) testCase {
		return testCase{validValue, validValue, false}
	}

	generateGenericInvalidTestCases := func(zeroVal interface{}, excludedChecks ...interface{}) []testCase {
		testCases := []testCase{}
		additionalValues := []interface{}{
			false, nil, dummyStruct{}, &dummyStruct{}, "Hello World",
			sdt.WChar(55), sdt.Map{}, sdt.Stream{}, &resource.Queue{},
			&resource.Topic{}, float32(1.23), float64(1.23),
		}
		for _, t := range append(intTypes, uintTypes...) {
			testCases = append(testCases, testCase{reflect.ValueOf(0).Convert(t).Interface(), zeroVal, true})
		}
		for _, val := range additionalValues {
			testCases = append(testCases, testCase{val, zeroVal, true})
		}

		toRemove := []int{}
		for i, check := range testCases {
			for _, excludedCheck := range excludedChecks {
				if check.input != nil && reflect.TypeOf(check.input) == reflect.TypeOf(excludedCheck) {
					toRemove = append(toRemove, i)
				}
			}
		}
		for i, r := range toRemove {
			testCases = append(testCases[:r-i], testCases[r-i+1:]...)
		}
		return testCases
	}

	generateBooleanTestCases := func() []testCase {
		// valid test case generator
		tc := func(input interface{}, output bool) testCase {
			return testCase{input, output, false}
		}
		// invalid test case generator
		itc := func(input interface{}) testCase {
			return testCase{input, false, true}
		}
		testCases := []testCase{}
		add := func(cases ...testCase) {
			testCases = append(testCases, cases...)
		}
		add(tc(true, true), tc(false, false))

		for _, t := range append(intTypes, uintTypes...) {
			add(
				tc(reflect.ValueOf(0).Convert(t).Interface(), false),
				tc(reflect.ValueOf(1).Convert(t).Interface(), true),
				tc(reflect.ValueOf(100).Convert(t).Interface(), true),
			)
		}

		add(tc("true", true), tc("false", false), tc("1", true), tc("0", false))
		add(itc("True"), itc("2"), itc("False"), itc("Hello World"))
		add(itc([]byte{1}))
		add(itc(float32(1.01)))
		add(itc(float64(1.01)))
		add(itc(sdt.WChar(64)))
		add(itc(sdt.Map{}))
		add(itc(sdt.Stream{}))
		add(itc(&resource.Queue{}))
		add(itc(&resource.Topic{}))
		add(itc(dummyStruct{}))
		add(itc(&dummyStruct{}))
		return testCases
	}

	generateIntegerTestCases := func(t reflect.Type, unsigned bool) []testCase {
		// valid test case generator
		tc := func(input interface{}, output int64) testCase {
			return testCase{input, reflect.ValueOf(output).Convert(t).Interface(), false}
		}
		// invalid test case generator
		itc := func(input interface{}) testCase {
			return testCase{input, reflect.ValueOf(0).Convert(t).Interface(), true}
		}
		testCases := []testCase{}
		add := func(cases ...testCase) {
			testCases = append(testCases, cases...)
		}

		bits := t.Bits()
		// boolean
		add(tc(true, 1), tc(false, 0))

		generateTestCaseForIntType := func(i reflect.Type) {
			convert := func(val int64) interface{} {
				return reflect.ValueOf(val).Convert(i).Interface()
			}
			inttc := func(input int64) testCase {
				return tc(convert(input), input)
			}
			// we can always add the 0 case
			add(inttc(0))
			if unsigned {
				add(itc(convert(-1)))
			}
			if bits >= i.Bits() {
				// if we're greater or we're the same
				add(inttc(1<<(i.Bits()-1) - 1))
			} else {
				// we have fewer bits, check the extremeties
				add(itc(convert(1 << bits)))
			}
		}
		for _, intType := range intTypes {
			generateTestCaseForIntType(intType)
		}

		generateTestCaseForUintType := func(i reflect.Type) {
			convert := func(val uint64) interface{} {
				return reflect.ValueOf(val).Convert(i).Interface()
			}
			uinttc := func(input uint64) testCase {
				return tc(convert(input), int64(input))
			}
			// we can always add the 0 case
			add(uinttc(0))
			if bits >= i.Bits() {
				if bits == i.Bits() && !unsigned {
					// signedness counts here since we can't convert above the bit shift-1
					add(uinttc(1<<(bits-1)-1), itc(convert(1<<bits-1)))
				} else {
					// if we're greater or we're the same
					add(uinttc(1<<i.Bits() - 1))
				}
			} else {
				// we have fewer bits, check the extremeties
				add(itc(convert(1 << bits)))
			}
		}
		for _, uintType := range uintTypes {
			generateTestCaseForUintType(uintType)
		}

		add(tc("1", 1))
		if unsigned {
			add(itc("-1"))
		} else {
			add(tc("-1", -1))
		}
		if bits != 64 {
			add(itc(fmt.Sprint(1 << bits)))
		}
		add(itc([]byte{1}))
		add(itc(float32(0.1)))
		add(itc(float64(0.1)))
		add(itc(sdt.WChar(64)))
		add(itc(nil))
		add(itc(sdt.Map{}))
		add(itc(sdt.Stream{}))
		add(itc(&resource.Queue{}))
		add(itc(&resource.Topic{}))
		add(itc(dummyStruct{}))
		add(itc(&dummyStruct{}))
		return testCases
	}

	var typeCases = []typeCase{
		// Integer types
		{
			typeName:  "int8",
			name:      "GetInt8",
			testCases: generateIntegerTestCases(reflect.TypeOf(int8(0)), false),
		},
		{
			typeName:  "int16",
			name:      "GetInt16",
			testCases: generateIntegerTestCases(reflect.TypeOf(int16(0)), false),
		},
		{
			typeName:  "int32",
			name:      "GetInt32",
			testCases: generateIntegerTestCases(reflect.TypeOf(int32(0)), false),
		},
		{
			typeName:  "int64",
			name:      "GetInt64",
			testCases: generateIntegerTestCases(reflect.TypeOf(int64(0)), false),
		},
		{
			typeName:  "int",
			name:      "GetInt",
			testCases: generateIntegerTestCases(reflect.TypeOf(int(0)), false),
		},
		// Unsigned integer types
		{
			typeName:  "uint8",
			name:      "GetByte",
			testCases: generateIntegerTestCases(reflect.TypeOf(byte(0)), true),
		},
		{
			typeName:  "uint8",
			name:      "GetUInt8",
			testCases: generateIntegerTestCases(reflect.TypeOf(uint8(0)), true),
		},
		{
			typeName:  "uint16",
			name:      "GetUInt16",
			testCases: generateIntegerTestCases(reflect.TypeOf(uint16(0)), true),
		},
		{
			typeName:  "uint32",
			name:      "GetUInt32",
			testCases: generateIntegerTestCases(reflect.TypeOf(uint32(0)), true),
		},
		{
			typeName:  "uint64",
			name:      "GetUInt64",
			testCases: generateIntegerTestCases(reflect.TypeOf(uint64(0)), true),
		},
		{
			typeName:  "uint",
			name:      "GetUInt",
			testCases: generateIntegerTestCases(reflect.TypeOf(uint(0)), true),
		},
		{
			typeName:  "bool",
			name:      "GetBool",
			testCases: generateBooleanTestCases(),
		},
		// Byte array
		{
			typeName: "[]uint8",
			name:     "GetByteArray",
			testCases: append(
				generateGenericInvalidTestCases([]byte(nil), []byte{}, ""),
				generateBaseCaseTestCase([]byte{1, 2, 3, 4}),
				testCase{"Hello World", []byte("Hello World"), false},
				testCase{"", []byte(""), false},
			),
		},
		// floats
		{
			typeName: "float32",
			name:     "GetFloat32",
			testCases: append(
				generateGenericInvalidTestCases(float32(0), float32(0)),
				generateBaseCaseTestCase(float32(100)),
				testCase{"1.23", float32(1.23), false},
			),
		},
		{
			typeName: "float64",
			name:     "GetFloat64",
			testCases: append(
				generateGenericInvalidTestCases(float64(0), float64(0)),
				generateBaseCaseTestCase(float64(100)),
				testCase{"1.23", float64(1.23), false},
			),
		},
		// String
		{
			typeName: "string",
			name:     "GetString",
			testCases: append(
				generateGenericInvalidTestCases("", ""),
				generateBaseCaseTestCase("hello world"),
			),
		},
		// Wchar
		{
			typeName: "sdt.WChar",
			name:     "GetWChar",
			testCases: append(
				generateGenericInvalidTestCases(sdt.WChar(0), sdt.WChar(0), int8(0), uint8(0)),
				generateBaseCaseTestCase(sdt.WChar(123)),
				testCase{int8(123), sdt.WChar(123), false},
				testCase{uint8(123), sdt.WChar(123), false},
				testCase{"1", sdt.WChar('1'), false},
			),
		},
		// SDT Types
		{
			typeName: "sdt.Map",
			name:     "GetMap",
			testCases: append(
				generateGenericInvalidTestCases(sdt.Map(nil), sdt.Map{}),
				generateBaseCaseTestCase(sdt.Map{
					"some": "data",
				}),
			),
		},
		{
			typeName: "sdt.Stream",
			name:     "GetStream",
			testCases: append(
				generateGenericInvalidTestCases(sdt.Stream(nil), sdt.Stream{}),
				generateBaseCaseTestCase(sdt.Stream{
					"data",
				}),
			),
		},
		// Destinations
		{
			typeName: "*resource.Queue",
			name:     "GetQueue",
			testCases: append(
				generateGenericInvalidTestCases((*resource.Queue)(nil), &resource.Queue{}),
				generateBaseCaseTestCase(resource.QueueDurableExclusive("MyQueue")),
			),
		},
		{
			typeName: "*resource.Topic",
			name:     "GetTopic",
			testCases: append(
				generateGenericInvalidTestCases((*resource.Topic)(nil), &resource.Topic{}),
				generateBaseCaseTestCase(resource.TopicOf("MyTopic")),
			),
		},
		{
			typeName: "resource.Destination",
			name:     "GetDestination",
			testCases: append(
				generateGenericInvalidTestCases((resource.Destination)(nil), &resource.Topic{}, &resource.Queue{}),
				generateBaseCaseTestCase(resource.QueueDurableExclusive("MyQueue")),
				generateBaseCaseTestCase(resource.TopicOf("MyTopic")),
			),
		},
	}

	for _, casesRef := range typeCases {
		cases := casesRef
		Describe(cases.name, func() {
			for _, testCaseRef := range cases.testCases {
				testCase := testCaseRef
				validateOutput := func(values []reflect.Value) {
					Expect(len(values)).To(Equal(2))
					output := values[0].Interface()
					if testCase.output != nil {
						Expect(output).To(Equal(testCase.output))
					} else {
						Expect(output).To(BeNil())
					}
					errInterface := values[1].Interface()
					if testCase.err {
						err, ok := errInterface.(error)
						Expect(ok).To(BeTrue())
						helpers.ValidateError(err, &sdt.FormatConversionError{}, cases.typeName)
					} else {
						Expect(errInterface).ToNot(HaveOccurred())
					}
				}

				When("using a map", func() {
					const mapKey = "some_key"
					var sdtMap sdt.Map
					var mapMethod reflect.Value
					BeforeEach(func() {
						sdtMap = sdt.Map{}
						mapMethod = reflect.ValueOf(sdtMap).MethodByName(cases.name)
					})

					It(fmt.Sprintf("expects output %v with input %v expecting error %t", testCase.output, testCase.input, testCase.err), func() {
						sdtMap[mapKey] = testCase.input
						values := mapMethod.Call([]reflect.Value{reflect.ValueOf(mapKey)})
						validateOutput(values)
					})
				})

				When("using a stream", func() {

					var sdtStream sdt.Stream
					var streamMethod reflect.Value
					BeforeEach(func() {
						sdtStream = make(sdt.Stream, 1)
						streamMethod = reflect.ValueOf(sdtStream).MethodByName(cases.name)
					})

					It(fmt.Sprintf("expects output %v with input %v expecting error %t", testCase.output, testCase.input, testCase.err), func() {
						sdtStream[0] = testCase.input
						values := streamMethod.Call([]reflect.Value{reflect.ValueOf(0)})
						validateOutput(values)
					})
				})
			}

			It("fails to retrieve an unknown map key", func() {
				const notAKey = "notakey"
				emptyMap := sdt.Map{}
				results := reflect.ValueOf(emptyMap).MethodByName(cases.name).Call([]reflect.Value{
					reflect.ValueOf(notAKey),
				})
				Expect(len(results)).To(Equal(2))
				err, ok := results[1].Interface().(error)
				Expect(ok).To(BeTrue())
				helpers.ValidateError(err, &sdt.KeyNotFoundError{}, notAKey)
			})

			It("fails to retrieve an out of range stream key", func() {
				const outOfBounds = 0
				emptyStream := sdt.Stream{}
				results := reflect.ValueOf(emptyStream).MethodByName(cases.name).Call([]reflect.Value{
					reflect.ValueOf(outOfBounds),
				})
				Expect(len(results)).To(Equal(2))
				err, ok := results[1].Interface().(error)
				Expect(ok).To(BeTrue())
				helpers.ValidateError(err, &sdt.OutOfBoundsError{}, fmt.Sprint(outOfBounds))
			})
		})
	}
})
