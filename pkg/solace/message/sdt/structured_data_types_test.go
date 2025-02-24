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

package sdt_test

import (
	"errors"
	"fmt"
	"reflect"
	"testing"

	"solace.dev/go/messaging/pkg/solace/message/sdt"
	"solace.dev/go/messaging/pkg/solace/resource"
)

type typeCase struct {
	t         reflect.Type
	streamFn  func(sdtStream sdt.Stream, index int) (sdt.Data, error)
	mapFn     func(sdtMap sdt.Map, key string) (sdt.Data, error)
	testCases []testCase
}

var typeCases = []typeCase{
	// Integer types
	{
		t: reflect.TypeOf(int8(0)),
		streamFn: func(sdtStream sdt.Stream, index int) (sdt.Data, error) {
			return sdtStream.GetInt8(index)
		},
		mapFn: func(sdtMap sdt.Map, key string) (sdt.Data, error) {
			return sdtMap.GetInt8(key)
		},
		testCases: generateIntegerTestCases(reflect.TypeOf(int8(0)), false),
	},
	{
		t: reflect.TypeOf(int16(0)),
		streamFn: func(sdtStream sdt.Stream, index int) (sdt.Data, error) {
			return sdtStream.GetInt16(index)
		},
		mapFn: func(sdtMap sdt.Map, key string) (sdt.Data, error) {
			return sdtMap.GetInt16(key)
		},
		testCases: generateIntegerTestCases(reflect.TypeOf(int16(0)), false),
	},
	{
		t: reflect.TypeOf(int32(0)),
		streamFn: func(sdtStream sdt.Stream, index int) (sdt.Data, error) {
			return sdtStream.GetInt32(index)
		},
		mapFn: func(sdtMap sdt.Map, key string) (sdt.Data, error) {
			return sdtMap.GetInt32(key)
		},
		testCases: generateIntegerTestCases(reflect.TypeOf(int32(0)), false),
	},
	{
		t: reflect.TypeOf(int64(0)),
		streamFn: func(sdtStream sdt.Stream, index int) (sdt.Data, error) {
			return sdtStream.GetInt64(index)
		},
		mapFn: func(sdtMap sdt.Map, key string) (sdt.Data, error) {
			return sdtMap.GetInt64(key)
		},
		testCases: generateIntegerTestCases(reflect.TypeOf(int64(0)), false),
	},
	{
		t: reflect.TypeOf(int(0)),
		streamFn: func(sdtStream sdt.Stream, index int) (sdt.Data, error) {
			return sdtStream.GetInt(index)
		},
		mapFn: func(sdtMap sdt.Map, key string) (sdt.Data, error) {
			return sdtMap.GetInt(key)
		},
		testCases: generateIntegerTestCases(reflect.TypeOf(int(0)), false),
	},
	// Unsigned integer types
	{
		t: reflect.TypeOf(byte(0)),
		streamFn: func(sdtStream sdt.Stream, index int) (sdt.Data, error) {
			return sdtStream.GetByte(index)
		},
		mapFn: func(sdtMap sdt.Map, key string) (sdt.Data, error) {
			return sdtMap.GetByte(key)
		},
		testCases: generateIntegerTestCases(reflect.TypeOf(byte(0)), true),
	},
	{
		t: reflect.TypeOf(uint8(0)),
		streamFn: func(sdtStream sdt.Stream, index int) (sdt.Data, error) {
			return sdtStream.GetUInt8(index)
		},
		mapFn: func(sdtMap sdt.Map, key string) (sdt.Data, error) {
			return sdtMap.GetUInt8(key)
		},
		testCases: generateIntegerTestCases(reflect.TypeOf(uint8(0)), true),
	},
	{
		t: reflect.TypeOf(uint16(0)),
		streamFn: func(sdtStream sdt.Stream, index int) (sdt.Data, error) {
			return sdtStream.GetUInt16(index)
		},
		mapFn: func(sdtMap sdt.Map, key string) (sdt.Data, error) {
			return sdtMap.GetUInt16(key)
		},
		testCases: generateIntegerTestCases(reflect.TypeOf(uint16(0)), true),
	},
	{
		t: reflect.TypeOf(uint32(0)),
		streamFn: func(sdtStream sdt.Stream, index int) (sdt.Data, error) {
			return sdtStream.GetUInt32(index)
		},
		mapFn: func(sdtMap sdt.Map, key string) (sdt.Data, error) {
			return sdtMap.GetUInt32(key)
		},
		testCases: generateIntegerTestCases(reflect.TypeOf(uint32(0)), true),
	},
	{
		t: reflect.TypeOf(uint64(0)),
		streamFn: func(sdtStream sdt.Stream, index int) (sdt.Data, error) {
			return sdtStream.GetUInt64(index)
		},
		mapFn: func(sdtMap sdt.Map, key string) (sdt.Data, error) {
			return sdtMap.GetUInt64(key)
		},
		testCases: generateIntegerTestCases(reflect.TypeOf(uint64(0)), true),
	},
	{
		t: reflect.TypeOf(uint(0)),
		streamFn: func(sdtStream sdt.Stream, index int) (sdt.Data, error) {
			return sdtStream.GetUInt(index)
		},
		mapFn: func(sdtMap sdt.Map, key string) (sdt.Data, error) {
			return sdtMap.GetUInt(key)
		},
		testCases: generateIntegerTestCases(reflect.TypeOf(uint(0)), true),
	},
	{
		t: reflect.TypeOf(true),
		streamFn: func(sdtStream sdt.Stream, index int) (sdt.Data, error) {
			return sdtStream.GetBool(index)
		},
		mapFn: func(sdtMap sdt.Map, key string) (sdt.Data, error) {
			return sdtMap.GetBool(key)
		},
		testCases: generateBooleanTestCases(),
	},
	// Byte array
	{
		t: reflect.TypeOf([]byte{1}),
		streamFn: func(sdtStream sdt.Stream, index int) (sdt.Data, error) {
			return sdtStream.GetByteArray(index)
		},
		mapFn: func(sdtMap sdt.Map, key string) (sdt.Data, error) {
			return sdtMap.GetByteArray(key)
		},
		testCases: append(
			generateGenericInvalidTestCases([]byte(nil), []byte{}, ""),
			generateBaseCaseTestCase([]byte{1, 2, 3, 4}),
			testCase{"Hello World", []byte("Hello World"), false},
			testCase{"", []byte(""), false},
		),
	},
	// floats
	{
		t: reflect.TypeOf(float32(0)),
		streamFn: func(sdtStream sdt.Stream, index int) (sdt.Data, error) {
			return sdtStream.GetFloat32(index)
		},
		mapFn: func(sdtMap sdt.Map, key string) (sdt.Data, error) {
			return sdtMap.GetFloat32(key)
		},
		testCases: append(
			generateGenericInvalidTestCases(float32(0), float32(0)),
			generateBaseCaseTestCase(float32(100)),
			testCase{"1.23", float32(1.23), false},
		),
	},
	{
		t: reflect.TypeOf(float64(0)),
		streamFn: func(sdtStream sdt.Stream, index int) (sdt.Data, error) {
			return sdtStream.GetFloat64(index)
		},
		mapFn: func(sdtMap sdt.Map, key string) (sdt.Data, error) {
			return sdtMap.GetFloat64(key)
		},
		testCases: append(
			generateGenericInvalidTestCases(float64(0), float64(0)),
			generateBaseCaseTestCase(float64(100)),
			testCase{"1.23", float64(1.23), false},
		),
	},
	// String
	{
		t: reflect.TypeOf(""),
		streamFn: func(sdtStream sdt.Stream, index int) (sdt.Data, error) {
			return sdtStream.GetString(index)
		},
		mapFn: func(sdtMap sdt.Map, key string) (sdt.Data, error) {
			return sdtMap.GetString(key)
		},
		testCases: append(
			generateGenericInvalidTestCases("", ""),
			generateBaseCaseTestCase("hello world"),
		),
	},
	// Wchar
	{
		t: reflect.TypeOf(sdt.WChar(0)),
		streamFn: func(sdtStream sdt.Stream, index int) (sdt.Data, error) {
			return sdtStream.GetWChar(index)
		},
		mapFn: func(sdtMap sdt.Map, key string) (sdt.Data, error) {
			return sdtMap.GetWChar(key)
		},
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
		t: reflect.TypeOf(sdt.Map{}),
		streamFn: func(sdtStream sdt.Stream, index int) (sdt.Data, error) {
			return sdtStream.GetMap(index)
		},
		mapFn: func(sdtMap sdt.Map, key string) (sdt.Data, error) {
			return sdtMap.GetMap(key)
		},
		testCases: append(
			generateGenericInvalidTestCases(sdt.Map(nil), sdt.Map{}),
			generateBaseCaseTestCase(sdt.Map{
				"some": "data",
			}),
		),
	},
	{
		t: reflect.TypeOf(sdt.Stream{}),
		streamFn: func(sdtStream sdt.Stream, index int) (sdt.Data, error) {
			return sdtStream.GetStream(index)
		},
		mapFn: func(sdtMap sdt.Map, key string) (sdt.Data, error) {
			return sdtMap.GetStream(key)
		},
		testCases: append(
			generateGenericInvalidTestCases(sdt.Stream(nil), sdt.Stream{}),
			generateBaseCaseTestCase(sdt.Stream{
				"data",
			}),
		),
	},
	// Destinations
	{
		t: reflect.TypeOf(&resource.Queue{}),
		streamFn: func(sdtStream sdt.Stream, index int) (sdt.Data, error) {
			return sdtStream.GetQueue(index)
		},
		mapFn: func(sdtMap sdt.Map, key string) (sdt.Data, error) {
			return sdtMap.GetQueue(key)
		},
		testCases: append(
			generateGenericInvalidTestCases((*resource.Queue)(nil), &resource.Queue{}),
			generateBaseCaseTestCase(resource.QueueDurableExclusive("MyQueue")),
		),
	},
	{
		t: reflect.TypeOf(&resource.Topic{}),
		streamFn: func(sdtStream sdt.Stream, index int) (sdt.Data, error) {
			return sdtStream.GetTopic(index)
		},
		mapFn: func(sdtMap sdt.Map, key string) (sdt.Data, error) {
			return sdtMap.GetTopic(key)
		},
		testCases: append(
			generateGenericInvalidTestCases((*resource.Topic)(nil), &resource.Topic{}),
			generateBaseCaseTestCase(resource.TopicOf("MyTopic")),
		),
	},
	{
		t: reflect.TypeOf(resource.Destination(&resource.Topic{})),
		streamFn: func(sdtStream sdt.Stream, index int) (sdt.Data, error) {
			return sdtStream.GetDestination(index)
		},
		mapFn: func(sdtMap sdt.Map, key string) (sdt.Data, error) {
			return sdtMap.GetDestination(key)
		},
		testCases: append(
			generateGenericInvalidTestCases((resource.Destination)(nil), &resource.Topic{}, &resource.Queue{}),
			generateBaseCaseTestCase(resource.QueueDurableExclusive("MyQueue")),
			generateBaseCaseTestCase(resource.TopicOf("MyTopic")),
		),
	},
}

type testCase struct {
	input  interface{}
	output interface{}
	err    bool
}

func TestMapSDT(t *testing.T) {
	for _, cases := range typeCases {
		for _, tc := range cases.testCases {
			const key = "some_key"
			m := sdt.Map{}
			m[key] = tc.input
			output, err := cases.mapFn(m, key)
			checkResult(t, cases, tc, output, err)
		}
	}
}

func TestMapMissingKey(t *testing.T) {
	for _, cases := range typeCases {
		const badKey = "notakey"
		m := sdt.Map{}
		_, err := cases.mapFn(m, badKey)
		var keyNotFoundErr *sdt.KeyNotFoundError
		if !errors.As(err, &keyNotFoundErr) {
			t.Errorf("Unexpected error type for missing key error: %T", err)
		}
		if keyNotFoundErr.Key != badKey {
			t.Errorf("Key not set in error, got %s", keyNotFoundErr.Key)
		}
		if err != nil && err.Error() == "" {
			t.Errorf("Expected error message to be present")
		}
	}
}

func TestStreamSDT(t *testing.T) {
	for _, cases := range typeCases {
		for _, tc := range cases.testCases {
			const index = 0
			s := make(sdt.Stream, index+1)
			s[index] = tc.input
			output, err := cases.streamFn(s, index)
			checkResult(t, cases, tc, output, err)
		}
	}
}

func TestStreamMissingIndex(t *testing.T) {
	for _, cases := range typeCases {
		const badIndex = 1
		s := sdt.Stream{}
		_, err := cases.streamFn(s, badIndex)
		var keyNotFoundErr *sdt.OutOfBoundsError
		if !errors.As(err, &keyNotFoundErr) {
			t.Errorf("Unexpected error type for out of bounds error: %T", err)
		}
		if keyNotFoundErr.Index != badIndex {
			t.Errorf("Key not set in error, got %d", keyNotFoundErr.Index)
		}
		if err != nil && err.Error() == "" {
			t.Errorf("Expected error message to be present")
		}
	}
}

func checkResult(t *testing.T, c typeCase, tc testCase, output interface{}, err error) {
	if !reflect.DeepEqual(output, tc.output) {
		t.Errorf("Test Expect %s: Expected %v (%T) to equal %v (%T) in test with input %T value '%v'", c.t.Name(), output, output, tc.output, tc.output, tc.input, tc.input)
	}
	if !tc.err && err != nil {
		t.Errorf("Test Expect %s: Unexpected error: got %T, expected nil", c.t.Name(), err)
	}
	if tc.err {
		if reflect.TypeOf(err) != reflect.TypeOf(&sdt.FormatConversionError{}) {
			t.Errorf("Test Expect %s: Unexpected error: got %T in test with input %T value '%v', expected %T", c.t.Name(), err, tc.input, tc.input, &sdt.FormatConversionError{})
		}
		if err != nil && err.Error() == "" {
			t.Errorf("Expected error message to be present")
		}
	}
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
func generateBaseCaseTestCase(validValue interface{}) testCase {
	return testCase{validValue, validValue, false}
}

func generateGenericInvalidTestCases(zeroVal interface{}, excludedChecks ...interface{}) []testCase {
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

type dummyStruct struct {
}

func generateBooleanTestCases() []testCase {
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

func generateIntegerTestCases(t reflect.Type, unsigned bool) []testCase {
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
