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

import "testing"

func TestFlattenMap(t *testing.T) {
	input := map[string]interface{}{
		"hello": map[string]interface{}{
			"world": 1234,
		},
	}
	expectedOutput := map[string]interface{}{
		"hello.world": 1234,
	}
	output := flattenMap(input)
	for k, v := range output {
		expected, ok := expectedOutput[k]
		if !ok {
			t.Errorf("did not expect to find key " + k)
		}
		if expected != v {
			t.Errorf("got wrong value, expected %s, got %s", expected, v)
		}
	}
	for k := range expectedOutput {
		_, ok := output[k]
		if !ok {
			t.Error("expected to find key " + k)
		}
	}
}

func TestJSONFlattenMap(t *testing.T) {
	input := []byte(`{"hello":{"world":{"test":1234}}}`)
	expectedOutput := map[string]interface{}{
		"hello.world.test": float64(1234), // json automatically parses all numbers as float64
	}
	output, err := flattenJSON(input)
	if err != nil {
		t.Errorf("did not expect error %s", err)
	}
	for k, v := range output {
		expected, ok := expectedOutput[k]
		if !ok {
			t.Errorf("did not expect to find key " + k)
		}
		if expected != v {
			t.Errorf("got wrong value, expected %s, got %s", expected, v)
		}
	}
	for k := range expectedOutput {
		_, ok := output[k]
		if !ok {
			t.Error("expected to find key " + k)
		}
	}
}

func TestJSONFlattenMapInvalid(t *testing.T) {
	input := []byte(`thisisnotjson`)
	output, err := flattenJSON(input)
	if err == nil {
		t.Error("expected error to not be nil when passed invalid JSON")
	}
	if output != nil {
		t.Errorf("did not expect output to be returned, got %s", output)
	}
}

func TestNestMap(t *testing.T) {
	input := map[string]interface{}{
		"hello.world.test": 1234,
	}
	expectedOutput := map[string]interface{}{
		"hello": map[string]interface{}{
			"world": map[string]interface{}{
				"test": 1234,
			},
		},
	}
	output, err := nestMap(input)
	if err != nil {
		t.Errorf("did not expect error %s", err)
	}

	var compareMap func(a, b map[string]interface{})
	compareMap = func(a, b map[string]interface{}) {
		for k, vA := range a {
			vB, ok := b[k]
			if !ok {
				t.Errorf("did not expect to find key " + k)
			}
			castedValA, okA := vA.(map[string]interface{})
			castedValB, okB := vB.(map[string]interface{})
			if okA != okB {
				t.Errorf("got two different types, expected %T, got %T", vB, vA)
			}
			if okA {
				compareMap(castedValA, castedValB)
			} else {
				if vA != vB {
					t.Errorf("got wrong value, expected %s, got %s", vB, vA)
				}
			}
		}
		for k := range b {
			_, ok := a[k]
			if !ok {
				t.Error("expected to find key " + k)
			}
		}
	}
	compareMap(expectedOutput, output)
}

func TestNestMapInvalidKeys(t *testing.T) {
	input := map[string]interface{}{
		"hello":       2034,
		"hello.world": 1234,
	}
	output, err := nestMap(input)
	if err != errIllegalKey {
		t.Errorf("expected illegal key error, got %s", err)
	}
	if output != nil {
		t.Error("expected output to be nil")
	}
	input = map[string]interface{}{
		"hello.world": 1234,
		"hello":       2034,
	}
	output, err = nestMap(input)
	if err != errIllegalKey {
		t.Errorf("expected illegal key error, got %s", err)
	}
	if output != nil {
		t.Error("expected output to be nil")
	}
}

func TestNestJSON(t *testing.T) {
	input := map[string]interface{}{
		"hello.world": 1234,
	}
	expectedOutput := []byte(`{"hello":{"world":1234}}`)
	output, err := nestJSON(input)
	if err != nil {
		t.Errorf("expected error to be nil, got %s", err)
	}
	if len(output) != len(expectedOutput) {
		t.Errorf("expected arrays to be equal, got %s != %s", output, expectedOutput)
	}
	for i, v := range expectedOutput {
		if output[i] != v {
			t.Errorf("expected arrays to be equal, got %s != %s", output, expectedOutput)
		}
	}
}

func TestNestJSONWithError(t *testing.T) {
	input := map[string]interface{}{
		"hello":       2034,
		"hello.world": 1234,
	}
	output, err := nestJSON(input)
	if err != errIllegalKey {
		t.Errorf("expected illegal key error, got %s", err)
	}
	if output != nil {
		t.Error("expected output to be nil")
	}
	input = map[string]interface{}{
		"hello.world": 1234,
		"hello":       2034,
	}
	output, err = nestJSON(input)
	if err != errIllegalKey {
		t.Errorf("expected illegal key error, got %s", err)
	}
	if output != nil {
		t.Error("expected output to be nil")
	}
}
