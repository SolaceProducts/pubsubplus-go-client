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

package message

import (
	"testing"

	"solace.dev/go/messaging/pkg/solace"
	"solace.dev/go/messaging/pkg/solace/message/rgmid"

	// Include core to boot up CCSMP with solclient initialize
	_ "solace.dev/go/messaging/internal/impl/core"
)

func TestRMIDToString(t *testing.T) {
	rmidString := "rmid1:0d77c-b0b2e66aece-00000000-00000001"
	rmid, err := ReplicationGroupMessageIDFromString(rmidString)
	if err != nil {
		t.Error(err)
	}
	if rmid == nil {
		t.Error("Expected non nil RMID, got nil")
	}
	actualRmidToString := rmid.String()
	if actualRmidToString != rmidString {
		t.Errorf("Expected RMID %s to equal %s", actualRmidToString, rmidString)
	}
}

func TestRMIDComparison(t *testing.T) {
	a := "rmid1:0d77c-b0b2e66aece-00000000-00000001"
	b := "rmid1:0d77c-b0b2e66aece-00000000-00000002"
	rmidA, err := ReplicationGroupMessageIDFromString(a)
	if err != nil {
		t.Error(err)
	}
	if rmidA == nil {
		t.Error("Expected non nil RMID, got nil")
	}
	rmidB, err := ReplicationGroupMessageIDFromString(b)
	if err != nil {
		t.Error(err)
	}
	if rmidB == nil {
		t.Error("Expected non nil RMID, got nil")
	}
	result, err := rmidA.Compare(rmidB)
	if err != nil {
		t.Error(err)
	}
	if result != -1 {
		t.Errorf("Expected result to be -1, got %d", result)
	}
	result, err = rmidB.Compare(rmidA)
	if err != nil {
		t.Error(err)
	}
	if result != 1 {
		t.Errorf("Expected result to be 1, got %d", result)
	}
}

func TestRMIDComparisonDifferentGroups(t *testing.T) {
	a := "rmid1:0d77c-b0b2e66aece-00000000-00000001"
	b := "rmid1:0d77c-b0b2e66aecd-00000000-00000002"
	rmidA, err := ReplicationGroupMessageIDFromString(a)
	if err != nil {
		t.Error(err)
	}
	if rmidA == nil {
		t.Error("Expected non nil RMID, got nil")
	}
	rmidB, err := ReplicationGroupMessageIDFromString(b)
	if err != nil {
		t.Error(err)
	}
	if rmidB == nil {
		t.Error("Expected non nil RMID, got nil")
	}
	_, err = rmidB.Compare(rmidA)
	if err == nil {
		t.Error("Expected non nil error")
	}
	if _, ok := err.(*solace.IllegalArgumentError); !ok {
		t.Errorf("Expected error to be of type IllegalArgumentError, got %T", err)
	}
	_, err = rmidA.Compare(rmidB)
	if err == nil {
		t.Error("Expected non nil error")
	}
	if _, ok := err.(*solace.IllegalArgumentError); !ok {
		t.Errorf("Expected error to be of type IllegalArgumentError, got %T", err)
	}
}

func TestRMIDComparisonEquality(t *testing.T) {
	a := "rmid1:0d77c-b0b2e66aece-00000000-00000001"
	rmidA, err := ReplicationGroupMessageIDFromString(a)
	if err != nil {
		t.Error(err)
	}
	if rmidA == nil {
		t.Error("Expected non nil RMID, got nil")
	}
	rmidB, err := ReplicationGroupMessageIDFromString(a)
	if err != nil {
		t.Error(err)
	}
	if rmidB == nil {
		t.Error("Expected non nil RMID, got nil")
	}
	result, err := rmidA.Compare(rmidB)
	if err != nil {
		t.Error(err)
	}
	if result != 0 {
		t.Errorf("Expected result to be 0, got %d", result)
	}
	result, err = rmidB.Compare(rmidA)
	if err != nil {
		t.Error(err)
	}
	if result != 0 {
		t.Errorf("Expected result to be 0, got %d", result)
	}
}

func TestInvalidRMIDFromString(t *testing.T) {
	rmidInvalidString := "something thats definitely not an RMID"
	rmid, err := ReplicationGroupMessageIDFromString(rmidInvalidString)
	if err == nil {
		t.Error("Expected error, got nil")
	}
	if _, ok := err.(*solace.IllegalArgumentError); !ok {
		t.Errorf("Expected error to be of type *solace.IllegalArgumentError, got %T", err)
	}
	if rmid != nil {
		t.Error("Expected returned rmid to be nil")
	}
}

func TestInvalidRMIDToString(t *testing.T) {
	rmidNotAllocatedProperly := replicationGroupMessageID{}
	str := rmidNotAllocatedProperly.String()
	if str != "" {
		t.Error("got unexpected string: " + str)
	}
}

func TestRMIDComparisonInvalidType(t *testing.T) {
	a := "rmid1:0d77c-b0b2e66aece-00000000-00000001"
	rmidA, err := ReplicationGroupMessageIDFromString(a)
	if err != nil {
		t.Error(err)
	}
	if rmidA == nil {
		t.Error("Expected non nil RMID, got nil")
	}
	rmidB := &dummyRmidStruct{}
	_, err = rmidA.Compare(rmidB)
	if err == nil {
		t.Error("Expected non nil error")
	}
	if _, ok := err.(*solace.IllegalArgumentError); !ok {
		t.Errorf("Expected error to be of type IllegalArgumentError, got %T", err)
	}
}

type dummyRmidStruct struct{}

func (dummy *dummyRmidStruct) Compare(_ rgmid.ReplicationGroupMessageID) (int, error) {
	return 0, nil
}

func (dummy *dummyRmidStruct) String() string {
	return ""
}
