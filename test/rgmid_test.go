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
	"solace.dev/go/messaging"
	"solace.dev/go/messaging/pkg/solace"
	"solace.dev/go/messaging/pkg/solace/message/rgmid"
	"solace.dev/go/messaging/test/helpers"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("ReplicationGroupMessageID", func() {

	Describe("Testing the validity of the ReplicationGroupMessageID", func() {
		Context("with a valid RGMID", func() {
			It("should not return an error", func() {
				rgmid1, err := messaging.ReplicationGroupMessageIDOf("rmid1:0d77c-b0b2e66aece-00000000-00000001")
				Expect(rgmid1).ToNot(BeNil())
				Expect(err).To(BeNil())
			})

			Context("with an invalid RGMID", func() {
				It("should return an error", func() {
					rgmid1, err := messaging.ReplicationGroupMessageIDOf("rmid1:0-0-0")
					Expect(rgmid1).To(BeNil())
					Expect(err).To(HaveOccurred())
				})
			})

		})
	})

	Describe("Comparing ReplicationGroupMessageIDs", func() {
		When("the messages are issued to the same broker,", func() {
			Context("with different messages", func() {
				It("should return -1", func() {
					rmid1, _ := messaging.ReplicationGroupMessageIDOf("rmid1:3e2a1-a2c6f334fce-00000000-00000001")
					rmid2, _ := messaging.ReplicationGroupMessageIDOf("rmid1:3e2a1-a2c6f334fce-00003400-00206001")
					int1, err1 := rmid1.Compare(rmid2)
					int2, err2 := rmid2.Compare(rmid1)
					Expect(err1).ToNot(HaveOccurred())
					Expect(err2).ToNot(HaveOccurred())
					Expect(int1).To(Equal(-1))
					Expect(int2).To(Equal(1))
				})
			})

			Context("with identical messages", func() {
				It("should return 0", func() {
					rmid1, _ := messaging.ReplicationGroupMessageIDOf("rmid1:3e2a1-a2c6f334fce-12300000-00067801")
					rmid2, _ := messaging.ReplicationGroupMessageIDOf("rmid1:3e2a1-a2c6f334fce-12300000-00067801")
					int1, err := rmid1.Compare(rmid2)
					Expect(err).ToNot(HaveOccurred())
					Expect(int1).To(Equal(0))
				})
			})
		})
		When("the messages are not issued to the same broker and are thus not comparable", func() {
			Context("with the last 16 characters (the message IDs) being identical", func() {
				It("should return an error", func() {
					rmid1, _ := messaging.ReplicationGroupMessageIDOf("rmid1:fffff-a3c6fb34fce-12300000-00067801")
					rmid2, _ := messaging.ReplicationGroupMessageIDOf("rmid1:3e2a1-a2c6f334fce-12300000-00067801")
					_, err := rmid1.Compare(rmid2)
					Expect(err).To(HaveOccurred())
					helpers.ValidateError(err, &solace.IllegalArgumentError{})
				})
			})

			Context("with the last 16 characters (the message IDs) being different", func() {
				It("should return an error", func() {
					rmid1, _ := messaging.ReplicationGroupMessageIDOf("rmid1:fffff-a3c6fb34fce-732963af-00346801")
					rmid2, _ := messaging.ReplicationGroupMessageIDOf("rmid1:3e2a1-a2c6f334fce-12300000-00067801")
					_, err := rmid1.Compare(rmid2)
					Expect(err).To(HaveOccurred())
					helpers.ValidateError(err, &solace.IllegalArgumentError{})
				})
			})
		})
		When("the message to compare to is not a C backed rgmid", func() {
			It("should fail to compare", func() {
				rmid1, err := messaging.ReplicationGroupMessageIDOf("rmid1:fffff-a3c6fb34fce-732963af-00346801")
				Expect(err).ToNot(HaveOccurred())
				rmid2 := &dummyRmidStruct{}
				_, err = rmid1.Compare(rmid2)
				helpers.ValidateError(err, &solace.IllegalArgumentError{})
			})
		})
	})

	Describe("Get the ReplicationGroupMessageID as a string", func() {
		Context("with a valid RGMID", func() {
			It("should return the original string used during its creation", func() {
				rmid1, err := messaging.ReplicationGroupMessageIDOf("rmid1:fffff-a3c6fb34fce-732963af-00346801")
				str := rmid1.String()
				Expect(err).ToNot(HaveOccurred())
				Expect(str).To(Equal("rmid1:fffff-a3c6fb34fce-732963af-00346801"))
			})
		})
	})

})

type dummyRmidStruct struct{}

func (dummy *dummyRmidStruct) Compare(_ rgmid.ReplicationGroupMessageID) (int, error) {
	return 0, nil
}

func (dummy *dummyRmidStruct) String() string {
	return ""
}
