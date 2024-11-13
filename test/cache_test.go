// pubsubplus-go-client
//
// Copyright 2021-2024 Solace Corporation. All rights reserved.
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
	"solace.dev/go/messaging/test/testcontext"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)
func CheckCache() {
    if !testcontext.CacheEnabled() {
        Skip("The infrastructure required for running cache tests is not available, skipping this test since it requires a cache.")
    }
}

func CheckCacheProxy() {
    if !testcontext.CacheProxyEnabled() {
        Skip("The infrastructure required for running cache proxy tests is not available, skipping this test since it requires a cache proxy.")
    }
}

var _ = Describe("Cache Strategy", func() {
        /* The following tests are just demos of how to check for infrastructure.
        They should be removed once proper integration tests are added. */
    Describe("Demo isolated Cache functionality", func() {
        BeforeEach(func () {
            CheckCache() // skips test with message if cache image is not available
        })
        It("should run this test", func() {
                Expect(1 == 1).To(BeTrue())
        })
    })

    Describe("Demo isolated Cache Proxy functionality", func() {
        BeforeEach(func () {
                CheckCacheProxy() // skips test with message if cache proxy image is not available
        })
        It("should run this test", func() {
                Expect(1 == 1).To(BeTrue())
        })
    })
    Describe("Demo Cache & Cache Proxy functionality", func() {
        BeforeEach(func () {
                CheckCache()
                CheckCacheProxy()
        })
        It("should run this test", func() {
                Expect(1 == 1).To(BeTrue())
        })
    })
})
