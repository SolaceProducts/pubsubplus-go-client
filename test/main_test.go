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
	"bytes"
	"runtime"
	"sync"
	"testing"

	"solace.dev/go/messaging/pkg/solace/logging"
	"solace.dev/go/messaging/test/testcontext"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// Bootstrap for Ginkgo tests
func TestPubSubPlus(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Main Suite")
}

// Allocate the logger buffer
var loggerBuffer *threadSafeBuffer = &threadSafeBuffer{buffer: &bytes.Buffer{}}

// This flag can be set to true to always attach the logs to the test results.
// This could eventually be factored out into a build flag too if we want to
// have the property parameterized on run.
const alwaysAttachLogs = false

// Add a reporter after each individual test setting the logs data to the contents of the buffer
var _ = ReportAfterEach(func(sr SpecReport) {
	// We should only attach the debug logs when we fail, otherwise discard them
	// This is because the reports are ridiculously large when attached to every test
	if alwaysAttachLogs || sr.Failed() {
		AddReportEntry("logs", ReportEntryVisibilityFailureOrVerbose, loggerBuffer.String())
	}
	loggerBuffer.Reset()
})

var _ = BeforeSuite(func() {
	// Setup logging
	setLogLevel()
	logging.SetLogOutput(loggerBuffer)
	// Setup test context
	err := testcontext.Setup()
	Expect(err).ToNot(HaveOccurred())
})

var _ = AfterSuite(func() {
	// Teardown
	err := testcontext.Teardown()
	Expect(err).ToNot(HaveOccurred())
	runtime.GC()
	runtime.GC()
	runtime.GC()
})

// A quick and dirty threadsafe writer implementation that can be used for log captures on the suite
type threadSafeBuffer struct {
	sync.Mutex
	buffer *bytes.Buffer
}

func (buffer *threadSafeBuffer) Write(p []byte) (n int, err error) {
	buffer.Lock()
	defer buffer.Unlock()
	return buffer.buffer.Write(p)
}

func (buffer *threadSafeBuffer) String() string {
	buffer.Lock()
	defer buffer.Unlock()
	return buffer.buffer.String()
}

func (buffer *threadSafeBuffer) Reset() {
	buffer.Lock()
	defer buffer.Unlock()
	buffer.buffer.Reset()
}
