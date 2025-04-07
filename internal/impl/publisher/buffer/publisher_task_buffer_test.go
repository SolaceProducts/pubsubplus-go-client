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

package buffer

import (
	"testing"
	"time"

	"solace.dev/go/messaging/internal/impl/core"
)

func TestTaskBufferSubmitTask(t *testing.T) {
	testSharedBuffer := make(chan core.SendTask)
	taskBuffer := NewChannelBasedPublisherTaskBuffer(100, func() chan core.SendTask { return testSharedBuffer })
	go taskBuffer.Run()
	result := make(chan struct{})
	myTask := func(interrupt chan struct{}) {
		close(result)
	}
	taskBuffer.Submit(myTask)
	select {
	case fn := <-testSharedBuffer:
		fn()
	case <-time.After(100 * time.Millisecond):
		t.Error("timed out waiting for function to be pushed to the shared buffer")
	}
	select {
	case <-result:
		// success
	default:
		t.Error("function passed through task buffer did not execute")
	}
	graceful := taskBuffer.Terminate(time.NewTimer(100 * time.Millisecond))
	if !graceful {
		t.Error("expected task buffer to be shut down gracefully")
	}
}

func TestExecutorSubmitAfterTermination(t *testing.T) {
	testSharedBuffer := make(chan core.SendTask)
	taskBuffer := NewChannelBasedPublisherTaskBuffer(100, func() chan core.SendTask { return testSharedBuffer })
	go taskBuffer.Run()
	graceful := taskBuffer.Terminate(time.NewTimer(100 * time.Millisecond))
	if !graceful {
		t.Error("expected task buffer to be shut down gracefully")
	}
	myTask := func(interrupt chan struct{}) {
	}
	success := taskBuffer.Submit(myTask)
	if success {
		t.Error("expected task buffer to be shutdown, but it still accepted the task")
	}
	select {
	case <-testSharedBuffer:
		t.Error("did not expect to receive a task in the shared buffer")
	case <-time.After(100 * time.Millisecond):
		// success
	}
}

func TestExecutorSubmitTaskFullBuffer(t *testing.T) {
	testSharedBuffer := make(chan core.SendTask)
	taskBuffer := NewChannelBasedPublisherTaskBuffer(0, func() chan core.SendTask { return testSharedBuffer })
	go taskBuffer.Run()
	myTask := func(interrupt chan struct{}) {
	}
	success := taskBuffer.Submit(myTask)
	if success {
		t.Error("expected task buffer to be full, but it still accepted the task")
	}
	select {
	case <-testSharedBuffer:
		t.Error("did not expect to receive a task in the shared buffer")
	case <-time.After(100 * time.Millisecond):
		// success
	}
	graceful := taskBuffer.Terminate(time.NewTimer(100 * time.Millisecond))
	if !graceful {
		t.Error("expected task buffer to be shut down gracefully")
	}
}

func TestExecutorTerminateUngracefullyWithInFlightTasks(t *testing.T) {
	testSharedBuffer := make(chan core.SendTask)
	taskBuffer := NewChannelBasedPublisherTaskBuffer(100, func() chan core.SendTask { return testSharedBuffer })
	go taskBuffer.Run()
	done := make(chan struct{})
	myTask := func(interrupt chan struct{}) {
		<-interrupt
		close(done)
	}
	taskBuffer.Submit(myTask)
	select {
	case fn := <-testSharedBuffer:
		go fn()
	case <-time.After(100 * time.Millisecond):
		t.Error("timed out waiting for function to be pushed to the shared buffer")
	}
	graceful := taskBuffer.Terminate(time.NewTimer(100 * time.Millisecond))
	if graceful {
		t.Error("expected an ungraceful shutdown when task is blocking forever, but we shutdown gracefully")
	}
	select {
	case <-done:
		// success
	default:
		t.Error("terminate returned, but the task did not complete")
	}
}

func TestExecutorTerminationWhileWaitingForTask(t *testing.T) {
	testSharedBuffer := make(chan core.SendTask)
	taskBuffer := NewChannelBasedPublisherTaskBuffer(100, func() chan core.SendTask { return testSharedBuffer })
	go taskBuffer.Run()
	// Wait for the task buffer to start waiting for a task
	<-time.After(100 * time.Millisecond)
	graceful := taskBuffer.Terminate(time.NewTimer(100 * time.Millisecond))
	if !graceful {
		t.Error("expected task buffer to be shut down gracefully")
	}
}

func TestExecutorTerminationWhileWaitingForTaskSubmission(t *testing.T) {
	testSharedBuffer := make(chan core.SendTask)
	taskBuffer := NewChannelBasedPublisherTaskBuffer(100, func() chan core.SendTask { return testSharedBuffer })
	go taskBuffer.Run()
	called := false
	myTask := func(interrupt chan struct{}) {
		called = true
	}
	success := taskBuffer.Submit(myTask)
	if !success {
		t.Error("expected task to be submitted, but it was rejected")
	}
	select {
	case task := <-testSharedBuffer:
		task()
	case <-time.After(100 * time.Millisecond):
		t.Error("task was not submitted to the shared buffer")
	}
	// Wait for the task buffer to start waiting for a task
	<-time.After(100 * time.Millisecond)
	graceful := taskBuffer.Terminate(time.NewTimer(100 * time.Millisecond))
	if !graceful {
		t.Error("expected task buffer to be shut down gracefully")
	}
	if !called {
		t.Error("task was never called")
	}
}

func TestExecutorTerminationUngraceful(t *testing.T) {
	testSharedBuffer := make(chan core.SendTask)
	taskBuffer := NewChannelBasedPublisherTaskBuffer(100, func() chan core.SendTask { return testSharedBuffer })
	go taskBuffer.Run()
	myTask := func(interrupt chan struct{}) {
	}
	success := taskBuffer.Submit(myTask)
	if !success {
		t.Error("expected task to be submitted, but it was rejected")
	}
	graceful := taskBuffer.Terminate(time.NewTimer(100 * time.Millisecond))
	if graceful {
		t.Error("expected task buffer to be shut down ungracefully")
	}
}

func TestExecutorTerminateJoin(t *testing.T) {
	testSharedBuffer := make(chan core.SendTask)
	taskBuffer := NewChannelBasedPublisherTaskBuffer(100, func() chan core.SendTask { return testSharedBuffer })
	go taskBuffer.Run()
	blocker := make(chan struct{})
	myTask := func(interrupt chan struct{}) {
		<-blocker
	}
	success := taskBuffer.Submit(myTask)
	go func() {
		(<-testSharedBuffer)()
	}()
	if !success {
		t.Error("expected task to be submitted, but it was rejected")
	}
	unblocked := false
	terminateSuccess := make(chan struct{})
	go func() {
		graceful := taskBuffer.Terminate(time.NewTimer(10 * time.Millisecond))
		if graceful {
			t.Error("expected task buffer to be shut down ungracefully")
		}
		if !unblocked {
			t.Error("expected terminate to block until threads were joined")
		}
		close(terminateSuccess)
	}()
	<-time.After(100 * time.Millisecond)
	unblocked = true
	close(blocker)
	select {
	case <-terminateSuccess:
		// success
	case <-time.After(100 * time.Millisecond):
		t.Error("timed out waiting for task to complete")
	}
}

func TestExecutorTerminateNow(t *testing.T) {
	testSharedBuffer := make(chan core.SendTask)
	taskBuffer := NewChannelBasedPublisherTaskBuffer(100, func() chan core.SendTask { return testSharedBuffer })
	go taskBuffer.Run()
	blocker := make(chan struct{})
	myTask := func(interrupt chan struct{}) {
		<-blocker
	}
	success := taskBuffer.Submit(myTask)
	go func() {
		(<-testSharedBuffer)()
	}()
	if !success {
		t.Error("expected task to be submitted, but it was rejected")
	}
	unblocked := false
	terminateSuccess := make(chan struct{})
	go func() {
		taskBuffer.TerminateNow()
		if unblocked {
			t.Error("expected terminate now to return immediately")
		}
		close(terminateSuccess)
	}()
	<-time.After(100 * time.Millisecond)
	unblocked = true
	close(blocker)
	select {
	case <-terminateSuccess:
		// success
	case <-time.After(100 * time.Millisecond):
		t.Error("timed out waiting for task to complete")
	}
}
