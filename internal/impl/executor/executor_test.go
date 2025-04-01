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

package executor

import (
	"testing"
	"time"
)

func TestExecutorSubmitTask(t *testing.T) {
	executor := NewExecutor()
	taskChannel := make(chan int, 1)
	go executor.Run()
	submitted := executor.Submit(func() {
		taskChannel <- 1
	})
	if !submitted {
		t.Error("function Submit says task was not submitted")
	}
	select {
	case <-taskChannel:
		// success
	case <-time.After(100 * time.Millisecond):
		t.Error("task was never executed")
	}
	// wait for shutdown
	executor.Terminate()
}

func TestExecutorMultipleTerminate(t *testing.T) {
	executor := NewExecutor()
	taskChannel := make(chan int, 1)
	go executor.Run()
	submitted := executor.Submit(func() {
		taskChannel <- 1
	})
	if !submitted {
		t.Error("function Submit says task was not submitted")
	}
	select {
	case <-taskChannel:
		// success
	case <-time.After(100 * time.Millisecond):
		t.Error("task was never executed")
	}
	// wait for shutdown
	executor.Terminate()
	// try it again, make sure we don't panic or anythin
	executor.Terminate()
	// and see if we can await termination after all that
	executor.AwaitTermination()

	// Make sure we can't submit tasks magically after two calls to terminate
	taskBlocker := make(chan int, 1)
	submitted = executor.Submit(func() {
		taskBlocker <- 1
	})
	if submitted {
		t.Error("function Submit says the function was submitted")
	}
	select {
	case <-taskBlocker:
		t.Error("expected task to not be submitted, but it was executed")
	case <-time.After(100 * time.Millisecond):
		// success
	}

}

func TestExecutorResizing(t *testing.T) {
	executor := NewExecutor()
	taskCount := defaultSize + 1
	taskResultChannel := make(chan int, taskCount)
	taskBlocker := make(chan int, 1)
	go executor.Run()
	executor.Submit(func() {
		<-taskBlocker
	})
	for i := 0; i < taskCount; i++ {
		k := i
		executor.Submit(func() {
			taskResultChannel <- k
		})
	}
	// start task processing
	taskBlocker <- 1
	for i := 0; i < taskCount; i++ {
		v := <-taskResultChannel
		if v != i {
			t.Errorf("task ordering has changed! expected %d to be %d", v, i)
		}
	}
	executor.Terminate()
}

func TestExecutorSubmitAfterTermination(t *testing.T) {
	executor := NewExecutor()
	taskBlocker := make(chan int, 1)
	go executor.Run()
	executor.Terminate()
	submitted := executor.Submit(func() {
		taskBlocker <- 1
	})
	if submitted {
		t.Error("function Submit says the function was submitted")
	}
	select {
	case <-taskBlocker:
		t.Error("expected task to not be submitted, but it was executed")
	case <-time.After(100 * time.Millisecond):
		// success
	}
}

func TestExecutorTerminateWaitForTask(t *testing.T) {
	executor := NewExecutor()
	go executor.Run()
	blocker := make(chan struct{})
	blockerBlocking := make(chan struct{})
	executor.Submit(func() {
		close(blockerBlocking)
		<-blocker
	})
	select {
	case <-blockerBlocking:
		// success
	case <-time.After(100 * time.Millisecond):
		t.Error("timed out waiting for task to be called")
	}
	channelClosed := false
	executorTerminated := make(chan struct{})
	go func() {
		executor.AwaitTermination()
		if !channelClosed {
			t.Error("did not expect executor to be terminated prior to task returning")
		}
		close(executorTerminated)
	}()
	<-time.After(200 * time.Millisecond)
	channelClosed = true
	close(blocker)
	select {
	case <-executorTerminated:
		// success
	case <-time.After(100 * time.Millisecond):
		t.Error("timed out waiting for executor to shut down")
	}
}
