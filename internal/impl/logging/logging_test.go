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

package logging_test

// Unit tests for the contents of logging

import (
	"bytes"
	"fmt"
	"reflect"
	"runtime"
	"strings"
	"testing"

	"solace.dev/go/messaging/internal/impl/logging"
)

var logLevels []logging.LogLevel = []logging.LogLevel{
	logging.Critical,
	logging.Error,
	logging.Warning,
	logging.Info,
	logging.Debug,
}

type logLevelCheck struct {
	level     logging.LogLevel
	print     func(message string)
	check     func() bool
	indicator string
}

func getLogLevelChecks(logger logging.LogLevelLogger) []logLevelCheck {
	return []logLevelCheck{
		{logging.Critical, logger.Critical, logger.IsCriticalEnabled, "CRITICAL"},
		{logging.Error, logger.Error, logger.IsErrorEnabled, "ERROR"},
		{logging.Warning, logger.Warning, logger.IsWarningEnabled, "WARNING"},
		{logging.Info, logger.Info, logger.IsInfoEnabled, "INFO"},
		{logging.Debug, logger.Debug, logger.IsDebugEnabled, "DEBUG"},
	}
}

func TestSetLoggerLevel(t *testing.T) {
	logger := logging.Default
	logger.SetLevel(logging.Critical)
	if logger.GetLevel() != logging.Critical {
		t.Errorf("expected logging level to be %d, got %d", logging.Critical, logger.GetLevel())
	}
	logger.SetLevel(logging.Info)
	if logger.GetLevel() != logging.Info {
		t.Errorf("expected logging level to be %d, got %d", logging.Info, logger.GetLevel())
	}
}

func TestLogLevelChecks(t *testing.T) {
	logger := logging.Default
	logLevelChecks := getLogLevelChecks(logger)
	for _, level := range logLevels {
		logger.SetLevel(logging.LogLevel(level))
		for _, check := range logLevelChecks {
			result := check.check()
			if (level >= check.level) != (result) {
				t.Errorf("expected function %s to return %t when log level equals %d", runtime.FuncForPC(reflect.ValueOf(check.check).Pointer()).Name(), level >= check.level, level)
			}
		}
	}
}

type myTestStruct struct {
}

func TestLogLevelPrefix(t *testing.T) {
	logger := logging.Default
	output := &bytes.Buffer{}
	logger.SetOutput(output)
	logger.SetFlags(0)
	logger.SetLevel(logging.Info)
	myInstance := &myTestStruct{}
	prefixedLogger := logger.For(myInstance)
	prefixedLogger.Info("Hello World")
	result := output.String()
	expected := fmt.Sprintf("myTestStruct$%p", myInstance)
	if !strings.Contains(result, expected) {
		t.Errorf("expected string '%s' to contain '%s'", result, expected)
	}
}

func TestLogLevelNonPointerNoPrefix(t *testing.T) {
	logger := logging.Default
	output := &bytes.Buffer{}
	logger.SetOutput(output)
	logger.SetFlags(0)
	logger.SetLevel(logging.Info)
	myInstance := myTestStruct{}
	prefixedLogger := logger.For(myInstance)
	prefixedLogger.Info("Hello World")
	result := output.String()
	unexpected := "myTestStruct"
	if strings.Contains(result, unexpected) {
		t.Errorf("expected string '%s' to not contain '%s'", result, unexpected)
	}
}

func TestLogLevelOutputs(t *testing.T) {
	logger := logging.Default
	logLevelChecks := getLogLevelChecks(logger)
	buffer := &bytes.Buffer{}
	logger.SetOutput(buffer)
	logger.SetFlags(0)
	for _, level := range logLevels {
		logger.SetLevel(logging.LogLevel(level))
		for _, output := range logLevelChecks {
			buffer.Reset()
			output.print("Some Message")
			if (level >= output.level) != (buffer.String() != "") {
				t.Errorf("expected function %s to output? %t when log level equals %d", runtime.FuncForPC(reflect.ValueOf(output.print).Pointer()).Name(), level == output.level, level)
			}
		}
	}
}

func TestLogLevelIndicators(t *testing.T) {
	logger := logging.Default
	buffer := &bytes.Buffer{}
	logger.SetOutput(buffer)
	logger.SetFlags(0)
	for _, output := range getLogLevelChecks(logger) {
		logger.SetLevel(output.level)
		buffer.Reset()
		output.print("Some Message")
		if !strings.HasPrefix(buffer.String(), output.indicator) {
			t.Errorf("expected function %s to output message with prefix %s, got string '%s'", runtime.FuncForPC(reflect.ValueOf(output.print).Pointer()).Name(), output.indicator, buffer.String())
		}
	}
}

func TestChildLogLevelOutputs(t *testing.T) {
	logger := logging.Default.For(t)
	logLevelChecks := getLogLevelChecks(logger)
	buffer := &bytes.Buffer{}
	logger.SetOutput(buffer)
	logger.SetFlags(0)
	for _, level := range logLevels {
		logger.SetLevel(logging.LogLevel(level))
		for _, output := range logLevelChecks {
			buffer.Reset()
			output.print("Some Message")
			if (level >= output.level) != (buffer.String() != "") {
				t.Errorf("expected function %s to output? %t when log level equals %d", runtime.FuncForPC(reflect.ValueOf(output.print).Pointer()).Name(), level == output.level, level)
			}
		}
	}
}

func TestChildLogLevelIndicators(t *testing.T) {
	logger := logging.Default.For(t)
	buffer := &bytes.Buffer{}
	logger.SetOutput(buffer)
	logger.SetFlags(0)
	for _, output := range getLogLevelChecks(logger) {
		logger.SetLevel(output.level)
		buffer.Reset()
		output.print("Some Message")
		if !strings.HasPrefix(buffer.String(), output.indicator) {
			t.Errorf("expected function %s to output message with prefix %s, got string '%s'", runtime.FuncForPC(reflect.ValueOf(output.print).Pointer()).Name(), output.indicator, buffer.String())
		}
	}
}

func TestParentLoggerUpdateOfLogLevel(t *testing.T) {
	logger := logging.Default
	buffer := &bytes.Buffer{}
	logger.SetFlags(0)
	logger.SetLevel(logging.Debug)
	logger.SetOutput(buffer)
	testStruct1 := &myTestStruct{}
	testStruct2 := &myTestStruct{}
	logger1String := "Hello"
	logger2String := "Goodbye"
	logger1 := logger.For(testStruct1)
	logger2 := logger.For(testStruct2)
	buffer.Reset()
	logger1.Info(logger1String)
	if !strings.Contains(buffer.String(), logger1String) {
		t.Errorf("expected logger1 to output string %s, got '%s'", logger1String, buffer.String())
	}
	buffer.Reset()
	logger2.Info(logger2String)
	if !strings.Contains(buffer.String(), logger2String) {
		t.Errorf("expected logger1 to output string %s, got '%s'", logger2String, buffer.String())
	}
	logger.SetLevel(logging.Critical)
	buffer.Reset()
	logger1.Info(logger1String)
	if buffer.String() != "" {
		t.Errorf("expected logger1 to not output string when log level changed, got '%s'", buffer.String())
	}
	buffer.Reset()
	logger2.Info(logger2String)
	if buffer.String() != "" {
		t.Errorf("expected logger1 to not output string when log level changed, got '%s'", buffer.String())
	}
}

func TestParentLoggerUpdateOfOutput(t *testing.T) {
	logger := logging.Default
	buffer := &bytes.Buffer{}
	logger.SetFlags(0)
	logger.SetLevel(logging.Debug)
	logger.SetOutput(buffer)
	testStruct1 := &myTestStruct{}
	testStruct2 := &myTestStruct{}
	logger1String := "Hello"
	logger2String := "Goodbye"
	logger1 := logger.For(testStruct1)
	logger2 := logger.For(testStruct2)
	buffer.Reset()
	logger1.Info(logger1String)
	if !strings.Contains(buffer.String(), logger1String) {
		t.Errorf("expected logger1 to output string %s, got '%s'", logger1String, buffer.String())
	}
	buffer.Reset()
	logger2.Info(logger2String)
	if !strings.Contains(buffer.String(), logger2String) {
		t.Errorf("expected logger1 to output string %s, got '%s'", logger2String, buffer.String())
	}
	buffer2 := &bytes.Buffer{}
	logger.SetOutput(buffer2)

	buffer.Reset()
	logger1.Info(logger1String)
	if buffer.String() != "" {
		t.Errorf("expected logger1 to not output string when log level changed, got '%s'", buffer.String())
	}
	if !strings.Contains(buffer2.String(), logger1String) {
		t.Errorf("expected logger1 to output string %s, got '%s'", logger1String, buffer2.String())
	}
	buffer.Reset()
	logger2.Info(logger2String)
	if buffer.String() != "" {
		t.Errorf("expected logger1 to not output string when log level changed, got '%s'", buffer.String())
	}
	if !strings.Contains(buffer2.String(), logger2String) {
		t.Errorf("expected logger1 to output string %s, got '%s'", logger2String, buffer2.String())
	}
}
