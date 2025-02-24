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

// Package logging allows for configuration of the API's logging levels.
package logging

import (
	"io"

	"solace.dev/go/messaging/internal/impl/core"
	"solace.dev/go/messaging/internal/impl/logging"
)

// LogLevel is used to configure the logging-level of the API. The different levels can
// include these type logs:
//
// - Critical/Error - These log levels indicates internal errors and should be a cause for investigation.
// Contact Solace for log events at this level.
//
// - Warning - Indicates an application error (for example, invalid parameters passed in or unsupported use of the APIs).
//
// - Info - Typically for unusual or informational occurrences that do not indicate an error, but might
// be unexpected or provide information useful for indicating a state.
//
// - Debug - Typically indicates state changes at a high-level, for example, connections/disconnections/reconnections
// These logs can indicate unusual occurrences that do not indicate any error, but they are unexpected
// and might need further investigation.
type LogLevel byte

const (
	// LogLevelCritical allows only critical logs.
	// Critical logs are only output if a catastrophic unrecoverable error occurs.
	LogLevelCritical LogLevel = LogLevel(logging.Critical)
	// LogLevelError allows Error and Critical logs.
	// Error logs are only output if unexpected behaviour is detected.
	LogLevelError LogLevel = LogLevel(logging.Error)
	// LogLevelWarning allows Warning, Error, and Critical logs.
	// Warning logs are output if an issue is encountered due to application behavior.
	LogLevelWarning LogLevel = LogLevel(logging.Warning)
	// LogLevelInfo allows Info, Warning, Error, and Critical logs.
	// Info logs are output when general API activities occur.
	LogLevelInfo LogLevel = LogLevel(logging.Info)
	// LogLevelDebug allows Debug, Info, Warning, Error, and Critical logs.
	// Debug logs are output when any API activity occurs. Note that
	// enabling Debug logs impacts the performance of the API.
	LogLevelDebug LogLevel = LogLevel(logging.Debug)
)

// SetLogLevel sets the global logging-level used for API logging.
func SetLogLevel(level LogLevel) {
	core.SetLogLevel(logging.LogLevel(level))
}

// SetLogOutput sets the global logging output redirection for API logging.
// The specified writer should be safe for concurrent writes.
func SetLogOutput(writer io.Writer) {
	logging.Default.SetOutput(writer)
}
