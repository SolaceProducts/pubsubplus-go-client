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

//go:build dummy
// +build dummy

// dummy.go includes the relevant ccsmp library directories to suport go vendoring.
// It is excluded from all builds and exists as a workaround for https://github.com/golang/go/issues/26366.

package ccsmp

import (
	_ "solace.dev/go/messaging/internal/ccsmp/lib/darwin"
	_ "solace.dev/go/messaging/internal/ccsmp/lib/include/solclient"
	_ "solace.dev/go/messaging/internal/ccsmp/lib/linux_amd64"
	_ "solace.dev/go/messaging/internal/ccsmp/lib/linux_arm64"
)
