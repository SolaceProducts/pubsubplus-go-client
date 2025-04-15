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

package testcontext

import (
	"fmt"
	"os/exec"
)

func kinit(username, password string) error {
	cmd := exec.Command("kinit", username)
	stdinPipe, err := cmd.StdinPipe()
	if err != nil {
		return fmt.Errorf("encountered error getting stdin pipe: %w", err)
	}
	_, err = stdinPipe.Write([]byte(password + "\n"))
	if err != nil {
		return fmt.Errorf("encountered error writing to pipe: %w", err)
	}
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("encountered error waiting for command to complete: %w\n%s", err, string(output))
	}
	return nil
}
