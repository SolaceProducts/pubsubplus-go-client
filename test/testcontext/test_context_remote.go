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

//go:build remote
// +build remote

package testcontext

const remoteConfig = "./data/config/config_remote.json"

type remoteTestContext struct {
	testContextCommon
}

func getTestContext() testContext {
	context := remoteTestContext{}
	return &context
}

// SetupTestContext sets up new context
func (context *remoteTestContext) Setup() error {
	context.setupCommon(remoteConfig)

	err := context.waitForMessaging()
	if err != nil {
		return err
	}
	err = context.waitForSEMP()
	if err != nil {
		return err
	}
	context.semp = newSempV2(context.config.SEMP)
	context.semp.setup()

	if context.config.ToxiProxy != nil && context.config.ToxiProxy.Host != "" {
		err = context.waitForToxiProxy()
		if err != nil {
			return err
		}
		context.toxi = newToxiProxy(context.config.ToxiProxy)
		context.toxi.setup()
	}

	return nil
}

// TeardownTestContext impl of test context
func (context *remoteTestContext) Teardown() error {
	if context.toxi != nil {
		err := context.toxi.teardown()
		if err != nil {
			return err
		}
	}
	return nil
}
