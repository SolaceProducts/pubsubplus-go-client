// pubsubplus-go-client
//
// Copyright 2021-2022 Solace Corporation. All rights reserved.
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

// Package sempclient contains generated SEMPv2 code
package sempclient

// create docker image based off of swaggerapi/swagger-codegen-cli-v3
//go:generate docker build -f Dockerfile -t solace-semp-swagger-codegen-cli:3.0.40 --build-arg SWAGGER_VER=3.0.40 $PWD
//go:generate docker run --rm -v "$PWD/spec:/schema" -v "$PWD:/output" solace-semp-swagger-codegen-cli:3.0.40 generate -l go -i /schema/spec_action.json -o /output/action --type-mappings boolean=*bool --additional-properties packageName=action
//go:generate docker run --rm -v "$PWD/spec:/schema" -v "$PWD:/output" solace-semp-swagger-codegen-cli:3.0.40 generate -l go -i /schema/spec_config.json -o /output/config --type-mappings boolean=*bool --additional-properties packageName=config
//go:generate docker run --rm -v "$PWD/spec:/schema" -v "$PWD:/output" solace-semp-swagger-codegen-cli:3.0.40 generate -l go -i /schema/spec_monitor.json -o /output/monitor --type-mappings boolean=*bool --additional-properties packageName=monitor

