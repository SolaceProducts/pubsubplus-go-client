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

// MessagingConfig represents the configuration of the broker for messaging
type MessagingConfig struct {
	Host          string `json:"host" env:"PUBSUB_HOST"`
	VPN           string `json:"vpn" env:"PUBSUB_VPN"`
	ClientProfile string `json:"client_profile" env:"PUBSUB_CLIENT_PROFILE"`
	ACLProfile    string `json:"acl_profile" env:"PUBSUB_ACL_PROFILE"`

	Authentication *struct {
		BasicUsername string `json:"basic_username" env:"PUBSUB_BASIC_USERNAME"`
		BasicPassword string `json:"basic_password" env:"PUBSUB_BASIC_PASSWORD"`
	} `json:"authentication"`

	MessagingPorts *struct {
		PlaintextPort   int `json:"plaintext" env:"PUBSUB_PLAINTEXT_PORT"`
		SecurePort      int `json:"secure" env:"PUBSUB_SECURE_PORT"`
		CompressedPort  int `json:"compressed" env:"PUBSUB_COMPRESSED_PORT"`
		HealthCheckPort int `json:"healthcheck" env:"PUBSUB_HEALTHCHECK_PORT"`
		WebPort         int `json:"web" env:"PUBSUB_WEB_PORT"`
		SecureWebPort   int `json:"secure_web" env:"PUBSUB_SECURE_WEB_PORT"`
	} `json:"ports"`
}
