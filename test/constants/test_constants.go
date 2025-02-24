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

// Package constants is defined below
package constants

// ValidFixturesPath constant
const ValidFixturesPath = "./data/fixtures/"

// InvalidFixturesPath constant
const InvalidFixturesPath = "./data/invalid_fixtures/"

// ValidClientCertificatePEM to install to the server for valid client authentication
const ValidClientCertificatePEM = ValidFixturesPath + "public_root_ca.crt"

// InvalidClientCertificatePEM constant
const InvalidClientCertificatePEM = InvalidFixturesPath + "unused_root_ca.crt"

// ValidClientCertificateFile constant
const ValidClientCertificateFile = ValidFixturesPath + "api-client.crt"

// ExpiredClientCertificateFile constant
const ExpiredClientCertificateFile = InvalidFixturesPath + "api-clientExpired.crt"

// InvalidClientCertificateFile constant
const InvalidClientCertificateFile = InvalidFixturesPath + "api-clientInvalidKey.pem"

// ValidClientKeyFile constant
const ValidClientKeyFile = ValidFixturesPath + "api-client.key"

// InvalidClientKeyFile constant
const InvalidClientKeyFile = InvalidFixturesPath + "unused.key"

// ValidCertificateKeyPassword constant
const ValidCertificateKeyPassword = "changeme"

// InvalidCertificateKeyPassword constant
const InvalidCertificateKeyPassword = "thisisnotthepasswordyouarelookingfor"

// ServerCertificatePassphrase constant
const ServerCertificatePassphrase = "No Password"

// ValidServerCertificate constant
const ValidServerCertificate = ValidFixturesPath + "api-server.pem"

// BadServernameServerCertificate constant
const BadServernameServerCertificate = InvalidFixturesPath + "api-badserver.pem"

// ExpiredServerCertificate constant
const ExpiredServerCertificate = InvalidFixturesPath + "api-serverExpired.pem"
