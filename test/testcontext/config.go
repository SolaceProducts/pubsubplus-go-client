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
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"reflect"
	"strconv"
)

// TestConfig structure
type TestConfig struct {
	Messaging      *MessagingConfig      `json:"messaging,omitempty"`
	SEMP           *SEMPConfig           `json:"semp,omitempty"`
	ToxiProxy      *ToxiProxyConfig      `json:"toxiproxy,omitempty"`
	TestContainers *TestContainersConfig `json:"testcontainers,omitempty"`
	Kerberos       *KerberosConfig       `json:"kerberos,omitempty"`
	OAuth          *OAuthConfig          `json:"oauth,omitempty"`
}

// TestContainersConfig common context specific config should be placed here
type TestContainersConfig struct {
	BrokerHostname    string `json:"broker_hostname,omitempty" env:"PUBSUB_HOSTNAME"`
	ToxiProxyHostname string `json:"toxiproxy_hostname,omitempty" env:"TOXIPROXY_HOSTNAME"`
	BrokerTag         string `json:"broker_tag,omitempty" env:"PUBSUB_TAG"`
	BrokerRepo        string `json:"broker_repo,omitempty" env:"PUBSUB_REPO_BASE"`
	BrokerEdition     string `json:"broker_edition,omitempty" env:"PUBSUB_EDITION"`
	NetworkName       string `json:"network_name,omitempty" env:"PUBSUB_NETWORK_NAME"`
}

// OAuthConfig represents OAuth's config
type OAuthConfig struct {
	Hostname  string               `json:"hostname,omitempty" env:"PUBSUB_OAUTHSERVER_HOSTNAME"`
	Endpoints *OAuthEndpointConfig `json:"endpoints,omitempty"`
	Image     string               `env:"OAUTH_TEST_IMAGE"`
}

// OAuthEndpointConfig
type OAuthEndpointConfig struct {
	JwksEndpoint     string `json:"jwks,omitempty" env:"PUBSUB_OAUTHSERVER_JWKS_ENDPOINT"`
	UserInfoEndpoint string `json:"user_info,omitempty" env:"PUBSUB_OAUTHSERVER_USERINFO_ENDPOINT"`
}

// KerberosConfig represents Kerberos's config
type KerberosConfig struct {
	Image    string `json:"image" env:"KRB_TEST_IMAGE"`
	Hostname string `json:"hostname" env:"PUBSUB_KDC_HOSTNAME"`
	Domain   string `json:"domain" env:"PUBSUB_DOMAIN"`
	Username string `json:"username" env:"KUSER"`
	Password string `json:"password" env:"KPASSWORD"`
}

// ToEnvironment dumps the config to a map of environment variables
func (config *TestConfig) ToEnvironment() map[string]string {
	return toEnvironment(config)
}

func (config *TestConfig) String() string {
	str, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		return fmt.Sprintf("%T", config)
	}
	return string(str)
}

func toEnvironment(val interface{}) map[string]string {
	result := make(map[string]string)
	elem := reflect.ValueOf(val).Elem()
	for i := 0; i < elem.NumField(); i++ {
		field := elem.Field(i)
		if field.Kind() == reflect.Ptr {
			nestedVal := field.Interface()
			if nestedVal == nil {
				continue
			}
			nested := toEnvironment(nestedVal)
			for key, value := range nested {
				result[key] = value
			}
		} else {
			fieldTag := elem.Type().Field(i).Tag.Get("env")
			if fieldTag == "" {
				continue
			}
			switch field.Kind() {
			case reflect.String:
				val := field.String()
				if val != "" {
					result[fieldTag] = field.String()
				}
			case reflect.Int:
				result[fieldTag] = fmt.Sprintf("%d", field.Int())
			}
		}
	}
	return result
}

func (config *TestConfig) loadConfig(configFile string) error {
	// TODO allow additional config files to be layered through environment variables
	configJSON, err := ioutil.ReadFile(configFile)
	if err != nil {
		return err
	}
	json.Unmarshal(configJSON, &config)
	return nil
}

func (config *TestConfig) loadEnvironment() error {
	fmt.Println("Overriding JSON config with environment variables")
	_, err := loadFromEnvironment(config)
	if err != nil {
		return err
	}
	return nil
}

func loadFromEnvironment(val interface{}) (foundAny bool, retErr error) {
	foundAny = false
	elem := reflect.ValueOf(val).Elem()
	for i := 0; i < elem.NumField(); i++ {
		// fetch each field as value, as well as the type definition of the field (StructField)
		field := elem.Field(i)
		if field.Kind() == reflect.Ptr {
			nestedVal := field.Interface()
			if field.IsNil() {
				nestedVal = reflect.New(field.Type().Elem()).Interface()
			}
			found, err := loadFromEnvironment(nestedVal)
			if found {
				foundAny = true
				if field.IsNil() {
					field.Set(reflect.ValueOf(nestedVal))
				}
			}
			if err != nil {
				return false, err
			}
		} else {
			environmentVariable := elem.Type().Field(i).Tag.Get("env")
			switch field.Kind() {
			case reflect.Int:
				loaded, found, err := loadInt(environmentVariable)
				if found {
					if err != nil {
						return false, err
					}
					foundAny = true
					fmt.Printf("Found environment variable %s, setting %s to %d\n", environmentVariable, elem.Type().Field(i).Name, loaded)
					field.Set(reflect.ValueOf(loaded))
				}
			case reflect.String:
				loaded, found := loadString(environmentVariable)
				if found {
					foundAny = true
					fmt.Printf("Found environment variable %s, setting %s to %s\n", environmentVariable, elem.Type().Field(i).Name, loaded)
					field.Set(reflect.ValueOf(loaded))
				}
			}
		}
	}
	return foundAny, nil
}

func loadInt(key string) (value int, found bool, err error) {
	str, ok := os.LookupEnv(key)
	if ok {
		i, err := strconv.Atoi(str)
		if err != nil {
			return 0, true, fmt.Errorf("could not convert %s with value %s to integer: %s", key, str, err)
		}
		return i, true, nil
	}
	return 0, false, nil
}

func loadString(key string) (val string, found bool) {
	str, ok := os.LookupEnv(key)
	if ok {
		return str, true
	}
	return "", false
}
