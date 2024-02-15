// pubsubplus-go-client
//
// Copyright 2024 Solace Corporation. All rights reserved.
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

package publisher

import (
	"encoding/json"
	//    "fmt"
	"testing"
	//    "time"
	//    "unsafe"

	//    "solace.dev/go/messaging/internal/ccsmp"

	//    "solace.dev/go/messaging/internal/impl/core"
	//    "solace.dev/go/messaging/internal/impl/message"

	//    "solace.dev/go/messaging/internal/impl/constants"

	//    "solace.dev/go/messaging/internal/impl/executor"
	//    "solace.dev/go/messaging/internal/impl/publisher/buffer"
	"solace.dev/go/messaging/pkg/solace"
	"solace.dev/go/messaging/pkg/solace/config"
	// "solace.dev/go/messaging/pkg/solace/resource"
	// "solace.dev/go/messaging/pkg/solace/subcode"
)

func TestRequestReplyMessagePublisherBuilderWithValidBackpressure(t *testing.T) {
	backpressureConfigurations := []func(builder solace.RequestReplyMessagePublisherBuilder) (solace.RequestReplyMessagePublisherBuilder, backpressureConfiguration, int){
		func(builder solace.RequestReplyMessagePublisherBuilder) (solace.RequestReplyMessagePublisherBuilder, backpressureConfiguration, int) {
			return builder.OnBackPressureReject(0), backpressureConfigurationDirect, 0

		},
		func(builder solace.RequestReplyMessagePublisherBuilder) (solace.RequestReplyMessagePublisherBuilder, backpressureConfiguration, int) {
			bufferSize := 1
			return builder.OnBackPressureReject(uint(bufferSize)), backpressureConfigurationReject, bufferSize
		},
		func(builder solace.RequestReplyMessagePublisherBuilder) (solace.RequestReplyMessagePublisherBuilder, backpressureConfiguration, int) {
			bufferSize := 1
			return builder.OnBackPressureWait(uint(bufferSize)), backpressureConfigurationWait, bufferSize
		},
	}
	shared := &mockInternalPublisher{}
	for _, config := range backpressureConfigurations {
		builder, backpressureConfig, capacity := config(NewRequestReplyMessagePublisherBuilderImpl(shared))
		publisher, err := builder.Build()
		if err != nil {
			t.Error(err)
		}
		if publisher == nil {
			t.Error("expected publisher to not be nil")
		}
		publisherImpl := publisher.(*requestReplyMessagePublisherImpl)
		if publisherImpl.backpressureConfiguration != backpressureConfig {
			t.Errorf("expected backpressure config to equal %d, was %d", backpressureConfig, publisherImpl.backpressureConfiguration)
		}
		if cap(publisherImpl.buffer) != capacity {
			t.Errorf("expected backpressure capacity to equal %d, was %d", capacity, cap(publisherImpl.buffer))
		}
	}
}

func TestRequestReplyMessagePublisherBuilderWithInvalidBackpressureWait(t *testing.T) {
	publisher, err := NewRequestReplyMessagePublisherBuilderImpl(&mockInternalPublisher{}).OnBackPressureWait(0).Build()
	// we should get an error saying that buffer must be > 0 for wait
	if err == nil {
		t.Error("expected error to not be nil")
	}
	if publisher != nil {
		t.Error("expected publisher to be nil")
	}
}

func TestRequestReplyMessagePublisherBuilderWithCustomPropertiesStructFromJSON(t *testing.T) {
	jsonData := `{"solace":{"messaging":{"publisher":{"back-pressure":{"strategy":"BUFFER_WAIT_WHEN_FULL","buffer-capacity": 100,"buffer-wait-timeout": 1000}}}}}`
	baselineProperties := make(config.PublisherPropertyMap)
	err := json.Unmarshal([]byte(jsonData), &baselineProperties)
	if err != nil {
		t.Error(err)
	}
	publisher, err := NewRequestReplyMessagePublisherBuilderImpl(&mockInternalPublisher{}).FromConfigurationProvider(baselineProperties).Build()
	if publisher == nil {
		t.Error("expected publisher to not be nil")
	}
	if err != nil {
		t.Error(err)
	}
	publisherImpl := publisher.(*requestReplyMessagePublisherImpl)
	if publisherImpl.backpressureConfiguration != backpressureConfigurationWait {
		t.Errorf("expected backpressure config to equal %d, was %d", backpressureConfigurationWait, publisherImpl.backpressureConfiguration)
	}
	if cap(publisherImpl.buffer) != 100 {
		t.Errorf("expected backpressure capacity to equal %d, was %d", 100, cap(publisherImpl.buffer))
	}
}

func TestRequestReplyMessagePublisherBuilderWithInvalidCustomPropertiesMapNegativeBufferCapacity(t *testing.T) {
	baselineProperties := config.PublisherPropertyMap{
		config.PublisherPropertyBackPressureBufferCapacity: -1,
		config.PublisherPropertyBackPressureStrategy:       config.PublisherPropertyBackPressureStrategyBufferRejectWhenFull,
	}
	publisher, err := NewRequestReplyMessagePublisherBuilderImpl(&mockInternalPublisher{}).FromConfigurationProvider(baselineProperties).Build()
	if publisher != nil {
		t.Error("expected publisher to be nil")
	}
	if err == nil {
		t.Error("expected error when backpressure capacity is negative")
	}
}

func TestRequestReplyMessagePublisherBuilderWithInvalidCustomPropertiesMapWrongTypeBufferCapacity(t *testing.T) {
	baselineProperties := config.PublisherPropertyMap{
		config.PublisherPropertyBackPressureBufferCapacity: "hello",
		config.PublisherPropertyBackPressureStrategy:       config.PublisherPropertyBackPressureStrategyBufferWaitWhenFull,
	}
	publisher, err := NewRequestReplyMessagePublisherBuilderImpl(&mockInternalPublisher{}).FromConfigurationProvider(baselineProperties).Build()
	if publisher != nil {
		t.Error("expected publisher to be nil")
	}
	if err == nil {
		t.Error("expected error when backpressure capacity is a string")
	}
}

func TestRequestReplyMessagePublisherBuilderWithInvalidCustomPropertiesMapWrongTypeStrategy(t *testing.T) {
	baselineProperties := config.PublisherPropertyMap{
		config.PublisherPropertyBackPressureBufferCapacity: 1,
		config.PublisherPropertyBackPressureStrategy:       23,
	}
	publisher, err := NewRequestReplyMessagePublisherBuilderImpl(&mockInternalPublisher{}).FromConfigurationProvider(baselineProperties).Build()
	if publisher != nil {
		t.Error("expected publisher to be nil")
	}
	if err == nil {
		t.Error("expected error when backpressure strategy is an integer")
	}
}

func TestRequestReplyMessagePublisherBuilderWithInvalidCustomPropertiesMapWrongStrategy(t *testing.T) {
	baselineProperties := config.PublisherPropertyMap{
		config.PublisherPropertyBackPressureBufferCapacity: 1,
		config.PublisherPropertyBackPressureStrategy:       "hello world",
	}
	publisher, err := NewRequestReplyMessagePublisherBuilderImpl(&mockInternalPublisher{}).FromConfigurationProvider(baselineProperties).Build()
	if publisher != nil {
		t.Error("expected publisher to be nil")
	}
	if err == nil {
		t.Error("expected error when backpressure strategy is an integer")
	}
}
