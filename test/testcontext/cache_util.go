// pubsubplus-go-client
//
// Copyright 2021-2024 Solace Corporation. All rights reserved.
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
	"time"
	"net/url"
    "fmt"

	"solace.dev/go/messaging/test/sempclient/config"
)

func ConfigureDistributedCacheIfNotPresent(semp SEMPv2, msgVpnName string, distributedCache DistributedCacheConfig) error {
        _, httpResponse, err := semp.Monitor().DistributedCacheApi.GetMsgVpnDistributedCache(semp.MonitorCtx(), msgVpnName, distributedCache.Name, nil)
                if err != nil {
                        if httpResponse.StatusCode == 400 {
                                /* Cache missing so create it, otherwise we assume configuration has already occurred. */
                                _, _, err = semp.Config().DistributedCacheApi.CreateMsgVpnDistributedCache(semp.ConfigCtx(),
                                config.MsgVpnDistributedCache{
                                        CacheName: distributedCache.Name,
                                        MsgVpnName: msgVpnName,
                                        Enabled: True,
                                },
                                msgVpnName,
                                nil)
                                if err != nil {
                                        return err
                                }
                        } else {
                                return err
                        }
                }
        return err
}

func ConfigureCacheClusterIfNotPresent(semp SEMPv2, msgVpnName string, distributedCache DistributedCacheConfig, cacheCluster CacheClusterConfig) error {
        _, httpResponse, err := semp.Monitor().DistributedCacheApi.GetMsgVpnDistributedCacheCluster(semp.MonitorCtx(), msgVpnName, distributedCache.Name, cacheCluster.Name, nil)
                        if err != nil {
                                if httpResponse.StatusCode == 400 {
                                        _, _, err = semp.Config().DistributedCacheApi.CreateMsgVpnDistributedCacheCluster(semp.ConfigCtx(),
                                        config.MsgVpnDistributedCacheCluster{
                                                CacheName: distributedCache.Name,
                                                ClusterName: cacheCluster.Name,
                                                MsgVpnName: msgVpnName,
                                                Enabled: True,
                                                MaxMsgsPerTopic: int64(cacheCluster.Properties.MaxMsgsPerTopic),
                                                MaxMemory: int64(cacheCluster.Properties.MaxMemory),
                                                MaxTopicCount: int64(cacheCluster.Properties.MaxTopicCount),
                                        },
                                        msgVpnName,
                                        distributedCache.Name,
                                        nil)
                                        if err != nil {
                                                return err
                                        }
                                } else {
                                        return err
                                }
                        }
        return err
}

func ConfigureCacheInstanceIfNotPresent(semp SEMPv2, msgVpnName string, distributedCache DistributedCacheConfig, cacheCluster CacheClusterConfig, cacheInstance CacheInstanceConfig) error {
        _, httpResponse, err := semp.Monitor().DistributedCacheApi.GetMsgVpnDistributedCacheClusterInstance(semp.MonitorCtx(), msgVpnName, distributedCache.Name, cacheCluster.Name, cacheInstance.Name, nil)
                                if err != nil {
                                        if httpResponse.StatusCode == 400 {
                                                _, _, err = semp.Config().DistributedCacheApi.CreateMsgVpnDistributedCacheClusterInstance(semp.ConfigCtx(),
                                                config.MsgVpnDistributedCacheClusterInstance{
                                                        Enabled: True,
                                                        AutoStartEnabled: True,
                                                        MsgVpnName: msgVpnName,
                                                        CacheName: distributedCache.Name,
                                                        ClusterName: cacheCluster.Name,
                                                        InstanceName: cacheInstance.Name,
                                                        StopOnLostMsgEnabled: True,
                                                },
                                                msgVpnName,
                                                distributedCache.Name,
                                                cacheCluster.Name,
                                                nil)
                                                if err != nil {
                                                        return err
                                                }
                                        } else {
                                                return err
                                        }
                                }
        return err
}

func ConfigureCacheTopicIfNotPresent(semp SEMPv2, msgVpnName string, distributedCache DistributedCacheConfig, cacheCluster CacheClusterConfig, cacheTopic string) error {
        cacheTopic = url.QueryEscape(cacheTopic)
        inst, httpResponse, err := semp.Monitor().DistributedCacheApi.GetMsgVpnDistributedCacheClusterTopics(semp.MonitorCtx(), msgVpnName, distributedCache.Name, cacheCluster.Name, nil)
        for _, topic_group := range inst.Data {
                if topic_group.Topic == cacheTopic {
                        return nil
                }
        }
                        if httpResponse.StatusCode == 400 {
                                _, _, err = semp.Config().DistributedCacheApi.CreateMsgVpnDistributedCacheClusterTopic(semp.ConfigCtx(),
                                config.MsgVpnDistributedCacheClusterTopic{
                                        CacheName: distributedCache.Name,
                                        ClusterName: cacheCluster.Name,
                                        MsgVpnName: msgVpnName,
                                        Topic: cacheTopic,
                                },
                                msgVpnName,
                                distributedCache.Name,
                                cacheCluster.Name,
                                nil)
                                if err != nil {
                                        return err
                                }
                        } else {
                                return err
                        }
        return err
}

// WaitForCacheState polls the given cache instance operational state until it matches the expected state or until the timer expires.
func waitForCacheState(semp SEMPv2, msgVpnName string, distributedCacheName string, cacheClusterName string, cacheClusterInstanceName string, targetState string, timeout time.Duration) error {
    found := false

	pollInterval := 500 * time.Millisecond
	timeoutChannel := time.After(timeout)

    for !found {
        cacheInstance, httpResponse, err := semp.Monitor().DistributedCacheApi.GetMsgVpnDistributedCacheClusterInstance(semp.MonitorCtx(),
                                                                               msgVpnName,
                                                                               distributedCacheName,
                                                                               cacheClusterName,
                                                                               cacheClusterInstanceName,
                                                                               nil)
 
                if httpResponse.StatusCode == 200 {
                        if cacheInstance.Data.State ==  targetState {
                                return nil
                        }
                }
        select {
                
        case <- timeoutChannel:
            // break, logging?
            found = true
            if err == nil {
                if httpResponse.StatusCode != 200 {
                        return fmt.Errorf("cache did not setup properly, got response code %d", httpResponse.StatusCode)
                }
                return fmt.Errorf("cache did not setup properly, got SEMPv2 result %s", httpResponse.Body)
            }
            return err

        case <- time.After(pollInterval):
                continue
        }
    }
    return fmt.Errorf("failed to retrieve cache instance update")
 }


// CacheOperationalState enumerates the different possible states of a cache instance.
// It would be great if this was defined as an accessible enum in the generated SEMPv2, but it is not. At least having
//an enum gives a common point of reference that can be updated as needed.
type CacheOperationalState string
const (
    // CacheOperationalStateInvalid: the cache intance state is invalid.
    CacheOperationalStateInvalid CacheOperationalState = "invalid"
    // CacheOperationalStateDown: the cache instance is operationally down.
    CacheOperationalStateDown CacheOperationalState = "down"
    // CacheOperationalStateStopped: the cache instance has stopped processing cache requests.
    CacheOperationalStateStopped CacheOperationalState = "stopped"
    // CacheOperationalStateStoppedLostMsg: the cache instance has stopped due to a lost message.
    CacheOperationalStateStoppedLostMsg CacheOperationalState = "stopped-lost-msg"
    // CacheOperationalStateRegister: the cache instance is registering with the broker.
    CacheOperationalStateRegister CacheOperationalState = "register"
    // CacheOperationalStateConfigSync: the cache instance is synchronizing its configuration with the broker.
    CacheOperationalStateConfigSync CacheOperationalState = "config-sync"
    // CacheOperationalStateConfigSync: the cache instance is synchronizing its messages with the Cache Cluster.
    CacheOperationalStateClusterSync CacheOperationalState = "cluster-sync"
    // CacheOperationalStateUp: the cache instance is operationally up.
    CacheOperationalStateUp CacheOperationalState = "up"
    // CacheOperationalStateBackup: the cache instance is backing up its messages to disk.
    CacheOperationalStateBackup CacheOperationalState = "backup"
    // CacheOperationalStateRestore: the cache instance is restoring its messages from disk.
    CacheOperationalStateRestore CacheOperationalState = "restore"
    // CacheOperationalStateNotAvailable: the cache instance state is not available.
    CacheOperationalStateNotAvailable CacheOperationalState = "not-available"
)

//func (ctx *testContainersTestContext) initCache() error {
func (ctx *testContextCommon) initCache() error {
        var msgVpnName = ctx.Cache().Vpn
        err := CreateMessageVpnIfNotPresent(ctx.SEMPv2(), GetDefaultCacheMessageVpnConfig(ctx.Cache().Vpn))
        if err != nil {
                return err
        }
        err = UpdateMessageVpn(ctx.SEMPv2(),
                GetDefaultCacheMessageVpnClientProfileConfig(msgVpnName),
                GetDefaultCacheMessageVpnClientUsernameConfig(msgVpnName),
                GetMessageVpnOperationalStateConfig(msgVpnName, true))
        if err != nil {
                return err
        }

        for _, distributedCache := range ctx.Cache().DistributedCaches {
                err = ConfigureDistributedCacheIfNotPresent(ctx.SEMPv2(), msgVpnName, distributedCache)
                if err != nil {
                        return err
                }
                for _, cacheCluster := range distributedCache.CacheClusters {
                        err = ConfigureCacheClusterIfNotPresent(ctx.SEMPv2(), msgVpnName, distributedCache, cacheCluster)
                        if err != nil {
                                return err
                        }
                        for _, cacheTopic := range cacheCluster.Topics {
                                err = ConfigureCacheTopicIfNotPresent(ctx.SEMPv2(), msgVpnName, distributedCache, cacheCluster, cacheTopic)
                                if err != nil {
                                        return err
                                }
                        }
                        for _, cacheInstance := range cacheCluster.CacheInstances {
                                err = ConfigureCacheInstanceIfNotPresent(ctx.SEMPv2(), msgVpnName, distributedCache, cacheCluster, cacheInstance)
                                if err != nil {
                                        return err
                                }
                        }
                }
        }

    return err
}

// waitForOperationalCache checks that each cache instance is in the expected operational state. This has only been
// tested when checking for an operational state of CacheOperationalStateUp, where it is necessary that if all cache
// instances are up, then all distributed caches and cache clusters are also up. This relationship may not hold for
// other operational states. The usage of this method for checking other operational states may require refactoring.
func (ctx *testContextCommon) waitForOperationalCache(timeout time.Duration, state CacheOperationalState) error {
        for _, distributedCache := range ctx.Cache().DistributedCaches {
                for _, cacheCluster := range distributedCache.CacheClusters {
                        for _, cacheInstance := range cacheCluster.CacheInstances {
                                err := waitForCacheState(
                                        ctx.SEMPv2(),
                                        ctx.Cache().Vpn,
                                        distributedCache.Name,
                                        cacheCluster.Name,
                                        cacheInstance.Name,
                                        string(state),
                                        timeout)
                                if err != nil {
                                        return err
                                }
                        }
                }
        }
        return nil
}

func (ctx *testContextCommon) setupCache() error {
        err := ctx.initCache()
        if err != nil {
            return err
        }
        err = ctx.waitForOperationalCache(time.Duration(30 * time.Second), CacheOperationalStateUp)
        return err
}
