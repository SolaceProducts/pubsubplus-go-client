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

//go:build !remote
// +build !remote

package testcontext

import (
	"fmt"
	"os"
	"os/exec"
	"path"
	"strings"
    "time"

	"github.com/google/uuid"
	"github.com/testcontainers/testcontainers-go"
	"solace.dev/go/messaging/test/sempclient/config"
)

// the default config file relative to the location the tests are run from
// this assumes that the tests are run from the root of the `test` directory
const testContainersConfig = "./data/config/config_testcontainers.json"
const defaultComposeFilePath = "data/compose/docker-compose.yml"
const kerberosComposeFilePath = "data/compose/docker-compose.krb.yml"
const oauthComposeFilePath = "data/compose/docker-compose.oauth.yml"
const cacheComposeFilePath = "data/compose/docker-compose.cache.yml"
const cacheProxyComposeFilePath = "data/compose/docker-compose.cacheproxy.yml"
const testFolderVarName = "TEST_FOLDER"
const testFolderVarDefaultValue = ".."

type testContainersTestContext struct {
	testContextCommon
	compose *testcontainers.LocalDockerCompose
}

func getTestContext() testContext {
	context := testContainersTestContext{}
	return &context
}

// SetupTestContext sets up new context
func (context *testContainersTestContext) Setup() error {
	fmt.Println()
	fmt.Println("-- TESTCONTAINERS SETUP --")

    testFolderVar := os.Getenv(testFolderVarName)
    if testFolderVar == "" {
        os.Setenv(testFolderVarName, testFolderVarDefaultValue)
    }

	context.setupCommon(testContainersConfig)

	composeFilePaths := []string{defaultComposeFilePath}
	if context.config.Kerberos.Image != "" {
		context.kerberosEnabled = true
		composeFilePaths = append(composeFilePaths, kerberosComposeFilePath)
	}
	if context.config.OAuth.Image != "" {
		composeFilePaths = append(composeFilePaths, oauthComposeFilePath)
	} else {
		context.config.OAuth.Hostname = ""
	}
    if context.config.Cache.Image != "" {
        context.cacheEnabled = true
        composeFilePaths = append(composeFilePaths, cacheComposeFilePath)
    }
    if context.config.CacheProxy.Image != "" {
        context.cacheProxyEnabled = true
        composeFilePaths = append(composeFilePaths, cacheProxyComposeFilePath)
    }
	identifier := strings.ToLower(uuid.New().String())
	compose := testcontainers.NewLocalDockerCompose(composeFilePaths, identifier)

	context.compose = compose

	context.semp = newSempV2(context.config.SEMP)
	context.toxi = newToxiProxy(context.config.ToxiProxy)

	fmt.Println("Starting dockerized broker via docker-compose")
    fmt.Printf("CacheDebug: compose file list is:\n%s\n", context.compose.ComposeFilePaths)

	execError := context.compose.
		WithEnv(context.config.ToEnvironment()).
		WithCommand([]string{"up", "-d"}).
		Invoke()

	err := execError.Error
	if err != nil {
		return err
	}

	fmt.Println("Docker-compose command succeeded")
	fmt.Println("Waiting for SEMPv2 to be up")

	err = context.waitForSEMP()
	if err != nil {
		return err
	}

	fmt.Println("Waiting for Messaging to be up")
	err = context.waitForMessaging()
	if err != nil {
		return err
	}

	err = context.semp.setup()
	if err != nil {
		return err
	}

	fmt.Println("Waiting for ToxiProxy to be up")
	err = context.waitForToxiProxy()
	if err != nil {
		return err
	}

	fmt.Println("Setting up ToxiProxy proxies")
	err = context.toxi.setup()
	if err != nil {
		return err
	}

	if context.kerberosEnabled {
		fmt.Println("Setting up Kerberos")
		err = context.setupKerberos()
		if err != nil {
			return err
		}
	}

	fmt.Println("Waiting for MsgVPN " + context.config.Messaging.VPN + " to be up")
	err = context.waitForVPNState("up")
	if err != nil {
		return err
	}

    if context.cacheEnabled {
        fmt.Println("Waiting for Cache setup")
        err = context.setupCache()
        if err != nil {
            context.cacheEnabled = false
            return err
        }
    }
    if context.cacheProxyEnabled {
        fmt.Println("Cache Proxy has been enabled, but currently no verification of cache proxy is available.")
    }

	fmt.Println("-- TESTCONTAINERS SETUP COMPLETE --")
	fmt.Println()
	return nil
}

// TeardownTestContext impl of test context
func (context *testContainersTestContext) Teardown() error {
	fmt.Println()
	fmt.Println("-- TESTCONTAINERS TEARDOWN --")
	pubsubHostname := context.config.TestContainers.BrokerHostname
	err := context.toxi.teardown()
	if err != nil {
		fmt.Println("Encountered error tearing down toxiproxy: " + err.Error())
	}
	rc, output, err := context.dockerLogs(pubsubHostname)
	if err == nil {
		fmt.Println("Broker logs for " + pubsubHostname + ":")
		fmt.Println(output)
	} else {
		fmt.Println("Encountered error getting docker logs from " + pubsubHostname + ": rc=" + string(rc) + " err:" + err.Error())
	}
	wd, err := os.Getwd()
	if err != nil {
		fmt.Println("Encountered error getting working directory for " + pubsubHostname + " diagnostics err:" + err.Error())
	}

	err = context.gatherBrokerDiagnostics(path.Join(wd, "diagnostics.tgz"))
	if err != nil {
		fmt.Println("Encountered error getting " + pubsubHostname + " diagnostics err:" + err.Error())
	}

	composeResult := context.compose.Down()
	if composeResult.Error != nil {
		err = composeResult.Error
		fmt.Println("Encountered error tearing down docker compose: " + composeResult.Error.Error())
	}
	fmt.Println("-- TESTCONTAINERS TEARDOWN COMPLETE --")
	return err
}

func (context *testContainersTestContext) gatherBrokerDiagnostics(destinationPath string) error {
	fmt.Println()
	pubsubHostname := context.config.TestContainers.BrokerHostname
	// gather all important infomation and logs from pubsubHostname container
	fmt.Println("Run gather-diagnostics for " + pubsubHostname + "...")
	resp, output, err := context.dockerExec(pubsubHostname, []string{"/bin/bash", "-l", "-c", "\"gather-diagnostics\""})
	if err != nil {
		return err
	}
	fmt.Println(output)
	if resp != 0 {
		return fmt.Errorf("failed to gather %s diagnostics", pubsubHostname)
	}
	fmt.Println("Gathered gather-diagnostics for " + pubsubHostname)
	// extract diagnostic to host
	// first get absolute path from container
	resp, diagnosticPath, err := context.dockerExec(pubsubHostname, []string{"/bin/bash", "-l", "-c", "ls -rt /usr/sw/jail/logs/gather-diagnostics*.tgz | tail -n 1"})
	//resp, diagnosticPath, err := context.dockerExec(pubsubHostname, []string{"/bin/bash", "-l", "-c", " realpath $(ls -rt /usr/sw/jail/logs/gather-diagnostics*.tgz | tail -n 1)"})
	if err != nil {
		return err
	}
	if resp != 0 {
		return fmt.Errorf("failed to locate %s diagnostics", pubsubHostname)
	}
	fmt.Println("Exacting gather-diagnostics " + diagnosticPath + " for " + pubsubHostname + " to " + destinationPath + "...")
	err = context.dockerCpToHost(pubsubHostname, strings.TrimSpace(diagnosticPath), destinationPath)
	return err
}

// CacheOperationalState enumerates the different possible states of a cache instance.
// It would be great if this was defined as an accessible enum in the generated SEMP, but it is not. At least having
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

func (ctx *testContainersTestContext) waitForOperationalCache(timeout float64, state CacheOperationalState) error {
        for _, distributedCache := range ctx.Cache().DistributedCaches {
                for _, cacheCluster := range distributedCache.CacheClusters {
                        for _, cacheInstance := range cacheCluster.CacheInstances {
                                err := ctx.waitForCacheState(distributedCache.Name,
                                        cacheCluster.Name,
                                        cacheInstance.Name,
                                        string(state),
                                        time.Duration(timeout * float64(time.Second)))
                                if err != nil {
                                        return err
                                }
                        }
                }
        }
        return nil
}

func (ctx *testContainersTestContext) waitForCacheState(distributedCacheName string, cacheClusterName string, cacheClusterInstanceName string, targetState string, timeout time.Duration) error {
    found := false

	pollInterval := 500 * time.Millisecond
	timeoutChannel := time.After(timeout)

    for !found {
        cacheInstance, httpResponse, err := ctx.SEMPv2().Monitor().DistributedCacheApi.GetMsgVpnDistributedCacheClusterInstance(ctx.SEMPv2().MonitorCtx(),
                                                                               ctx.Cache().Vpn,
                                                                               distributedCacheName,
                                                                               cacheClusterName,
                                                                               cacheClusterInstanceName,
                                                                               nil)
 
                                                                               fmt.Printf("CacheDebug: err is %s\n", err)
                                                                               fmt.Printf("CacheDebug: httpResponse is %s\n", httpResponse.Body)
                                                                               fmt.Printf("CacheDebug: httpResponse.StatusCode is %d\n", httpResponse.StatusCode)
                if httpResponse.StatusCode == 200 {
                        fmt.Printf("CacheDebug: cacheInstance.Data.State is %s\n", cacheInstance.Data.State)
                        fmt.Printf("CacheDebug: cacheInstance.Data.InstanceName is %s\n", cacheInstance.Data.InstanceName)
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
                return fmt.Errorf("cache did not setup properly, got SEMP result %s", httpResponse.Body)
            }
            return err

        case <- time.After(pollInterval):
                continue
        }
    }
    return fmt.Errorf("failed to retrieve cache instance update")
 }

/* TODO: Refactor this to use the one in semphhelpers instead of rewriting it here.
*/
var True = tempBoolPointer(true)
var False = tempBoolPointer(false)

func tempBoolPointer(b bool) **bool {
        bp := &b
        return &bp
}

func (ctx *testContainersTestContext) initCache() error {
        const clientProfileName = "default"
        fmt.Println("CacheDebug: configuring msg vpn now")
        vpn, httpResponse, err := ctx.SEMPv2().Monitor().MsgVpnApi.GetMsgVpn(ctx.SEMPv2().MonitorCtx(), ctx.Cache().Vpn, nil)
        if err != nil {
                if httpResponse.StatusCode == 400 {
                        _, _, err = ctx.SEMPv2().Config().MsgVpnApi.CreateMsgVpn(ctx.SEMPv2().ConfigCtx(), config.MsgVpn{
                                AuthenticationBasicEnabled: True,
                                AuthenticationBasicProfileName: "",
                                AuthenticationBasicType: "none",
                                Enabled: True,
                                MaxMsgSpoolUsage: 0,
                                MsgVpnName: ctx.Cache().Vpn,
                        }, nil)
                        if err != nil {
                                return err
                        }
                        _, _, err = ctx.SEMPv2().Config().MsgVpnApi.UpdateMsgVpnClientProfile(ctx.SEMPv2().ConfigCtx(),
                                config.MsgVpnClientProfile{
                                        ClientProfileName: clientProfileName,
                                        //ClientProfileName: ctx.Messaging().ClientProfile,
                                        AllowBridgeConnectionsEnabled: True,
                                        AllowGuaranteedEndpointCreateEnabled: True,
                                        AllowGuaranteedMsgReceiveEnabled: True,
                                        AllowGuaranteedMsgSendEnabled: True,
                                },
                                ctx.Cache().Vpn,
                                clientProfileName,
                                nil)
                        if err != nil {
                                return err
                        }
                        _, _, err = ctx.SEMPv2().Config().MsgVpnApi.UpdateMsgVpnClientUsername(ctx.SEMPv2().ConfigCtx(),
                                config.MsgVpnClientUsername{
                                        Password: "default",
                                        ClientUsername: "default",
                                        MsgVpnName: ctx.Cache().Vpn,
                                        Enabled: True,
                                },
                                ctx.Cache().Vpn,
                                clientProfileName,
                                nil)
                        if err != nil {
                                return err
                        }
                        _, _, err = ctx.SEMPv2().Config().MsgVpnApi.UpdateMsgVpn(ctx.SEMPv2().ConfigCtx(),
                                config.MsgVpn{
                                        Enabled: True,
                                },
                                ctx.Cache().Vpn,
                                nil)
                        if err != nil {
                                return nil
                        }
                } else {
                        return err
                }
        } else if httpResponse.StatusCode == 200 {
                if !**vpn.Data.Enabled {
                       return fmt.Errorf("Cache configuration vpn %s is present but not enabled", vpn.Data.MsgVpnName)
                }
        } else {
                return fmt.Errorf("encountered unexpected HTTP response code %d with nil error", httpResponse.StatusCode)
        }
        fmt.Println("CacheDebug: creating distributed cache now")
        for _, distributedCache := range ctx.Cache().DistributedCaches {
                _, httpResponse, err = ctx.SEMPv2().Monitor().DistributedCacheApi.GetMsgVpnDistributedCache(ctx.SEMPv2().MonitorCtx(), ctx.Cache().Vpn, distributedCache.Name, nil)
                fmt.Printf("CacheDebug: Tried getting distributed cache.\nerr was %s,\nhttpResponse.StatusCode was %d,\nhttpResponse.Body is %s\n", err, httpResponse.StatusCode, httpResponse.Body)
                if err != nil {
                        if httpResponse.StatusCode == 400 {
                                /* Cache missing so create it, otherwise we assume configuration has already occurred. */
                                _, _, err = ctx.SEMPv2().Config().DistributedCacheApi.CreateMsgVpnDistributedCache(ctx.SEMPv2().ConfigCtx(),
                                config.MsgVpnDistributedCache{
                                        CacheName: distributedCache.Name,
                                        MsgVpnName: ctx.Cache().Vpn,
                                        Enabled: True,
                                },
                                ctx.Cache().Vpn,
                                nil)
                                if err != nil {
                                        fmt.Printf("CacheDebug: returning err %s, when cache was missing and creation was attempted but ret was 400.", err)
                                        return err
                                }
                        } else {
                                fmt.Printf("CacheDebug: returning err %s, when cache was missing but ret was not 400", err)
                                return err
                        }
                }
                fmt.Println("CacheDebug: creating cache clusters now.")
                for _, cacheCluster := range distributedCache.CacheClusters {
                        _, httpResponse, err = ctx.SEMPv2().Monitor().DistributedCacheApi.GetMsgVpnDistributedCacheCluster(ctx.SEMPv2().MonitorCtx(), ctx.Cache().Vpn, distributedCache.Name, cacheCluster.Name, nil)
                        fmt.Printf("CacheDebug: err for cache cluster get is %s", err)
                        fmt.Printf("CacheDebug: status code for cache cluster get is %d", httpResponse.StatusCode)
                        if err != nil {
                                if httpResponse.StatusCode == 400 {
                                        _, _, err = ctx.SEMPv2().Config().DistributedCacheApi.CreateMsgVpnDistributedCacheCluster(ctx.SEMPv2().ConfigCtx(),
                                        config.MsgVpnDistributedCacheCluster{
                                                CacheName: distributedCache.Name,
                                                ClusterName: cacheCluster.Name,
                                                MsgVpnName: ctx.Cache().Vpn,
                                                Enabled: True,
                                                MaxMsgsPerTopic: int64(cacheCluster.Properties.MaxMsgsPerTopic),
                                                MaxMemory: int64(cacheCluster.Properties.MaxMemory),
                                                MaxTopicCount: int64(cacheCluster.Properties.MaxTopicCount),
                                        },
                                        ctx.Cache().Vpn,
                                        distributedCache.Name,
                                        nil)
                                        if err != nil {
                                                return err
                                        }
                                } else {
                                        return err
                                }
                        }
                        fmt.Println("CacheDebug: configuring cache cluster topics now")
                        for _, cacheTopic := range cacheCluster.Topics {
                                _, httpResponse, err = ctx.SEMPv2().Monitor().DistributedCacheApi.GetMsgVpnDistributedCacheClusterTopic(ctx.SEMPv2().MonitorCtx(), ctx.Cache().Vpn, distributedCache.Name, cacheCluster.Name, cacheTopic, nil)
                                fmt.Printf("CacheDebug: err for cache cluster topics get is %s\n", err)
                                fmt.Printf("CacheDebug: status code for cache cluster topics get is %d\n", httpResponse.StatusCode)

                                if err != nil {
                                        if httpResponse.StatusCode == 400 {
                                                _, httpResponse, err = ctx.SEMPv2().Config().DistributedCacheApi.CreateMsgVpnDistributedCacheClusterTopic(ctx.SEMPv2().ConfigCtx(),
                                                config.MsgVpnDistributedCacheClusterTopic{
                                                        CacheName: distributedCache.Name,
                                                        ClusterName: cacheCluster.Name,
                                                        MsgVpnName: ctx.Cache().Vpn,
                                                        Topic: cacheTopic,
                                                },
                                                ctx.Cache().Vpn,
                                                distributedCache.Name,
                                                cacheCluster.Name,
                                                nil)
                                                if err != nil {
                                                        fmt.Printf("CacheDebug: err for cache cluster topic create is %s\n", err)
                                                        fmt.Printf("CacheDebug: status code for cache cluster topic create is %d\n", httpResponse.StatusCode)
                                                        return err
                                                }
                                        } else {
                                                return err
                                        }
                                }
                        }
                        fmt.Println("CacheDebug: configuring cache instances now")
                        for _, cacheInstance := range cacheCluster.CacheInstances {
                                _, httpResponse, err = ctx.SEMPv2().Monitor().DistributedCacheApi.GetMsgVpnDistributedCacheClusterInstance(ctx.SEMPv2().MonitorCtx(), ctx.Cache().Vpn, distributedCache.Name, cacheCluster.Name, cacheInstance.Name, nil)
                                fmt.Printf("CacheDebug: err for cache instances get is %s\n", err)
                                fmt.Printf("CacheDebug: status code for cache instances get is %d\n", httpResponse.StatusCode)
                                if err != nil {
                                        if httpResponse.StatusCode == 400 {
                                                _, httpResponse, err = ctx.SEMPv2().Config().DistributedCacheApi.CreateMsgVpnDistributedCacheClusterInstance(ctx.SEMPv2().ConfigCtx(),
                                                config.MsgVpnDistributedCacheClusterInstance{
                                                        Enabled: True,
                                                        //AutoStartEnabled: tempBoolPointer(cacheInstance.Autostart),
                                                        AutoStartEnabled: True,
                                                        MsgVpnName: ctx.Cache().Vpn,
                                                        CacheName: distributedCache.Name,
                                                        ClusterName: cacheCluster.Name,
                                                        InstanceName: cacheInstance.Name,
                                                        //StopOnLostMsgEnabled: tempBoolPointer(cacheInstance.Properties.StopOnLostMsgEnabled),
                                                        StopOnLostMsgEnabled: True,
                                                },
                                                ctx.Cache().Vpn,
                                                distributedCache.Name,
                                                cacheCluster.Name,
                                                nil)
                                                if err != nil {
                                                        fmt.Printf("CacheDebug: err for cache instances create is %s\n", err)
                                                        fmt.Printf("CacheDebug: status code for cache instances create is %d\n", httpResponse.StatusCode)
                                                        return err
                                                }
                                        } else {
                                                return err
                                        }
                                }
                        }
                }
        }

        fmt.Println("CacheDebug: returning nil because initCache bottomed out")
    return nil
}

func (ctx *testContainersTestContext) setupCache() error {
        fmt.Println("CacheDebug: Initializing cache configuration on the broker")
        err := ctx.initCache()
        fmt.Printf("err from initCache is %s", err)
        if err != nil {
            return err
        }
        fmt.Println("CacheDebug: Waiting for operational cache")
        err = ctx.waitForOperationalCache(30.0, CacheOperationalStateUp)
        fmt.Printf("CacheDebug: err from waitForOperationalCache is %s", err)
        return err
}

// setupKerberos sets up kerberos with the following steps:
// 1. creates the pubsub domain keytab on the kerberos auth server (createPubsubKeytabTemplate)
// 2. creates the local domain keytab on the kerberos auth server (createLocalKeytabTemplate)
// 3. copies both keytabs into the broker and adds read permissions
// 4. adds the keytabs to the broker via SEMPv1 commands
// 5. runs kinit with the configured username and password
func (ctx *testContainersTestContext) setupKerberos() error {
	const keytabTemplate = "krb5-%s.keytab"
	const createPubsubKeytabTemplate = `cat << EOF | kadmin.local
	add_principal -pw testadmin "tadmin/admin@%s"
	add_principal -pw test "test@%s"
	add_principal -randkey -kvno 3 "solace/%s@%s"
	add_principal -randkey -kvno 3 "solace/localhost@$%s"
	ktadd -k /etc/%s -norandkey "solace/%s@%s"
	listprincs
	quit
	EOF
	`
	const createLocalKeytabTemplate = `cat << EOF | kadmin.local -r %s
	add_principal -pw testadmin "tadmin/admin@%s"
	add_principal -pw localtest "localtest@%s"
	add_principal -randkey -kvno 3 "%s"
	ktadd -k /etc/%s -norandkey "%s"
	listprincs
	quit
	EOF
	`

	pubsubHostname := ctx.config.TestContainers.BrokerHostname
	krbHost := ctx.config.Kerberos.Hostname
	domain := ctx.config.Kerberos.Domain
	pubsubKeytab := fmt.Sprintf(keytabTemplate, pubsubHostname)
	// Create keytab
	createPubSubKeytab := fmt.Sprintf(createPubsubKeytabTemplate,
		domain,
		domain,
		pubsubHostname,
		domain,
		domain,
		pubsubKeytab,
		pubsubHostname,
		domain,
	)
	resp, out, err := ctx.dockerExec(krbHost, []string{"/bin/bash", "-c", createPubSubKeytab})
	if err != nil {
		return err
	}
	if resp != 0 {
		fmt.Println(out)
		return fmt.Errorf("failed to setup kerberos config")
	}
	err = ctx.dockerCp(krbHost, "/etc/"+pubsubKeytab, pubsubHostname, "/usr/sw/jail/keytabs/"+pubsubKeytab)
	if err != nil {
		return err
	}

	// Local keytab
	localDomain := fmt.Sprintf("LOCAL.%s", domain)
	localPrinciple := fmt.Sprintf("solace/localhost@%s", localDomain)
	localKeytab := fmt.Sprintf(keytabTemplate, "local")
	// Create local keytab
	createLocalKeytab := fmt.Sprintf(createLocalKeytabTemplate,
		localDomain,
		localDomain,
		localDomain,
		localPrinciple,
		localKeytab,
		localPrinciple,
	)
	resp, out, err = ctx.dockerExec(krbHost, []string{"/bin/bash", "-c", createLocalKeytab})
	if err != nil {
		return err
	}
	if resp != 0 {
		fmt.Println(out)
		return fmt.Errorf("failed to setup local kerberos config")
	}
	err = ctx.dockerCp(krbHost, "/etc/"+localKeytab, pubsubHostname, "/usr/sw/jail/keytabs/"+localKeytab)
	if err != nil {
		return err
	}

	// Set krb config file
	wd, err := os.Getwd()
	if err != nil {
		return err
	}
	os.Setenv("KRB5_CONFIG", path.Join(wd, "data", "krb", "krb5.conf"))

	// Configure the keytabs
	const keytabRequest = `<authentication>
		<kerberos>
			<keytab>
				<add-key>
					<keytab-filename>%s</keytab-filename>
				</add-key>
			</keytab>
		</kerberos>
	</authentication>`

	for _, keytab := range []string{pubsubKeytab, localKeytab} {
		err = ctx.sempV1(fmt.Sprintf(keytabRequest, keytab))
		if err != nil {
			return err
		}
	}

	// Call kinit
	err = kinit(ctx.config.Kerberos.Username, ctx.config.Kerberos.Password)
	if err != nil {
		return err
	}
	return nil
}

const dockerCmd = "docker"

// dockerCp copies a file from one container to another container
func (ctx *testContainersTestContext) dockerCp(sourceContainer, sourcePath, destContainer, destPath string) error {
	tmpFileName := path.Join(os.TempDir(), "tmp-go-docker-cp")
	defer os.Remove(tmpFileName)
	cmd := exec.Command(dockerCmd, "cp", sourceContainer+":"+sourcePath, tmpFileName)
	output, err := cmd.CombinedOutput()
	if err != nil {
		fmt.Println(string(output))
		return fmt.Errorf("encountered error running docker cp to local from %s: %w", sourceContainer, err)
	}
	err = os.Chmod(tmpFileName, 0655)
	if err != nil {
		return fmt.Errorf("encountered error running chmod on %s: %w", tmpFileName, err)
	}
	cmd = exec.Command(dockerCmd, "cp", tmpFileName, destContainer+":"+destPath)
	output, err = cmd.CombinedOutput()
	if err != nil {
		fmt.Println(string(output))
		return fmt.Errorf("encountered error running docker cp from local to %s: %w", destContainer, err)
	}
	return nil
}

func (ctx *testContainersTestContext) dockerCpToHost(sourceContainer, sourcePath, destPath string) error {
	hostFileName := destPath
	fmt.Println("Copying source file " + sourceContainer + ":" + sourcePath + " to " + hostFileName)
	cmd := exec.Command(dockerCmd, "cp", "-L", sourceContainer+":"+sourcePath, hostFileName)
	output, err := cmd.CombinedOutput()
	if err != nil {
		fmt.Println(string(output))
		return fmt.Errorf("encountered error running docker cp to local from %s: %w", sourceContainer, err)
	}
	err = os.Chmod(hostFileName, 0655)
	if err != nil {
		return fmt.Errorf("encountered error running chmod on %s: %w", hostFileName, err)
	}
	return nil
}

// dockerExec runs the given command on the given container using the given user if not empty
func (ctx *testContainersTestContext) dockerExec(container string, command []string) (int, string, error) {
	args := append([]string{"exec", container}, command...)
	cmd := exec.Command(dockerCmd, args...)
	output, err := cmd.CombinedOutput()
	return cmd.ProcessState.ExitCode(), string(output), err
}

// dockerLogs runs docker log on given container
func (ctx *testContainersTestContext) dockerLogs(container string) (int, string, error) {
	args := []string{"logs", container}
	cmd := exec.Command(dockerCmd, args...)
	output, err := cmd.CombinedOutput()
	return cmd.ProcessState.ExitCode(), string(output), err
}
