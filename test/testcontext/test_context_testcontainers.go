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

//go:build !remote
// +build !remote

package testcontext

import (
	"fmt"
	"os"
	"os/exec"
	"path"
	"strings"

	"github.com/google/uuid"
	"github.com/testcontainers/testcontainers-go"
)

// the default config file relative to the location the tests are run from
// this assumes that the tests are run from the root of the `test` directory
const testContainersConfig = "./data/config/config_testcontainers.json"
const defaultComposeFilePath = "data/compose/docker-compose.yml"
const kerberosComposeFilePath = "data/compose/docker-compose.krb.yml"
const oauthComposeFilePath = "data/compose/docker-compose.oauth.yml"

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
	identifier := strings.ToLower(uuid.New().String())
	compose := testcontainers.NewLocalDockerCompose(composeFilePaths, identifier)

	context.compose = compose

	context.semp = newSempV2(context.config.SEMP)
	context.toxi = newToxiProxy(context.config.ToxiProxy)

	fmt.Println("Starting dockerized broker via docker-compose")

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
