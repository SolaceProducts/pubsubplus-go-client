#!/bin/bash

# Needs go on the path, probably:
# export PATH=$PATH:/usr/local/go/bin
# Blindly following the instructions at https://sol-jira.atlassian.net/wiki/spaces/CPD/pages/2868052115/Golang+Process+and+Tools#Updating-CCSMP
# Worked once, so saving for posterity.

LOAD=/home/public/RND/loads/ccsmp/trunk/100.0.0.924/

cp ${LOAD}/Linux26-x86_64_opt/solclient/lib/libsolclient.a ./internal/ccsmp/lib/linux_amd64/
cp ${LOAD}/Linux-aarch64_opt/solclient/lib/libsolclient.a ./internal/ccsmp/lib/linux_arm64/
cp ${LOAD}/Darwin-universal2_opt/solclient/lib/libsolclient.a ./internal/ccsmp/lib/darwin/

cp ${LOAD}/Linux26-x86_64_opt/solclient/include/solclient/solClient*.h ./internal/ccsmp/lib/include/solclient/
rm internal/ccsmp/lib/include/solclient/solClientIPC.h

pushd internal/ccsmp/
SOLCLIENT_H=`realpath ./lib/include/solclient/solClient.h`  go generate
popd
pushd pkg/solace/subcode
SOLCLIENT_H=`realpath ../../../internal/ccsmp/lib/include/solclient/solClient.h`  go generate
popd

cp ${LOAD}/Linux26-x86_64_opt/solclient/lib/licenses.txt  ./internal/ccsmp/lib/
