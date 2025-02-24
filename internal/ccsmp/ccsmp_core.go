// pubsubplus-go-client
//
// Copyright 2021-2025 Solace Corporation. All rights reserved.
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

// Package ccsmp contains all the code required to wrap ccsmp in golang with cgo.
// Ideally, no cgo code should be required when using this package.
package ccsmp

/*
#cgo CFLAGS: -DSOLCLIENT_PSPLUS_GO
#include <stdlib.h>
#include <stdio.h>

#include <string.h>

#include "solclient/solClient.h"
#include "solclient/solClientMsg.h"
#include "./ccsmp_helper.h"

solClient_rxMsgCallback_returnCode_t messageReceiveCallback ( solClient_opaqueSession_pt opaqueSession_p, solClient_opaqueMsg_pt msg_p, void *user_p );
solClient_rxMsgCallback_returnCode_t requestResponseReplyMessageReceiveCallback ( solClient_opaqueSession_pt opaqueSession_p, solClient_opaqueMsg_pt msg_p, void *user_p );
solClient_rxMsgCallback_returnCode_t defaultMessageReceiveCallback ( solClient_opaqueSession_pt opaqueSession_p, solClient_opaqueMsg_pt msg_p, void *user_p );
void eventCallback ( solClient_opaqueSession_pt opaqueSession_p, solClient_session_eventCallbackInfo_pt eventInfo_p, void *user_p );
void handleLogCallback(solClient_log_callbackInfo_pt logInfo_p, void *user_p);

solClient_rxMsgCallback_returnCode_t flowMessageReceiveCallback ( solClient_opaqueFlow_pt opaqueFlow_p, solClient_opaqueMsg_pt msg_p, void *user_p );
solClient_rxMsgCallback_returnCode_t defaultFlowMessageReceiveCallback ( solClient_opaqueFlow_pt opaqueFlow_p, solClient_opaqueMsg_pt msg_p, void *user_p );
void flowEventCallback ( solClient_opaqueFlow_pt opaqueFlow_p, solClient_flow_eventCallbackInfo_pt eventInfo_p, void *user_p );

solClient_returnCode_t _solClient_version_set(solClient_version_info_pt version_p);
*/
import "C"
import (
	"fmt"
	"runtime"
	"sync"
	"unsafe"

	"solace.dev/go/messaging/internal/impl/logging"
)

// Reexport of various CCSMP types

type SolClientOpaquePointerType = C.uintptr_t

const SolClientOpaquePointerInvalidValue = SolClientOpaquePointerType(0)

// SolClientContextPt is assigned a value
type SolClientContextPt = C.solClient_opaqueContext_pt

// SolClientSessionPt is assigned a value
type SolClientSessionPt = C.solClient_opaqueSession_pt

// SolClientSessionEventInfoPt is assigned a value
type SolClientSessionEventInfoPt = C.solClient_session_eventCallbackInfo_pt

// SolClientLogInfoPt is assigned a value
type SolClientLogInfoPt = C.solClient_log_callbackInfo_pt

// SolClientSessionRxMsgDispatchFuncInfo is assigned a value
type SolClientSessionRxMsgDispatchFuncInfo = C.solClient_session_rxMsgDispatchFuncInfo_t

// SolClientVersionInfo is assigned a value
type SolClientVersionInfo = C.solClient_version_info_t

// SolClientCorrelationID is assigned a value
type SolClientCorrelationID = *C.char

// Reexport various CCSMP variables

// SolClientPropEnableVal is assigned a value
var SolClientPropEnableVal = C.SOLCLIENT_PROP_ENABLE_VAL

// SolClientPropDisableVal is assigned a value
var SolClientPropDisableVal = C.SOLCLIENT_PROP_DISABLE_VAL

// Reexport solclientgo variables

// SolClientGoPropCorrelationPrefix property value
// var SolClientGoPropCorrelationPrefix = C.GoString(C.SOLCLIENTGO_REPLY_CORRELATION_PREFIX)
var SolClientGoPropCorrelationPrefix = C.SOLCLIENTGO_REPLY_CORRELATION_PREFIX

// Callbacks

// SolClientMessageCallback is assigned a function
type SolClientMessageCallback = func(msgP SolClientMessagePt, userP unsafe.Pointer) bool

// SolClientReplyMessageCallback assigned a function
type SolClientReplyMessageCallback = func(msgP SolClientMessagePt, userP unsafe.Pointer, correlationP string) bool

// SolClientSessionEventCallback is assigned a function
type SolClientSessionEventCallback = func(sessionEvent SolClientSessionEvent, responseCode SolClientResponseCode, info string, correlationP unsafe.Pointer, userP unsafe.Pointer)

// maps to callbacks
var sessionToRXCallbackMap sync.Map
var sessionToReplyRXCallbackMap sync.Map
var sessionToEventCallbackMap sync.Map

//export goMessageReceiveCallback
func goMessageReceiveCallback(sessionP SolClientSessionPt, msgP SolClientMessagePt, userP unsafe.Pointer) C.solClient_rxMsgCallback_returnCode_t {
	if callback, ok := sessionToRXCallbackMap.Load(sessionP); ok {
		if callback.(SolClientMessageCallback)(msgP, userP) {
			return C.SOLCLIENT_CALLBACK_TAKE_MSG
		}
		return C.SOLCLIENT_CALLBACK_OK
	}
	logging.Default.Error("Received message from core API without an associated session callback")
	return C.SOLCLIENT_CALLBACK_OK
}

//export goReplyMessageReceiveCallback
func goReplyMessageReceiveCallback(sessionP SolClientSessionPt, msgP SolClientMessagePt, userP unsafe.Pointer, correlationIDP SolClientCorrelationID) C.solClient_rxMsgCallback_returnCode_t {
	// propagate to request reponse reply message handler
	if callback, ok := sessionToReplyRXCallbackMap.Load(sessionP); ok {
		if callback.(SolClientReplyMessageCallback)(msgP, userP, C.GoString(correlationIDP)) {
			return C.SOLCLIENT_CALLBACK_TAKE_MSG
		}
		return C.SOLCLIENT_CALLBACK_OK
	}
	logging.Default.Error("Received reply message from core API without an associated session callback")
	return C.SOLCLIENT_CALLBACK_OK
}

//export goDefaultMessageReceiveCallback
func goDefaultMessageReceiveCallback(sessionP SolClientSessionPt, msgP SolClientMessagePt, userP unsafe.Pointer) C.solClient_rxMsgCallback_returnCode_t {
	logging.Default.Error("Received message from core API on the default session callback")
	return C.SOLCLIENT_CALLBACK_OK
}

//export goEventCallback
func goEventCallback(sessionP SolClientSessionPt, eventInfoP SolClientSessionEventInfoPt, userP unsafe.Pointer) {
	if callback, ok := sessionToEventCallbackMap.Load(sessionP); ok {
		callback.(SolClientSessionEventCallback)(SolClientSessionEvent(eventInfoP.sessionEvent), eventInfoP.responseCode, C.GoString(eventInfoP.info_p), eventInfoP.correlation_p, userP)
	} else {
		logging.Default.Debug("Received event callback from core API without an associated session callback")
	}
}

// Logging

// LogInfo structure
type LogInfo struct {
	Message string
	Level   SolClientLogLevel
}

// LogCallback function
type LogCallback func(logInfoP *LogInfo)

var logCallback LogCallback

//export goLogCallback
func goLogCallback(logInfoP SolClientLogInfoPt, userP unsafe.Pointer) {
	logCallback(&LogInfo{Message: C.GoString(logInfoP.msg_p), Level: SolClientLogLevel(logInfoP.level)})
}

// SetLogCallback sets the log callback for global logging
func SetLogCallback(callback LogCallback) *SolClientErrorInfoWrapper {
	logCallback = callback
	return handleCcsmpError(func() SolClientReturnCode {
		return C.solClient_log_setCallback((C.solClient_session_rxMsgCallbackFunc_t)(unsafe.Pointer(C.handleLogCallback)), nil)
	})
}

// SetLogLevel sets the log level for global logging
func SetLogLevel(level SolClientLogLevel) *SolClientErrorInfoWrapper {
	return handleCcsmpError(func() SolClientReturnCode {
		return C.solClient_log_setFilterLevel(C.SOLCLIENT_LOG_CATEGORY_ALL, C.solClient_log_level_t(level))
	})
}

// Error helpers

// SolClientReturnCode is assigned a value
type SolClientReturnCode = C.solClient_returnCode_t

// SolClientSubCode is assigned a value
type SolClientSubCode = C.solClient_subCode_t

// SolClientSubCodeOK is assigned a value
const SolClientSubCodeOK = C.SOLCLIENT_SUBCODE_OK

// SolClientResponseCode is assigned a value
type SolClientResponseCode = C.solClient_session_responseCode_t

// SolClientErrorInfoWrapper is assigned a value
type SolClientErrorInfoWrapper C.solClient_errorInfo_wrapper_t

// SolClientErrorInfoWrapperDetailed is assigned a value
type SolClientErrorInfoWrapperDetailed C.solClient_errorInfo_t

func (info *SolClientErrorInfoWrapper) String() string {
	if info == nil {
		return ""
	}
	if info.DetailedErrorInfo == nil {
		return fmt.Sprintf("{ReturnCode: %d, SubCode: nil, ResponseCode: nil, ErrorStr: nil}", info.ReturnCode)
	}
	detailedErrorInfo := *(info.DetailedErrorInfo)
	return fmt.Sprintf("{ReturnCode: %d, SubCode: %d, ResponseCode: %d, ErrorStr: %s}",
		info.ReturnCode,
		detailedErrorInfo.subCode,
		detailedErrorInfo.responseCode,
		info.GetMessageAsString())
}

// GetMessageAsString function outputs a string
func (info *SolClientErrorInfoWrapper) GetMessageAsString() string {
	if info.DetailedErrorInfo == nil || len(info.DetailedErrorInfo.errorStr) == 0 {
		return ""
	}
	return C.GoString((*C.char)(&info.DetailedErrorInfo.errorStr[0]))
}

// SubCode function returns subcode if available
func (info *SolClientErrorInfoWrapper) SubCode() SolClientSubCode {
	if info.DetailedErrorInfo != nil {
		return (*(info.DetailedErrorInfo)).subCode
	}
	return SolClientSubCode(0)
}

// ResponseCode function returns response code if available
func (info *SolClientErrorInfoWrapper) ResponseCode() SolClientResponseCode {
	if info.DetailedErrorInfo != nil {
		return (*(info.DetailedErrorInfo)).responseCode
	}
	return SolClientResponseCode(0)
}

// Definition of structs returned from this package to be used externally

// SolClientContext structure
type SolClientContext struct {
	pointer SolClientContextPt
}

// SolClientSession structure
type SolClientSession struct {
	context *SolClientContext
	pointer SolClientSessionPt
}

// SetMessageCallback sets the message callback to use
func (session *SolClientSession) SetMessageCallback(callback SolClientMessageCallback) error {
	if session == nil || session.pointer == SolClientOpaquePointerInvalidValue {
		return fmt.Errorf("could not set message receive callback for nil session")
	}
	if callback == nil {
		sessionToRXCallbackMap.Delete(session.pointer)
	} else {
		sessionToRXCallbackMap.Store(session.pointer, callback)
	}
	return nil
}

// SetReplyMessageCallback sets the message callback to use
func (session *SolClientSession) SetReplyMessageCallback(callback SolClientReplyMessageCallback) error {
	if session == nil || session.pointer == SolClientOpaquePointerInvalidValue {
		return fmt.Errorf("could not set message receive callback for nil session")
	}
	if callback == nil {
		sessionToReplyRXCallbackMap.Delete(session.pointer)
	} else {
		sessionToReplyRXCallbackMap.Store(session.pointer, callback)
	}
	return nil
}

// SetEventCallback sets the event callback to use
func (session *SolClientSession) SetEventCallback(callback SolClientSessionEventCallback) error {
	if session == nil || session.pointer == SolClientOpaquePointerInvalidValue {
		return fmt.Errorf("could not set event callback for nil session")
	}
	if callback == nil {
		sessionToEventCallbackMap.Delete(session.pointer)
	} else {
		sessionToEventCallbackMap.Store(session.pointer, callback)
	}
	return nil
}

// SolClientInitialize function initializes a client
func SolClientInitialize(props []string) *SolClientErrorInfoWrapper {
	return handleCcsmpError(func() SolClientReturnCode {
		cArr, freeFunc := ToCArray(props, true)
		defer freeFunc()
		return C.solClient_initialize(C.SOLCLIENT_LOG_DEFAULT_FILTER, cArr)
	})
}

// SolClientContextCreate wraps solClient_session_create
func SolClientContextCreate() (context *SolClientContext, err *SolClientErrorInfoWrapper) {
	var contextP SolClientContextPt
	solClientErrorInfo := handleCcsmpError(func() SolClientReturnCode {
		return C.SessionContextCreate(C.SOLCLIENT_CONTEXT_PROPS_DEFAULT_WITH_CREATE_THREAD, &contextP)
	})
	if solClientErrorInfo != nil {
		return nil, solClientErrorInfo
	}
	return &SolClientContext{pointer: contextP}, nil
}

// SolClientContextDestroy wraps solClient_context_destroy
func (context *SolClientContext) SolClientContextDestroy() *SolClientErrorInfoWrapper {
	return handleCcsmpError(func() SolClientReturnCode {
		return C.solClient_context_destroy(&context.pointer)
	})
}

// SolClientSessionCreate wraps solClient_session_create
func (context *SolClientContext) SolClientSessionCreate(properties []string) (session *SolClientSession, err *SolClientErrorInfoWrapper) {
	var sessionP SolClientSessionPt
	sessionPropsP, sessionPropertiesFreeFunction := ToCArray(properties, true)
	defer sessionPropertiesFreeFunction()

	solClientErrorInfo := handleCcsmpError(func() SolClientReturnCode {
		return C.SessionCreate(sessionPropsP,
			context.pointer,
			&sessionP)
	})
	if solClientErrorInfo != nil {
		return nil, solClientErrorInfo
	}
	return &SolClientSession{context: context, pointer: sessionP}, nil
}

// SolClientSessionConnect wraps solClient_session_connect
func (session *SolClientSession) SolClientSessionConnect() *SolClientErrorInfoWrapper {
	return handleCcsmpError(func() SolClientReturnCode {
		return C.solClient_session_connect(session.pointer)
	})
}

// SolClientSessionDisconnect wraps solClient_session_disconnect
func (session *SolClientSession) SolClientSessionDisconnect() *SolClientErrorInfoWrapper {
	return handleCcsmpError(func() SolClientReturnCode {
		return C.solClient_session_disconnect(session.pointer)
	})
}

// SolClientSessionDestroy wraps solClient_session_destroy
func (session *SolClientSession) SolClientSessionDestroy() *SolClientErrorInfoWrapper {
	// last line of defence to make sure everything is cleaned up
	sessionToEventCallbackMap.Delete(session.pointer)
	sessionToRXCallbackMap.Delete(session.pointer)
	sessionToReplyRXCallbackMap.Delete(session.pointer)
	return handleCcsmpError(func() SolClientReturnCode {
		return C.solClient_session_destroy(&session.pointer)
	})
}

// SolClientSessionPublish wraps solClient_session_sendMsg
func (session *SolClientSession) SolClientSessionPublish(message SolClientMessagePt) *SolClientErrorInfoWrapper {
	// TODO we may want to improve this by wrapping solClient_session_sendMsg in C so we do not have to lock the thread
	return handleCcsmpError(func() SolClientReturnCode {
		return C.solClient_session_sendMsg(session.pointer, message)
	})
}

// solClientSessionSubscribeWithFlags wraps solClient_session_topicSubscribeWithDispatch
func (session *SolClientSession) solClientSessionSubscribeWithFlags(topic string, flags C.solClient_subscribeFlags_t, dispatchID uintptr, correlationID uintptr) *SolClientErrorInfoWrapper {
	return handleCcsmpError(func() SolClientReturnCode {
		cString := C.CString(topic)
		defer C.free(unsafe.Pointer(cString))
		return C.SessionTopicSubscribeWithFlags(session.pointer,
			cString,
			flags,
			C.solClient_uint64_t(dispatchID),
			C.solClient_uint64_t(correlationID))
	})
}

// solClientSessionSubscribeWithFlags wraps solClient_session_topicSubscribeWithDispatch
func (session *SolClientSession) solClientSessionSubscribeReplyTopicWithFlags(topic string, flags C.solClient_subscribeFlags_t, dispatchID uintptr, correlationID uintptr) *SolClientErrorInfoWrapper {
	return handleCcsmpError(func() SolClientReturnCode {
		cString := C.CString(topic)
		defer C.free(unsafe.Pointer(cString))
		return C.SessionReplyTopicSubscribeWithFlags(session.pointer,
			cString,
			flags,
			C.solClient_uint64_t(dispatchID),
			C.solClient_uint64_t(correlationID))
	})
}

// solClientSessionUnsubscribeWithFlags wraps solClient_session_topicUnsubscribeWithDispatch
func (session *SolClientSession) solClientSessionUnsubscribeWithFlags(topic string, flags C.solClient_subscribeFlags_t, dispatchID uintptr, correlationID uintptr) *SolClientErrorInfoWrapper {
	return handleCcsmpError(func() SolClientReturnCode {
		cString := C.CString(topic)
		defer C.free(unsafe.Pointer(cString))
		return C.SessionTopicUnsubscribeWithFlags(session.pointer,
			cString,
			flags,
			C.solClient_uint64_t(dispatchID),
			C.solClient_uint64_t(correlationID))
	})
}

// solClientSessionUnsubscribeReplyTopicWithFlags wraps solClient_session_topicUnsubscribeWithDispatch
func (session *SolClientSession) solClientSessionUnsubscribeReplyTopicWithFlags(topic string, flags C.solClient_subscribeFlags_t, dispatchID uintptr, correlationID uintptr) *SolClientErrorInfoWrapper {
	return handleCcsmpError(func() SolClientReturnCode {
		cString := C.CString(topic)
		defer C.free(unsafe.Pointer(cString))
		return C.SessionReplyTopicUnsubscribeWithFlags(session.pointer,
			cString,
			flags,
			C.solClient_uint64_t(dispatchID),
			C.solClient_uint64_t(correlationID))
	})
}

// SolClientSessionSubscribeReplyTopic wraps solClient_session_topicSubscribeWithDispatch
func (session *SolClientSession) SolClientSessionSubscribeReplyTopic(topic string, dispatchID uintptr, correlationID uintptr) *SolClientErrorInfoWrapper {
	return session.solClientSessionSubscribeReplyTopicWithFlags(topic, C.SOLCLIENT_SUBSCRIBE_FLAGS_LOCAL_DISPATCH_ONLY, dispatchID, correlationID)
}

// SolClientSessionUnsubscribeReplyTopic wraps solClient_session_topicUnsubscribeWithDispatch
func (session *SolClientSession) SolClientSessionUnsubscribeReplyTopic(topic string, dispatchID uintptr, correlationID uintptr) *SolClientErrorInfoWrapper {
	return session.solClientSessionUnsubscribeReplyTopicWithFlags(topic, C.SOLCLIENT_SUBSCRIBE_FLAGS_LOCAL_DISPATCH_ONLY, dispatchID, correlationID)
}

// SolClientSessionSubscribe wraps solClient_session_topicSubscribeWithDispatch
func (session *SolClientSession) SolClientSessionSubscribe(topic string, dispatchID uintptr, correlationID uintptr) *SolClientErrorInfoWrapper {
	return session.solClientSessionSubscribeWithFlags(topic, C.SOLCLIENT_SUBSCRIBE_FLAGS_REQUEST_CONFIRM, dispatchID, correlationID)
}

// SolClientSessionUnsubscribe wraps solClient_session_topicUnsubscribeWithDispatch
func (session *SolClientSession) SolClientSessionUnsubscribe(topic string, dispatchID uintptr, correlationID uintptr) *SolClientErrorInfoWrapper {
	return session.solClientSessionUnsubscribeWithFlags(topic, C.SOLCLIENT_SUBSCRIBE_FLAGS_REQUEST_CONFIRM, dispatchID, correlationID)
}

// SolClientEndpointUnsusbcribe wraps solClient_session_endpointTopicUnsubscribe
func (session *SolClientSession) SolClientEndpointUnsusbcribe(properties []string, topic string, correlationID uintptr) *SolClientErrorInfoWrapper {
	return handleCcsmpError(func() SolClientReturnCode {
		cString := C.CString(topic)
		defer C.free(unsafe.Pointer(cString))
		endpointProps, endpointFree := ToCArray(properties, true)
		defer endpointFree()
		return C.SessionTopicEndpointUnsubscribeWithFlags(session.pointer,
			endpointProps,
			C.SOLCLIENT_SUBSCRIBE_FLAGS_REQUEST_CONFIRM,
			cString,
			C.solClient_uint64_t(correlationID))
	})
}

// SolClientEndpointProvision wraps solClient_session_endpointProvision
func (session *SolClientSession) SolClientEndpointProvision(properties []string) *SolClientErrorInfoWrapper {
	return handleCcsmpError(func() SolClientReturnCode {
		endpointProps, endpointFree := ToCArray(properties, true)
		defer endpointFree()
		return C.solClient_session_endpointProvision(endpointProps, session.pointer, C.SOLCLIENT_PROVISION_FLAGS_WAITFORCONFIRM, nil, nil, 0)
	})
}

// SolClientEndpointProvisionWithFlags wraps solClient_session_endpointProvision
func (session *SolClientSession) SolClientEndpointProvisionWithFlags(properties []string, flags C.solClient_uint32_t, correlationID uintptr) *SolClientErrorInfoWrapper {
	return handleCcsmpError(func() SolClientReturnCode {
		endpointProps, endpointFree := ToCArray(properties, true)
		defer endpointFree()
		return C.SessionEndpointProvisionWithFlags(session.pointer,
			endpointProps,
			flags,
			C.solClient_uint64_t(correlationID))
	})
}

// SolClientEndpointDeprovisionWithFlags wraps solClient_session_endpointDeprovision
func (session *SolClientSession) SolClientEndpointDeprovisionWithFlags(properties []string, flags C.solClient_uint32_t, correlationID uintptr) *SolClientErrorInfoWrapper {
	return handleCcsmpError(func() SolClientReturnCode {
		endpointProps, endpointFree := ToCArray(properties, true)
		defer endpointFree()
		return C.SessionEndpointDeprovisionWithFlags(session.pointer,
			endpointProps,
			flags,
			C.solClient_uint64_t(correlationID))
	})
}

// SolClientEndpointProvisionAsync wraps solClient_session_endpointProvision
func (session *SolClientSession) SolClientEndpointProvisionAsync(properties []string, correlationID uintptr, ignoreExistErrors bool) *SolClientErrorInfoWrapper {
	if ignoreExistErrors {
		return session.SolClientEndpointProvisionWithFlags(properties, C.SOLCLIENT_PROVISION_FLAGS_IGNORE_EXIST_ERRORS, correlationID)
	}
	return session.SolClientEndpointProvisionWithFlags(properties, 0x0, correlationID) // no flag pass here
}

// SolClientEndpointDeprovisionAsync wraps solClient_session_endpointDeprovision
func (session *SolClientSession) SolClientEndpointDeprovisionAsync(properties []string, correlationID uintptr, ignoreMissingErrors bool) *SolClientErrorInfoWrapper {
	if ignoreMissingErrors {
		return session.SolClientEndpointDeprovisionWithFlags(properties, C.SOLCLIENT_PROVISION_FLAGS_IGNORE_EXIST_ERRORS, correlationID)
	}
	return session.SolClientEndpointDeprovisionWithFlags(properties, 0x0, correlationID) // no flag pass here
}

// SolClientSessionGetRXStat wraps solClient_session_getRxStat
func (session *SolClientSession) SolClientSessionGetRXStat(stat SolClientStatsRX) (value uint64) {
	err := handleCcsmpError(func() SolClientReturnCode {
		return C.solClient_session_getRxStat(session.pointer, C.solClient_stats_rx_t(stat), (C.solClient_stats_pt)(unsafe.Pointer(&value)))
	})
	// we should not in normal operation encounter an error fetching stats, but just in case...
	if err != nil {
		logging.Default.Warning("Encountered error loading core rx stat: " + err.GetMessageAsString() + ", subcode " + fmt.Sprint(err.SubCode()))
	}
	return value
}

// SolClientSessionGetTXStat wraps solClient_session_getTxStat
func (session *SolClientSession) SolClientSessionGetTXStat(stat SolClientStatsTX) (value uint64) {
	err := handleCcsmpError(func() SolClientReturnCode {
		return C.solClient_session_getTxStat(session.pointer, C.solClient_stats_tx_t(stat), (C.solClient_stats_pt)(unsafe.Pointer(&value)))
	})
	if err != nil {
		logging.Default.Warning("Encountered error loading core stat: " + err.GetMessageAsString() + ", subcode " + fmt.Sprint(err.SubCode()))
	}
	return value
}

// SolClientSessionClearStats wraps solClient_session_clearStats
func (session *SolClientSession) SolClientSessionClearStats() *SolClientErrorInfoWrapper {
	return handleCcsmpError(func() SolClientReturnCode {
		return C.solClient_session_clearStats(session.pointer)
	})
}

// SolClientSessionGetClientName wraps solClient_session_getProperty
func (session *SolClientSession) SolClientSessionGetClientName() (string, *SolClientErrorInfoWrapper) {
	const maxClientNameSize = 160
	clientNameKey := C.CString(SolClientSessionPropClientName)
	defer C.free(unsafe.Pointer(clientNameKey))
	clientName := make([]byte, maxClientNameSize)
	errorInfo := handleCcsmpError(func() SolClientReturnCode {
		return C.solClient_session_getProperty(session.pointer, clientNameKey, (*C.char)(unsafe.Pointer(&clientName[0])), maxClientNameSize)
	})
	if errorInfo != nil {
		return "", errorInfo
	}
	endIndex := maxClientNameSize
	for i := 0; i < maxClientNameSize; i++ {
		if clientName[i] == 0 {
			endIndex = i
			break
		}
	}
	return string(clientName[:endIndex]), nil
}

// SolClientSessionGetP2PTopicPrefix wraps solClient_session_getProperty
func (session *SolClientSession) SolClientSessionGetP2PTopicPrefix() (string, *SolClientErrorInfoWrapper) {
	const maxTopicSize = 251 // max topic size including the nul terminal
	p2pTopicInUseKey := C.CString(SolClientSessionPropP2pinboxInUse)
	defer C.free(unsafe.Pointer(p2pTopicInUseKey))
	p2pTopicInUse := make([]byte, maxTopicSize)
	// Get the P2P topic for this session/transport.
	// It is used together with inbox request/reply MEP using
	// native CCSMP inbox
	// Example CCSMP session
	//   P2PINBOX_IN_USE: '#P2P/v:mybroker/mPuoLl8m/myhost/5221/00000001/oWxIwBFz28/#'
	// This only works if the session is connected
	errorInfo := handleCcsmpError(func() SolClientReturnCode {
		return C.solClient_session_getProperty(session.pointer, p2pTopicInUseKey, (*C.char)(unsafe.Pointer(&p2pTopicInUse[0])), maxTopicSize)
	})
	if errorInfo != nil {
		return "", errorInfo
	}
	endIndex := maxTopicSize
	for i := 0; i < maxTopicSize; i++ {
		if p2pTopicInUse[i] == 0 {
			endIndex = i
			break
		}
	}
	// truncate last character '#'
	if endIndex > 0 {
		endIndex = endIndex - 1
		p2pTopicInUse[endIndex] = 0
	}
	return string(p2pTopicInUse[:endIndex]), nil
}

// SolClientVersionGet wraps solClient_version_get
func SolClientVersionGet() (err *SolClientErrorInfoWrapper, version, dateTime, variant string) {
	var versionInfo *SolClientVersionInfo
	err = handleCcsmpError(func() SolClientReturnCode {
		return C.solClient_version_get((*C.solClient_version_info_pt)(&versionInfo))
	})
	if err != nil {
		return err, "", "", ""
	}
	// we don't have to worry about freeing the versionInfo memory since we get a pointer to the real struct
	return nil, C.GoString(versionInfo.version_p), C.GoString(versionInfo.dateTime_p), C.GoString(versionInfo.variant_p)
}

// SolClientVersionSet wraps solClient_version_set
func SolClientVersionSet(version, dateTime, variant string) *SolClientErrorInfoWrapper {
	versionString := C.CString(version)
	dateTimeString := C.CString(dateTime)
	variantString := C.CString(variant)
	defer func() {
		C.free(unsafe.Pointer(versionString))
		C.free(unsafe.Pointer(dateTimeString))
		C.free(unsafe.Pointer(variantString))
	}()
	versionInfo := &SolClientVersionInfo{
		version_p:  versionString,
		dateTime_p: dateTimeString,
		variant_p:  variantString,
	}
	return handleCcsmpError(func() SolClientReturnCode {
		return C._solClient_version_set(versionInfo)
	})
}

// Helpers

// NewSessionDispatch function
func NewSessionDispatch(id uint64) (*SolClientSessionRxMsgDispatchFuncInfo, uintptr) {
	// This is not a misuse of unsafe.Pointer as we are not storing a pointer.
	// CGO defines void* as unsafe.Pointer, however it is just arbitrary data.
	// We want to store a number at void*
	ptr := uintptr(id)
	return &SolClientSessionRxMsgDispatchFuncInfo{
		dispatchType: C.SOLCLIENT_DISPATCH_TYPE_CALLBACK,
		callback_p:   (C.solClient_session_rxMsgCallbackFunc_t)(unsafe.Pointer(C.messageReceiveCallback)),
		user_p:       nil, // this should be set to the uintptr
		rfu_p:        nil,
	}, ptr
}

// NewSessionReplyDispatch function
func NewSessionReplyDispatch(id uint64) uintptr {
	// This is not a misuse of unsafe.Pointer as we are not storing a pointer.
	// CGO defines void* as unsafe.Pointer, however it is just arbitrary data.
	// We want to store a number at void*
	return uintptr(id)
}

// GetLastErrorInfoReturnCodeOnly returns a SolClientErrorInfoWrapper with only the ReturnCode field set.
// This adds a function call on failure paths, but we'd be passing strings around in that case anyways and it should
// happen rarely, so it's fine to slow it down a bit more if it means avoiding code duplication. See this function's
// usage in GetLastErrorInfo() and handleCcsmpError to see where the duplicated code would otherwise have been.
func GetLastErrorInfoReturnCodeOnly(returnCode SolClientReturnCode) *SolClientErrorInfoWrapper {
	errorInfo := &SolClientErrorInfoWrapper{}
	errorInfo.ReturnCode = returnCode
	return errorInfo
}

// GetLastErrorInfo should NOT be called in most cases as it is dependent on the thread.
// Unless you know that the goroutine running the code will not be interrupted, do NOT
// call this function!
func GetLastErrorInfo(returnCode SolClientReturnCode) *SolClientErrorInfoWrapper {
	errorInfo := GetLastErrorInfoReturnCodeOnly(returnCode)
	if returnCode != SolClientReturnCodeNotFound {
		detailedErrorInfo := C.solClient_errorInfo_t{}
		solClientErrorInfoPt := C.solClient_getLastErrorInfo()
		detailedErrorInfo.subCode = solClientErrorInfoPt.subCode
		detailedErrorInfo.responseCode = solClientErrorInfoPt.responseCode
		C.strcpy((*C.char)(&detailedErrorInfo.errorStr[0]), (*C.char)(&solClientErrorInfoPt.errorStr[0]))
		errorInfo.DetailedErrorInfo = &detailedErrorInfo
	}
	return errorInfo
}

// SolClientSubCodeToString converts subcode to string
func SolClientSubCodeToString(subCode SolClientSubCode) string {
	return C.GoString(C.solClient_subCodeToString(subCode))
}

// handleCcsmpError takes a wrapped ccsmp function call and handles the error
// based on the return code. It returns a new SolClientErrorInfoWrapper which
// contains the return code as well as the error info if present, otherwise NULL.
func handleCcsmpError(f func() SolClientReturnCode) *SolClientErrorInfoWrapper {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	returnCode := f()
	if returnCode == SolClientReturnCodeFail || returnCode == SolClientReturnCodeNotReady {
		// Return full error struct if rc requires additional error info.
		return GetLastErrorInfo(returnCode)
	} else if returnCode != SolClientReturnCodeOk && returnCode != SolClientReturnCodeInProgress {
		// Return partial error if not ok but not failure so that caller can parse on rc
		return GetLastErrorInfoReturnCodeOnly(returnCode)
	}
	return nil
}

func (session *SolClientSession) SolClientModifySessionProperties(properties []string) *SolClientErrorInfoWrapper {
	sessionPropsP, sessionPropertiesFreeFunction := ToCArray(properties, true)
	defer sessionPropertiesFreeFunction()

	solClientErrorInfo := handleCcsmpError(func() SolClientReturnCode {
		return C.solClient_session_modifyProperties(session.pointer, sessionPropsP)
	})
	// If there was an error, return the error
	if solClientErrorInfo != nil {
		return solClientErrorInfo
	}
	// If there was no error, return nil
	return nil
}
