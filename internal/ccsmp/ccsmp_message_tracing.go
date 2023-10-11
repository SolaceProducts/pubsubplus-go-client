// pubsubplus-go-client
//
// Copyright 2021-2023 Solace Corporation. All rights reserved.
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

package ccsmp

/*
#include <stdlib.h>
#include <string.h>

#include "solclient/solClient.h"
#include "solclient/solClientMsg.h"
#include "solclient/solClientMsgTracingSupport.h"
*/
import "C"
import (
	"fmt"
	"unsafe"

	"solace.dev/go/messaging/internal/impl/logging"
)

// Reexport of various CCSMP types

// SolClientMessageTracingContextType is assigned a value
type SolClientMessageTracingContextType = C.solClient_msg_tracing_context_type_t

// SolClientMessageTracingInjectionStandardType is assigned a value
type SolClientMessageTracingInjectionStandardType = C.solClient_msg_tracing_injection_standard_type_t

// SolClientContextTypeTransportContext is assigned a value
const SolClientContextTypeTransportContext = C.TRANSPORT_CONTEXT

// SolClientContextTypeCreationContext is assigned a value
const SolClientContextTypeCreationContext = C.CREATION_CONTEXT

// SolClientMessageTracingInjectionStandardTypeSMF is assigned a value
const SolClientMessageTracingInjectionStandardTypeSMF = C.SOLCLIENT_INJECTION_STANDARD_SMF

// SolClientMessageTracingInjectionStandardTypeW3C is assigned a value
const SolClientMessageTracingInjectionStandardTypeW3C = C.SOLCLIENT_INJECTION_STANDARD_W3C

// TODO the calls to handleCcsmpError are slow since they lock the thread.
// Ideally, we wrap these calls in C such that the golang scheduler cannot
// interrupt us, and then there is no need to lock the thread. This should
// be done for all datapath functionality, ie. the contents of this file.

// Distributed tracing properties

// SolClientMessageGetTraceContextTraceID function
func SolClientMessageGetTraceContextTraceID(messageP SolClientMessagePt, contextType SolClientMessageTracingContextType) ([16]byte, *SolClientErrorInfoWrapper) {
	// to hold the traceID property
	var cChar C.solClient_uint8_t

	errorInfo := handleCcsmpError(func() SolClientReturnCode {
		return C.solClient_msg_tracing_getTraceIdByte(messageP, contextType, &cChar, C.size_t(16))
	})
	if errorInfo != nil {
		if errorInfo.ReturnCode == SolClientReturnCodeFail {
			logging.Default.Warning(fmt.Sprintf("Encountered error fetching Creation context traceID prop: %s, subcode: %d", errorInfo.GetMessageAsString(), errorInfo.SubCode))
		}
		return [16]byte{}, errorInfo
	}

	traceID := *(*[16]byte)(unsafe.Pointer(&cChar))
	return traceID, errorInfo
}

// SolClientMessageSetTraceContextTraceID function
func SolClientMessageSetTraceContextTraceID(messageP SolClientMessagePt, traceID [16]byte, contextType SolClientMessageTracingContextType) *SolClientErrorInfoWrapper {
	if len(traceID) > 0 {
		cTraceID := (*C.solClient_uint8_t)(C.CBytes(traceID[:]))

		defer C.free(unsafe.Pointer(cTraceID)) // free the pointer after function executes

		errorInfo := handleCcsmpError(func() SolClientReturnCode {
			return C.solClient_msg_tracing_setTraceIdByte(messageP, contextType, cTraceID, C.size_t(len(traceID)))
		})
		return errorInfo
	}
	return nil
}

// SolClientMessageGetTraceContextSpanID function
func SolClientMessageGetTraceContextSpanID(messageP SolClientMessagePt, contextType SolClientMessageTracingContextType) ([8]byte, *SolClientErrorInfoWrapper) {
	// to hold the spanID property
	var cChar C.solClient_uint8_t

	errorInfo := handleCcsmpError(func() SolClientReturnCode {
		return C.solClient_msg_tracing_getSpanIdByte(messageP, contextType, &cChar, C.size_t(8))
	})
	if errorInfo != nil {
		if errorInfo.ReturnCode == SolClientReturnCodeFail {
			logging.Default.Warning(fmt.Sprintf("Encountered error fetching Creation context spanID prop: %s, subcode: %d", errorInfo.GetMessageAsString(), errorInfo.SubCode))
		}
		return [8]byte{}, errorInfo
	}

	spanID := *(*[8]byte)(unsafe.Pointer(&cChar))
	return spanID, errorInfo
}

// SolClientMessageSetTraceContextSpanID function
func SolClientMessageSetTraceContextSpanID(messageP SolClientMessagePt, spanID [8]byte, contextType SolClientMessageTracingContextType) *SolClientErrorInfoWrapper {
	if len(spanID) > 0 {
		cSpanID := (*C.solClient_uint8_t)(C.CBytes(spanID[:]))

		defer C.free(unsafe.Pointer(cSpanID)) // free the pointer after function executes

		errorInfo := handleCcsmpError(func() SolClientReturnCode {
			return C.solClient_msg_tracing_setSpanIdByte(messageP, contextType, cSpanID, C.size_t(len(spanID)))
		})
		return errorInfo
	}
	return nil
}

// SolClientMessageGetTraceContextSampled function
func SolClientMessageGetTraceContextSampled(messageP SolClientMessagePt, contextType SolClientMessageTracingContextType) (bool, *SolClientErrorInfoWrapper) {
	// to hold the Sampled property
	var cSampled C.solClient_bool_t

	errorInfo := handleCcsmpError(func() SolClientReturnCode {
		return C.solClient_msg_tracing_isSampled(messageP, contextType, &cSampled)
	})
	if errorInfo != nil {
		if errorInfo.ReturnCode == SolClientReturnCodeFail {
			logging.Default.Warning(fmt.Sprintf("Encountered error fetching Creation context sampled prop: %s, subcode: %d", errorInfo.GetMessageAsString(), errorInfo.SubCode))
		}
		return false, errorInfo
	}

	isSampled := *(*bool)(unsafe.Pointer(&cSampled))
	return isSampled, errorInfo
}

// SolClientMessageSetTraceContextSampled function
func SolClientMessageSetTraceContextSampled(messageP SolClientMessagePt, sampled bool, contextType SolClientMessageTracingContextType) *SolClientErrorInfoWrapper {
	var isSampled C.solClient_bool_t = 0
	if sampled {
		isSampled = 1
	}
	return handleCcsmpError(func() SolClientReturnCode {
		return C.solClient_msg_tracing_setSampled(messageP, contextType, isSampled)
	})
}

// SolClientMessageGetTraceContextTraceState function
func SolClientMessageGetTraceContextTraceState(messageP SolClientMessagePt, contextType SolClientMessageTracingContextType) (string, *SolClientErrorInfoWrapper) {
	// to hold the trace state
	var traceStateChar *C.char
	var traceStateSize C.size_t

	errorInfo := handleCcsmpError(func() SolClientReturnCode {
		return C.solClient_msg_tracing_getTraceStatePtr(messageP, contextType, &traceStateChar, &traceStateSize)
	})
	if errorInfo != nil {
		if errorInfo.ReturnCode == SolClientReturnCodeFail {
			logging.Default.Warning(fmt.Sprintf("Encountered error fetching Creation contex traceState prop: %s, subcode: %d", errorInfo.GetMessageAsString(), errorInfo.SubCode))
		}
		return "", errorInfo
	}

	return C.GoStringN(traceStateChar, C.int(traceStateSize)), errorInfo
}

// SolClientMessageSetTraceContextTraceState function
func SolClientMessageSetTraceContextTraceState(messageP SolClientMessagePt, traceState string, contextType SolClientMessageTracingContextType) *SolClientErrorInfoWrapper {
	cStr := C.CString(traceState)
	defer C.free(unsafe.Pointer(cStr)) // free the pointer after function executes
	errorInfo := handleCcsmpError(func() SolClientReturnCode {
		return C.solClient_msg_tracing_setTraceState(messageP, contextType, cStr)
	})
	return errorInfo
}

// For the Creation Context

// SolClientMessageGetCreationTraceContextTraceID function
func SolClientMessageGetCreationTraceContextTraceID(messageP SolClientMessagePt) ([16]byte, *SolClientErrorInfoWrapper) {
	// return the traceID property for the creation trace context
	return SolClientMessageGetTraceContextTraceID(messageP, SolClientContextTypeCreationContext)
}

// SolClientMessageSetCreationTraceContextTraceID function
func SolClientMessageSetCreationTraceContextTraceID(messageP SolClientMessagePt, traceID [16]byte) *SolClientErrorInfoWrapper {
	// Sets the traceID property for the creation trace context
	return SolClientMessageSetTraceContextTraceID(messageP, traceID, SolClientContextTypeCreationContext)
}

// SolClientMessageGetCreationTraceContextSpanID function
func SolClientMessageGetCreationTraceContextSpanID(messageP SolClientMessagePt) ([8]byte, *SolClientErrorInfoWrapper) {
	// return the spanID property for the creation trace context
	return SolClientMessageGetTraceContextSpanID(messageP, SolClientContextTypeCreationContext)
}

// SolClientMessageSetCreationTraceContextSpanID function
func SolClientMessageSetCreationTraceContextSpanID(messageP SolClientMessagePt, spanID [8]byte) *SolClientErrorInfoWrapper {
	// Sets the spanID property for the creation trace context
	return SolClientMessageSetTraceContextSpanID(messageP, spanID, SolClientContextTypeCreationContext)
}

// SolClientMessageGetCreationTraceContextSampled function
func SolClientMessageGetCreationTraceContextSampled(messageP SolClientMessagePt) (bool, *SolClientErrorInfoWrapper) {
	// return the Sampled property for the creation trace context
	return SolClientMessageGetTraceContextSampled(messageP, SolClientContextTypeCreationContext)
}

// SolClientMessageSetCreationTraceContextSampled function
func SolClientMessageSetCreationTraceContextSampled(messageP SolClientMessagePt, sampled bool) *SolClientErrorInfoWrapper {
	// Sets the Sampled property for the creation trace context
	return SolClientMessageSetTraceContextSampled(messageP, sampled, SolClientContextTypeCreationContext)
}

// SolClientMessageGetCreationTraceContextTraceState function
func SolClientMessageGetCreationTraceContextTraceState(messageP SolClientMessagePt) (string, *SolClientErrorInfoWrapper) {
	// return the trace state property for the creation trace context
	return SolClientMessageGetTraceContextTraceState(messageP, SolClientContextTypeCreationContext)
}

// SolClientMessageSetCreationTraceContextTraceState function
func SolClientMessageSetCreationTraceContextTraceState(messageP SolClientMessagePt, traceState string) *SolClientErrorInfoWrapper {
	// Sets the trace state property for the creation trace context
	return SolClientMessageSetTraceContextTraceState(messageP, traceState, SolClientContextTypeCreationContext)
}

// For the Transport Context

// SolClientMessageGetTransportTraceContextTraceID function
func SolClientMessageGetTransportTraceContextTraceID(messageP SolClientMessagePt) ([16]byte, *SolClientErrorInfoWrapper) {
	// return the traceID property for the transport trace context
	return SolClientMessageGetTraceContextTraceID(messageP, SolClientContextTypeTransportContext)
}

// SolClientMessageSetTransportTraceContextTraceID function
func SolClientMessageSetTransportTraceContextTraceID(messageP SolClientMessagePt, traceID [16]byte) *SolClientErrorInfoWrapper {
	// Sets the traceID property for the transport trace context
	return SolClientMessageSetTraceContextTraceID(messageP, traceID, SolClientContextTypeTransportContext)
}

// SolClientMessageGetTransportTraceContextSpanID function
func SolClientMessageGetTransportTraceContextSpanID(messageP SolClientMessagePt) ([8]byte, *SolClientErrorInfoWrapper) {
	// return the spanID property for the transport trace context
	return SolClientMessageGetTraceContextSpanID(messageP, SolClientContextTypeTransportContext)
}

// SolClientMessageSetTransportTraceContextSpanID function
func SolClientMessageSetTransportTraceContextSpanID(messageP SolClientMessagePt, spanID [8]byte) *SolClientErrorInfoWrapper {
	// Sets the spanID property for the transport trace context
	return SolClientMessageSetTraceContextSpanID(messageP, spanID, SolClientContextTypeTransportContext)
}

// SolClientMessageGetTransportTraceContextSampled function
func SolClientMessageGetTransportTraceContextSampled(messageP SolClientMessagePt) (bool, *SolClientErrorInfoWrapper) {
	// return the Sampled property for the transport trace context
	return SolClientMessageGetTraceContextSampled(messageP, SolClientContextTypeTransportContext)
}

// SolClientMessageSetTransportTraceContextSampled function
func SolClientMessageSetTransportTraceContextSampled(messageP SolClientMessagePt, sampled bool) *SolClientErrorInfoWrapper {
	// Sets the Sampled property for the transport trace context
	return SolClientMessageSetTraceContextSampled(messageP, sampled, SolClientContextTypeTransportContext)
}

// SolClientMessageGetTransportTraceContextTraceState function
func SolClientMessageGetTransportTraceContextTraceState(messageP SolClientMessagePt) (string, *SolClientErrorInfoWrapper) {
	// return the trace state property for the transport trace context
	return SolClientMessageGetTraceContextTraceState(messageP, SolClientContextTypeTransportContext)
}

// SolClientMessageSetTransportTraceContextTraceState function
func SolClientMessageSetTransportTraceContextTraceState(messageP SolClientMessagePt, traceState string) *SolClientErrorInfoWrapper {
	// Sets the trace state property for the transport trace context
	return SolClientMessageSetTraceContextTraceState(messageP, traceState, SolClientContextTypeTransportContext)
}

// For the Baggage

// SolClientMessageGetBaggage function
func SolClientMessageGetBaggage(messageP SolClientMessagePt) (string, *SolClientErrorInfoWrapper) {
	var baggageChar *C.char
	var baggageSize C.size_t
	errorInfo := handleCcsmpError(func() SolClientReturnCode {
		return C.solClient_msg_tracing_getBaggagePtr(messageP, &baggageChar, &baggageSize)
	})
	if errorInfo != nil {
		if errorInfo.ReturnCode == SolClientReturnCodeFail {
			logging.Default.Warning(fmt.Sprintf("Encountered error fetching baggage: %s, subcode: %d", errorInfo.GetMessageAsString(), errorInfo.SubCode))
		}
		return "", errorInfo
	}

	// use baggageSize - 1 to exclude the null character at the end of the baggage string
	return C.GoStringN(baggageChar, C.int(baggageSize)-1), errorInfo
}

// SolClientMessageSetBaggage function
func SolClientMessageSetBaggage(messageP SolClientMessagePt, baggage string) *SolClientErrorInfoWrapper {
	cStr := C.CString(baggage)
	defer C.free(unsafe.Pointer(cStr)) // free the pointer after function executes
	errorInfo := handleCcsmpError(func() SolClientReturnCode {
		return C.solClient_msg_tracing_setBaggage(messageP, cStr)
	})
	return errorInfo
}
