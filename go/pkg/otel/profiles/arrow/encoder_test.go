// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package arrow

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pprofile"

	arrowpb "github.com/open-telemetry/otel-arrow/go/api/experimental/arrow/v1"
)

func TestEncodeProfilesPayloadFamily(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer pool.AssertSize(t, 0)

	data := pprofile.NewProfiles()
	dictionary := data.Dictionary()
	dictionary.StringTable().FromRaw([]string{"", "cpu", "ns", "main", "main.go", "service.name"})
	stack := dictionary.StackTable().AppendEmpty()
	stack.LocationIndices().FromRaw([]int32{0})
	function := dictionary.FunctionTable().AppendEmpty()
	function.SetNameStrindex(3)
	function.SetFilenameStrindex(4)
	location := dictionary.LocationTable().AppendEmpty()
	location.SetMappingIndex(0)
	location.Lines().AppendEmpty().SetFunctionIndex(0)
	mapping := dictionary.MappingTable().AppendEmpty()
	mapping.SetFilenameStrindex(4)
	link := dictionary.LinkTable().AppendEmpty()
	link.SetTraceID(pcommon.TraceID{1})
	link.SetSpanID(pcommon.SpanID{2})
	attribute := dictionary.AttributeTable().AppendEmpty()
	attribute.SetKeyStrindex(5)
	attribute.Value().SetStr("checkout")

	resourceProfiles := data.ResourceProfiles().AppendEmpty()
	resourceProfiles.Resource().Attributes().PutStr("service.name", "checkout")
	scopeProfiles := resourceProfiles.ScopeProfiles().AppendEmpty()
	scopeProfiles.Scope().SetName("test-scope")
	profile := scopeProfiles.Profiles().AppendEmpty()
	profile.SampleType().SetTypeStrindex(1)
	profile.SampleType().SetUnitStrindex(2)
	profile.SetProfileID(pprofile.ProfileID{3})
	profile.SetTime(pcommon.Timestamp(123))
	profile.AttributeIndices().FromRaw([]int32{0})
	sample := profile.Samples().AppendEmpty()
	sample.SetStackIndex(0)
	sample.SetLinkIndex(0)
	sample.Values().FromRaw([]int64{7})
	sample.AttributeIndices().FromRaw([]int32{0})
	sample.TimestampsUnixNano().FromRaw([]uint64{123})

	messages, err := Encode(pool, data)
	require.NoError(t, err)
	defer func() {
		for _, message := range messages {
			message.Record().Release()
		}
	}()

	seen := map[arrowpb.ArrowPayloadType]int64{}
	for _, message := range messages {
		seen[message.PayloadType()] = message.Record().NumRows()
	}
	require.Equal(t, int64(1), seen[arrowpb.ArrowPayloadType_PROFILES])
	require.Equal(t, int64(1), seen[arrowpb.ArrowPayloadType_SAMPLE])
	require.Equal(t, int64(1), seen[arrowpb.ArrowPayloadType_RESOURCE_ATTRS])
	require.Equal(t, int64(1), seen[arrowpb.ArrowPayloadType_MAPPING_TABLE])
	require.Equal(t, int64(1), seen[arrowpb.ArrowPayloadType_LOCATION_TABLE])
	require.Equal(t, int64(1), seen[arrowpb.ArrowPayloadType_FUNCTION_TABLE])
	require.Equal(t, int64(1), seen[arrowpb.ArrowPayloadType_LINK_TABLE])
	require.Equal(t, int64(6), seen[arrowpb.ArrowPayloadType_STRING_TABLE])
	require.Equal(t, int64(1), seen[arrowpb.ArrowPayloadType_ATTRIBUTE_TABLE])
	require.Equal(t, int64(1), seen[arrowpb.ArrowPayloadType_ATTRIBUTE_UNITS])
}

func TestEncodeRejectsInvalidStackIndex(t *testing.T) {
	data := pprofile.NewProfiles()
	profile := data.ResourceProfiles().AppendEmpty().ScopeProfiles().AppendEmpty().Profiles().AppendEmpty()
	profile.Samples().AppendEmpty().SetStackIndex(4)

	messages, err := Encode(memory.DefaultAllocator, data)
	require.ErrorContains(t, err, "outside table length")
	require.Nil(t, messages)
}
