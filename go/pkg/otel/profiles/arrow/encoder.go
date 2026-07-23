// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

// Package arrow encodes the Collector's dictionary-native profiles pdata as
// the profile payload family carried by OTAP.
package arrow

import (
	"fmt"
	"math"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pprofile"

	arrowpb "github.com/open-telemetry/otel-arrow/go/api/experimental/arrow/v1"
	"github.com/open-telemetry/otel-arrow/go/pkg/otel/common"
	"github.com/open-telemetry/otel-arrow/go/pkg/record_message"
)

const (
	encodingKey   = "encoding"
	plainEncoding = "plain"
)

func plainField(name string, dataType arrow.DataType, nullable bool) arrow.Field {
	return arrow.Field{
		Name:     name,
		Type:     dataType,
		Nullable: nullable,
		Metadata: arrow.NewMetadata([]string{encodingKey}, []string{plainEncoding}),
	}
}

func field(name string, dataType arrow.DataType, nullable bool) arrow.Field {
	return arrow.Field{Name: name, Type: dataType, Nullable: nullable}
}

func record(pool memory.Allocator, fields []arrow.Field, columns []arrow.Array) (arrow.Record, error) {
	schema := arrow.NewSchema(fields, nil)
	rec := array.NewRecord(schema, columns, int64(columns[0].Len()))
	for _, column := range columns {
		column.Release()
	}
	return rec, nil
}

func uint16Array(pool memory.Allocator, values []uint16) arrow.Array {
	b := array.NewUint16Builder(pool)
	defer b.Release()
	b.AppendValues(values, nil)
	return b.NewArray()
}

func uint32Array(pool memory.Allocator, values []uint32) arrow.Array {
	b := array.NewUint32Builder(pool)
	defer b.Release()
	b.AppendValues(values, nil)
	return b.NewArray()
}

func int32Array(pool memory.Allocator, values []int32) arrow.Array {
	b := array.NewInt32Builder(pool)
	defer b.Release()
	b.AppendValues(values, nil)
	return b.NewArray()
}

func int64Array(pool memory.Allocator, values []int64) arrow.Array {
	b := array.NewInt64Builder(pool)
	defer b.Release()
	b.AppendValues(values, nil)
	return b.NewArray()
}

func uint64Array(pool memory.Allocator, values []uint64) arrow.Array {
	b := array.NewUint64Builder(pool)
	defer b.Release()
	b.AppendValues(values, nil)
	return b.NewArray()
}

func boolArray(pool memory.Allocator, values []bool) arrow.Array {
	b := array.NewBooleanBuilder(pool)
	defer b.Release()
	b.AppendValues(values, nil)
	return b.NewArray()
}

func stringArray(pool memory.Allocator, values []string) arrow.Array {
	b := array.NewStringBuilder(pool)
	defer b.Release()
	b.AppendValues(values, nil)
	return b.NewArray()
}

func binaryArray(pool memory.Allocator, values [][]byte) arrow.Array {
	b := array.NewBinaryBuilder(pool, arrow.BinaryTypes.Binary)
	defer b.Release()
	for _, value := range values {
		b.Append(value)
	}
	return b.NewArray()
}

func fixedBinaryArray(pool memory.Allocator, width int, values [][]byte) arrow.Array {
	b := array.NewFixedSizeBinaryBuilder(pool, &arrow.FixedSizeBinaryType{ByteWidth: width})
	defer b.Release()
	for _, value := range values {
		if len(value) == width {
			b.Append(value)
		} else {
			b.AppendNull()
		}
	}
	return b.NewArray()
}

func int32Lists(pool memory.Allocator, values [][]int32) arrow.Array {
	b := array.NewListBuilder(pool, arrow.PrimitiveTypes.Int32)
	defer b.Release()
	v := b.ValueBuilder().(*array.Int32Builder)
	for _, list := range values {
		b.Append(true)
		v.AppendValues(list, nil)
	}
	return b.NewArray()
}

func int64Lists(pool memory.Allocator, values [][]int64) arrow.Array {
	b := array.NewListBuilder(pool, arrow.PrimitiveTypes.Int64)
	defer b.Release()
	v := b.ValueBuilder().(*array.Int64Builder)
	for _, list := range values {
		b.Append(true)
		v.AppendValues(list, nil)
	}
	return b.NewArray()
}

func uint64Lists(pool memory.Allocator, values [][]uint64) arrow.Array {
	b := array.NewListBuilder(pool, arrow.PrimitiveTypes.Uint64)
	defer b.Release()
	v := b.ValueBuilder().(*array.Uint64Builder)
	for _, list := range values {
		b.Append(true)
		v.AppendValues(list, nil)
	}
	return b.NewArray()
}

func makeMessage(payloadType arrowpb.ArrowPayloadType, rec arrow.Record) *record_message.RecordMessage {
	return record_message.NewRelatedDataMessage(
		fmt.Sprintf("profiles/%d/%s", payloadType, rec.Schema().String()),
		rec,
		payloadType,
	)
}

// Encode produces the complete OTAP profile payload family. Lookup-table row
// order is preserved because every dictionary index is a positional identity.
func Encode(pool memory.Allocator, profiles pprofile.Profiles) ([]*record_message.RecordMessage, error) {
	if profiles.ProfileCount() > math.MaxUint16 {
		return nil, fmt.Errorf("profile count %d exceeds OTAP uint16 identity space", profiles.ProfileCount())
	}

	root, samples, err := encodeProfiles(pool, profiles)
	if err != nil {
		return nil, err
	}
	messages := []*record_message.RecordMessage{
		makeMessage(arrowpb.ArrowPayloadType_PROFILES, root),
	}
	if samples.NumRows() != 0 {
		messages = append(messages, makeMessage(arrowpb.ArrowPayloadType_SAMPLE, samples))
	} else {
		samples.Release()
	}
	resourceAttrs, scopeAttrs, err := encodeResourceScopeAttrs(pool, profiles)
	if err != nil {
		for _, message := range messages {
			message.Record().Release()
		}
		return nil, err
	}
	if resourceAttrs.NumRows() != 0 {
		messages = append(messages, makeMessage(arrowpb.ArrowPayloadType_RESOURCE_ATTRS, resourceAttrs))
	} else {
		resourceAttrs.Release()
	}
	if scopeAttrs.NumRows() != 0 {
		messages = append(messages, makeMessage(arrowpb.ArrowPayloadType_SCOPE_ATTRS, scopeAttrs))
	} else {
		scopeAttrs.Release()
	}

	dictionary := profiles.Dictionary()
	tableEncoders := []struct {
		payload arrowpb.ArrowPayloadType
		encode  func() (arrow.Record, error)
	}{
		{arrowpb.ArrowPayloadType_MAPPING_TABLE, func() (arrow.Record, error) {
			return encodeMappings(pool, dictionary)
		}},
		{arrowpb.ArrowPayloadType_LOCATION_TABLE, func() (arrow.Record, error) {
			return encodeLocations(pool, dictionary)
		}},
		{arrowpb.ArrowPayloadType_FUNCTION_TABLE, func() (arrow.Record, error) {
			return encodeFunctions(pool, dictionary)
		}},
		{arrowpb.ArrowPayloadType_LINK_TABLE, func() (arrow.Record, error) {
			return encodeLinks(pool, dictionary)
		}},
		{arrowpb.ArrowPayloadType_STRING_TABLE, func() (arrow.Record, error) {
			return encodeStrings(pool, dictionary)
		}},
		{arrowpb.ArrowPayloadType_ATTRIBUTE_TABLE, func() (arrow.Record, error) {
			return encodeAttributeTable(pool, dictionary)
		}},
		{arrowpb.ArrowPayloadType_ATTRIBUTE_UNITS, func() (arrow.Record, error) {
			return encodeAttributeUnits(pool, dictionary)
		}},
	}
	for _, table := range tableEncoders {
		rec, encodeErr := table.encode()
		if encodeErr != nil {
			for _, message := range messages {
				message.Record().Release()
			}
			return nil, encodeErr
		}
		if rec.NumRows() == 0 {
			rec.Release()
			continue
		}
		messages = append(messages, makeMessage(table.payload, rec))
	}
	return messages, nil
}

func encodeProfiles(pool memory.Allocator, data pprofile.Profiles) (arrow.Record, arrow.Record, error) {
	var (
		ids, resourceIDs, scopeIDs                             []uint16
		resourceDropped, scopeDropped, dropped                 []uint32
		resourceSchemas, scopeNames, scopeVersions, schemaURLs []string
		times, durations, periods                              []int64
		profileIDs, originalPayloads                           [][]byte
		originalFormats                                        []string
		sampleTypes, locations, comments, attributes           [][]int32
		sampleParents                                          []uint16
		sampleStarts, sampleLengths, sampleLinks               []int32
		sampleValues                                           [][]int64
		sampleAttributes                                       [][]int32
		sampleTimestamps                                       [][]uint64
	)
	var profileID uint16
	var resourceID, scopeID uint16
	dictionary := data.Dictionary()
	for ri := 0; ri < data.ResourceProfiles().Len(); ri++ {
		rp := data.ResourceProfiles().At(ri)
		for si := 0; si < rp.ScopeProfiles().Len(); si++ {
			sp := rp.ScopeProfiles().At(si)
			for pi := 0; pi < sp.Profiles().Len(); pi++ {
				profile := sp.Profiles().At(pi)
				ids = append(ids, profileID)
				resourceIDs = append(resourceIDs, resourceID)
				resourceDropped = append(resourceDropped, rp.Resource().DroppedAttributesCount())
				resourceSchemas = append(resourceSchemas, rp.SchemaUrl())
				scopeIDs = append(scopeIDs, scopeID)
				scopeDropped = append(scopeDropped, sp.Scope().DroppedAttributesCount())
				scopeNames = append(scopeNames, sp.Scope().Name())
				scopeVersions = append(scopeVersions, sp.Scope().Version())
				schemaURLs = append(schemaURLs, sp.SchemaUrl())
				times = append(times, int64(profile.Time()))
				if profile.DurationNano() > math.MaxInt64 {
					return nil, nil, fmt.Errorf("profile duration %d exceeds OTAP int64", profile.DurationNano())
				}
				durations = append(durations, int64(profile.DurationNano()))
				periods = append(periods, profile.Period())
				id := profile.ProfileID()
				profileIDs = append(profileIDs, append([]byte(nil), id[:]...))
				dropped = append(dropped, profile.DroppedAttributesCount())
				originalFormats = append(originalFormats, profile.OriginalPayloadFormat())
				originalPayloads = append(originalPayloads, append([]byte(nil), profile.OriginalPayload().AsRaw()...))
				sampleTypes = append(sampleTypes, []int32{
					profile.SampleType().TypeStrindex(),
					profile.SampleType().UnitStrindex(),
				})
				locations = append(locations, nil)
				comments = append(comments, nil)
				attributes = append(attributes, profile.AttributeIndices().AsRaw())

				for sj := 0; sj < profile.Samples().Len(); sj++ {
					sample := profile.Samples().At(sj)
					stackIndex := int(sample.StackIndex())
					if stackIndex < 0 || stackIndex >= dictionary.StackTable().Len() {
						return nil, nil, fmt.Errorf("sample stack index %d outside table length %d", stackIndex, dictionary.StackTable().Len())
					}
					stackLocations := dictionary.StackTable().At(stackIndex).LocationIndices().AsRaw()
					start := len(locations[len(locations)-1])
					if start > math.MaxInt32 || len(stackLocations) > math.MaxInt32 {
						return nil, nil, fmt.Errorf("profile stack locations exceed OTAP int32 range")
					}
					locations[len(locations)-1] = append(locations[len(locations)-1], stackLocations...)
					sampleParents = append(sampleParents, profileID)
					sampleStarts = append(sampleStarts, int32(start))
					sampleLengths = append(sampleLengths, int32(len(stackLocations)))
					sampleLinks = append(sampleLinks, sample.LinkIndex())
					sampleValues = append(sampleValues, sample.Values().AsRaw())
					sampleAttributes = append(sampleAttributes, sample.AttributeIndices().AsRaw())
					sampleTimestamps = append(sampleTimestamps, sample.TimestampsUnixNano().AsRaw())
				}
				profileID++
			}
			scopeID++
		}
		resourceID++
	}

	resourceType := arrow.StructOf(
		plainField("id", arrow.PrimitiveTypes.Uint16, true),
		field("dropped_attributes_count", arrow.PrimitiveTypes.Uint32, true),
		field("schema_url", arrow.BinaryTypes.String, true),
	)
	resourceBuilder := array.NewStructBuilder(pool, resourceType)
	for i := range resourceIDs {
		resourceBuilder.Append(true)
		resourceBuilder.FieldBuilder(0).(*array.Uint16Builder).Append(resourceIDs[i])
		resourceBuilder.FieldBuilder(1).(*array.Uint32Builder).Append(resourceDropped[i])
		resourceBuilder.FieldBuilder(2).(*array.StringBuilder).Append(resourceSchemas[i])
	}
	resourceArray := resourceBuilder.NewArray()
	resourceBuilder.Release()

	scopeType := arrow.StructOf(
		plainField("id", arrow.PrimitiveTypes.Uint16, true),
		field("dropped_attributes_count", arrow.PrimitiveTypes.Uint32, true),
		field("name", arrow.BinaryTypes.String, true),
		field("version", arrow.BinaryTypes.String, true),
	)
	scopeBuilder := array.NewStructBuilder(pool, scopeType)
	for i := range scopeIDs {
		scopeBuilder.Append(true)
		scopeBuilder.FieldBuilder(0).(*array.Uint16Builder).Append(scopeIDs[i])
		scopeBuilder.FieldBuilder(1).(*array.Uint32Builder).Append(scopeDropped[i])
		scopeBuilder.FieldBuilder(2).(*array.StringBuilder).Append(scopeNames[i])
		scopeBuilder.FieldBuilder(3).(*array.StringBuilder).Append(scopeVersions[i])
	}
	scopeArray := scopeBuilder.NewArray()
	scopeBuilder.Release()

	rootFields := []arrow.Field{
		plainField("id", arrow.PrimitiveTypes.Uint16, true),
		field("resource", resourceType, true),
		field("scope", scopeType, true),
		field("schema_url", arrow.BinaryTypes.String, true),
		field("time_nanos", &arrow.TimestampType{Unit: arrow.Nanosecond}, true),
		field("duration_nanos", arrow.PrimitiveTypes.Int64, true),
		field("period", arrow.PrimitiveTypes.Int64, true),
		field("profile_id", &arrow.FixedSizeBinaryType{ByteWidth: 16}, true),
		field("dropped_attributes_count", arrow.PrimitiveTypes.Uint32, true),
		field("original_payload_format", arrow.BinaryTypes.String, true),
		field("original_payload", arrow.BinaryTypes.Binary, true),
		field("sample_type", arrow.ListOf(arrow.StructOf(
			field("type_strindex", arrow.PrimitiveTypes.Int32, false),
			field("unit_strindex", arrow.PrimitiveTypes.Int32, false),
			field("aggregation_temporality", arrow.PrimitiveTypes.Int32, false),
		)), false),
		field("location_indices", arrow.ListOf(arrow.PrimitiveTypes.Int32), false),
		field("comment_strindices", arrow.ListOf(arrow.PrimitiveTypes.Int32), false),
		field("attribute_indices", arrow.ListOf(arrow.PrimitiveTypes.Int32), false),
	}
	timeBuilder := array.NewTimestampBuilder(pool, &arrow.TimestampType{Unit: arrow.Nanosecond})
	timeBuilder.AppendValues(func() []arrow.Timestamp {
		out := make([]arrow.Timestamp, len(times))
		for i, value := range times {
			out[i] = arrow.Timestamp(value)
		}
		return out
	}(), nil)
	timeArray := timeBuilder.NewArray()
	timeBuilder.Release()

	sampleTypeBuilder := array.NewListBuilder(pool, rootFields[11].Type.(*arrow.ListType).Elem())
	sampleStruct := sampleTypeBuilder.ValueBuilder().(*array.StructBuilder)
	for _, sampleType := range sampleTypes {
		sampleTypeBuilder.Append(true)
		sampleStruct.Append(true)
		sampleStruct.FieldBuilder(0).(*array.Int32Builder).Append(sampleType[0])
		sampleStruct.FieldBuilder(1).(*array.Int32Builder).Append(sampleType[1])
		sampleStruct.FieldBuilder(2).(*array.Int32Builder).Append(0)
	}
	sampleTypeArray := sampleTypeBuilder.NewArray()
	sampleTypeBuilder.Release()

	root, err := record(pool, rootFields, []arrow.Array{
		uint16Array(pool, ids), resourceArray, scopeArray, stringArray(pool, schemaURLs),
		timeArray, int64Array(pool, durations), int64Array(pool, periods),
		fixedBinaryArray(pool, 16, profileIDs), uint32Array(pool, dropped),
		stringArray(pool, originalFormats), binaryArray(pool, originalPayloads),
		sampleTypeArray, int32Lists(pool, locations), int32Lists(pool, comments),
		int32Lists(pool, attributes),
	})
	if err != nil {
		return nil, nil, err
	}
	sample, err := record(pool, []arrow.Field{
		plainField("parent_id", arrow.PrimitiveTypes.Uint16, false),
		field("locations_start_index", arrow.PrimitiveTypes.Int32, true),
		field("locations_length", arrow.PrimitiveTypes.Int32, true),
		field("value", arrow.ListOf(arrow.PrimitiveTypes.Int64), false),
		field("attribute_indices", arrow.ListOf(arrow.PrimitiveTypes.Int32), false),
		field("link_index", arrow.PrimitiveTypes.Int32, true),
		field("timestamps_unix_nano", arrow.ListOf(arrow.PrimitiveTypes.Uint64), false),
	}, []arrow.Array{
		uint16Array(pool, sampleParents), int32Array(pool, sampleStarts),
		int32Array(pool, sampleLengths), int64Lists(pool, sampleValues),
		int32Lists(pool, sampleAttributes), int32Array(pool, sampleLinks),
		uint64Lists(pool, sampleTimestamps),
	})
	if err != nil {
		root.Release()
		return nil, nil, err
	}
	return root, sample, nil
}

type attributeRow struct {
	parent uint16
	key    string
	value  pcommon.Value
}

func encodeResourceScopeAttrs(pool memory.Allocator, data pprofile.Profiles) (arrow.Record, arrow.Record, error) {
	var resources, scopes []attributeRow
	var resourceID, scopeID uint16
	for ri := 0; ri < data.ResourceProfiles().Len(); ri++ {
		rp := data.ResourceProfiles().At(ri)
		rp.Resource().Attributes().Range(func(key string, value pcommon.Value) bool {
			resources = append(resources, attributeRow{parent: resourceID, key: key, value: value})
			return true
		})
		for si := 0; si < rp.ScopeProfiles().Len(); si++ {
			sp := rp.ScopeProfiles().At(si)
			sp.Scope().Attributes().Range(func(key string, value pcommon.Value) bool {
				scopes = append(scopes, attributeRow{parent: scopeID, key: key, value: value})
				return true
			})
			scopeID++
		}
		resourceID++
	}
	resourceRecord, err := encodeAttributes(pool, resources, "parent_id")
	if err != nil {
		return nil, nil, err
	}
	scopeRecord, err := encodeAttributes(pool, scopes, "parent_id")
	if err != nil {
		resourceRecord.Release()
		return nil, nil, err
	}
	return resourceRecord, scopeRecord, nil
}

func encodeAttributes(pool memory.Allocator, rows []attributeRow, identityColumn string) (arrow.Record, error) {
	parents := make([]uint16, len(rows))
	keys := make([]string, len(rows))
	types := make([]uint8, len(rows))
	strings := array.NewStringBuilder(pool)
	ints := array.NewInt64Builder(pool)
	doubles := array.NewFloat64Builder(pool)
	bools := array.NewBooleanBuilder(pool)
	bytesValues := array.NewBinaryBuilder(pool, arrow.BinaryTypes.Binary)
	serialized := array.NewBinaryBuilder(pool, arrow.BinaryTypes.Binary)
	defer strings.Release()
	defer ints.Release()
	defer doubles.Release()
	defer bools.Release()
	defer bytesValues.Release()
	defer serialized.Release()
	for i, row := range rows {
		parents[i], keys[i], types[i] = row.parent, row.key, uint8(row.value.Type())
		switch row.value.Type() {
		case pcommon.ValueTypeStr:
			strings.Append(row.value.Str())
			ints.AppendNull()
			doubles.AppendNull()
			bools.AppendNull()
			bytesValues.AppendNull()
			serialized.AppendNull()
		case pcommon.ValueTypeInt:
			strings.AppendNull()
			ints.Append(row.value.Int())
			doubles.AppendNull()
			bools.AppendNull()
			bytesValues.AppendNull()
			serialized.AppendNull()
		case pcommon.ValueTypeDouble:
			strings.AppendNull()
			ints.AppendNull()
			doubles.Append(row.value.Double())
			bools.AppendNull()
			bytesValues.AppendNull()
			serialized.AppendNull()
		case pcommon.ValueTypeBool:
			strings.AppendNull()
			ints.AppendNull()
			doubles.AppendNull()
			bools.Append(row.value.Bool())
			bytesValues.AppendNull()
			serialized.AppendNull()
		case pcommon.ValueTypeBytes:
			strings.AppendNull()
			ints.AppendNull()
			doubles.AppendNull()
			bools.AppendNull()
			bytesValues.Append(row.value.Bytes().AsRaw())
			serialized.AppendNull()
		case pcommon.ValueTypeMap, pcommon.ValueTypeSlice:
			encoded, err := common.Serialize(&row.value)
			if err != nil {
				return nil, fmt.Errorf("serialize profile attribute %q: %w", row.key, err)
			}
			strings.AppendNull()
			ints.AppendNull()
			doubles.AppendNull()
			bools.AppendNull()
			bytesValues.AppendNull()
			serialized.Append(encoded)
		default:
			strings.AppendNull()
			ints.AppendNull()
			doubles.AppendNull()
			bools.AppendNull()
			bytesValues.AppendNull()
			serialized.AppendNull()
		}
	}
	typeBuilder := array.NewUint8Builder(pool)
	typeBuilder.AppendValues(types, nil)
	typeArray := typeBuilder.NewArray()
	typeBuilder.Release()
	return record(pool, []arrow.Field{
		plainField(identityColumn, arrow.PrimitiveTypes.Uint16, false),
		field("key", arrow.BinaryTypes.String, false),
		field("type", arrow.PrimitiveTypes.Uint8, false),
		field("str", arrow.BinaryTypes.String, true),
		field("int", arrow.PrimitiveTypes.Int64, true),
		field("double", arrow.PrimitiveTypes.Float64, true),
		field("bool", arrow.FixedWidthTypes.Boolean, true),
		field("bytes", arrow.BinaryTypes.Binary, true),
		field("ser", arrow.BinaryTypes.Binary, true),
	}, []arrow.Array{
		uint16Array(pool, parents), stringArray(pool, keys), typeArray,
		strings.NewArray(), ints.NewArray(), doubles.NewArray(), bools.NewArray(),
		bytesValues.NewArray(), serialized.NewArray(),
	})
}

func encodeStrings(pool memory.Allocator, dictionary pprofile.ProfilesDictionary) (arrow.Record, error) {
	values := dictionary.StringTable().AsRaw()
	ids := make([]uint32, len(values))
	for i := range ids {
		ids[i] = uint32(i)
	}
	return record(pool, []arrow.Field{
		plainField("id", arrow.PrimitiveTypes.Uint32, true),
		field("value", arrow.BinaryTypes.String, true),
	}, []arrow.Array{uint32Array(pool, ids), stringArray(pool, values)})
}

func encodeFunctions(pool memory.Allocator, dictionary pprofile.ProfilesDictionary) (arrow.Record, error) {
	table := dictionary.FunctionTable()
	ids := make([]uint32, table.Len())
	names := make([]int32, table.Len())
	systemNames := make([]int32, table.Len())
	filenames := make([]int32, table.Len())
	startLines := make([]int64, table.Len())
	for i := range ids {
		row := table.At(i)
		ids[i] = uint32(i)
		names[i] = row.NameStrindex()
		systemNames[i] = row.SystemNameStrindex()
		filenames[i] = row.FilenameStrindex()
		startLines[i] = row.StartLine()
	}
	return record(pool, []arrow.Field{
		plainField("id", arrow.PrimitiveTypes.Uint32, true),
		field("name_strindex", arrow.PrimitiveTypes.Int32, true),
		field("system_name_strindex", arrow.PrimitiveTypes.Int32, true),
		field("filename_strindex", arrow.PrimitiveTypes.Int32, true),
		field("start_line", arrow.PrimitiveTypes.Int64, true),
	}, []arrow.Array{
		uint32Array(pool, ids), int32Array(pool, names), int32Array(pool, systemNames),
		int32Array(pool, filenames), int64Array(pool, startLines),
	})
}

func encodeMappings(pool memory.Allocator, dictionary pprofile.ProfilesDictionary) (arrow.Record, error) {
	table := dictionary.MappingTable()
	ids := make([]uint32, table.Len())
	starts, limits, offsets := make([]uint64, table.Len()), make([]uint64, table.Len()), make([]uint64, table.Len())
	filenames := make([]int32, table.Len())
	attrs := make([][]int32, table.Len())
	for i := range ids {
		row := table.At(i)
		ids[i], starts[i], limits[i], offsets[i] = uint32(i), row.MemoryStart(), row.MemoryLimit(), row.FileOffset()
		filenames[i], attrs[i] = row.FilenameStrindex(), row.AttributeIndices().AsRaw()
	}
	falses := make([]bool, table.Len())
	return record(pool, []arrow.Field{
		plainField("id", arrow.PrimitiveTypes.Uint32, true),
		field("memory_start", arrow.PrimitiveTypes.Uint64, true),
		field("memory_limit", arrow.PrimitiveTypes.Uint64, true),
		field("file_offset", arrow.PrimitiveTypes.Uint64, true),
		field("filename_strindex", arrow.PrimitiveTypes.Int32, true),
		field("has_functions", arrow.FixedWidthTypes.Boolean, true),
		field("has_filenames", arrow.FixedWidthTypes.Boolean, true),
		field("has_line_numbers", arrow.FixedWidthTypes.Boolean, true),
		field("has_inline_frames", arrow.FixedWidthTypes.Boolean, true),
		field("attribute_indices", arrow.ListOf(arrow.PrimitiveTypes.Int32), false),
	}, []arrow.Array{
		uint32Array(pool, ids), uint64Array(pool, starts), uint64Array(pool, limits),
		uint64Array(pool, offsets), int32Array(pool, filenames), boolArray(pool, falses),
		boolArray(pool, falses), boolArray(pool, falses), boolArray(pool, falses),
		int32Lists(pool, attrs),
	})
}

func encodeLocations(pool memory.Allocator, dictionary pprofile.ProfilesDictionary) (arrow.Record, error) {
	table := dictionary.LocationTable()
	ids := make([]uint32, table.Len())
	mappings := make([]int32, table.Len())
	addresses := make([]uint64, table.Len())
	attrs := make([][]int32, table.Len())
	linesType := arrow.StructOf(
		field("function_index", arrow.PrimitiveTypes.Int32, false),
		field("line", arrow.PrimitiveTypes.Int64, false),
		field("column", arrow.PrimitiveTypes.Int64, false),
	)
	linesBuilder := array.NewListBuilder(pool, linesType)
	linesStruct := linesBuilder.ValueBuilder().(*array.StructBuilder)
	for i := range ids {
		row := table.At(i)
		ids[i], mappings[i], addresses[i], attrs[i] = uint32(i), row.MappingIndex(), row.Address(), row.AttributeIndices().AsRaw()
		linesBuilder.Append(true)
		for j := 0; j < row.Lines().Len(); j++ {
			line := row.Lines().At(j)
			linesStruct.Append(true)
			linesStruct.FieldBuilder(0).(*array.Int32Builder).Append(line.FunctionIndex())
			linesStruct.FieldBuilder(1).(*array.Int64Builder).Append(line.Line())
			linesStruct.FieldBuilder(2).(*array.Int64Builder).Append(line.Column())
		}
	}
	lines := linesBuilder.NewArray()
	linesBuilder.Release()
	return record(pool, []arrow.Field{
		plainField("id", arrow.PrimitiveTypes.Uint32, true),
		field("mapping_index", arrow.PrimitiveTypes.Int32, true),
		field("address", arrow.PrimitiveTypes.Uint64, true),
		field("is_folded", arrow.FixedWidthTypes.Boolean, true),
		field("attribute_indices", arrow.ListOf(arrow.PrimitiveTypes.Int32), false),
		field("line", arrow.ListOf(linesType), false),
	}, []arrow.Array{
		uint32Array(pool, ids), int32Array(pool, mappings), uint64Array(pool, addresses),
		boolArray(pool, make([]bool, table.Len())), int32Lists(pool, attrs), lines,
	})
}

func encodeLinks(pool memory.Allocator, dictionary pprofile.ProfilesDictionary) (arrow.Record, error) {
	table := dictionary.LinkTable()
	ids := make([]uint32, table.Len())
	traces, spans := make([][]byte, table.Len()), make([][]byte, table.Len())
	for i := range ids {
		row := table.At(i)
		traceID, spanID := row.TraceID(), row.SpanID()
		ids[i], traces[i], spans[i] = uint32(i), append([]byte(nil), traceID[:]...), append([]byte(nil), spanID[:]...)
	}
	return record(pool, []arrow.Field{
		plainField("id", arrow.PrimitiveTypes.Uint32, true),
		field("trace_id", &arrow.FixedSizeBinaryType{ByteWidth: 16}, true),
		field("span_id", &arrow.FixedSizeBinaryType{ByteWidth: 8}, true),
	}, []arrow.Array{
		uint32Array(pool, ids), fixedBinaryArray(pool, 16, traces), fixedBinaryArray(pool, 8, spans),
	})
}

func encodeAttributeTable(pool memory.Allocator, dictionary pprofile.ProfilesDictionary) (arrow.Record, error) {
	table := dictionary.AttributeTable()
	strings := dictionary.StringTable()
	rows := make([]attributeRow, table.Len())
	ids := make([]uint32, table.Len())
	for i := range rows {
		row := table.At(i)
		keyIndex := int(row.KeyStrindex())
		if keyIndex < 0 || keyIndex >= strings.Len() {
			return nil, fmt.Errorf("profile attribute key index %d outside string table length %d", keyIndex, strings.Len())
		}
		ids[i] = uint32(i)
		rows[i] = attributeRow{key: strings.At(keyIndex), value: row.Value()}
	}
	attrs, err := encodeAttributes(pool, rows, "id")
	if err != nil {
		return nil, err
	}
	fields := attrs.Schema().Fields()
	fields[0] = plainField("id", arrow.PrimitiveTypes.Uint32, true)
	columns := make([]arrow.Array, attrs.NumCols())
	columns[0] = uint32Array(pool, ids)
	for i := 1; i < int(attrs.NumCols()); i++ {
		columns[i] = attrs.Column(i)
		columns[i].Retain()
	}
	attrs.Release()
	return record(pool, fields, columns)
}

func encodeAttributeUnits(pool memory.Allocator, dictionary pprofile.ProfilesDictionary) (arrow.Record, error) {
	table := dictionary.AttributeTable()
	ids := make([]uint32, table.Len())
	keys, units := make([]int32, table.Len()), make([]int32, table.Len())
	for i := range ids {
		row := table.At(i)
		ids[i], keys[i], units[i] = uint32(i), row.KeyStrindex(), row.UnitStrindex()
	}
	return record(pool, []arrow.Field{
		plainField("id", arrow.PrimitiveTypes.Uint32, true),
		field("attribute_key_strindex", arrow.PrimitiveTypes.Int32, true),
		field("unit_strindex", arrow.PrimitiveTypes.Int32, true),
	}, []arrow.Array{uint32Array(pool, ids), int32Array(pool, keys), int32Array(pool, units)})
}
