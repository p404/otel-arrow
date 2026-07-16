// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! This module contains builders for record batches for profiles.
//!
//! OTLP profiles is already a dictionary-normalized model: `ProfilesData`
//! carries flat lookup tables (string/function/mapping/location/link/
//! attribute/attribute-units) and the profile/sample tree references entries
//! by index. The builders in this module therefore perform a *faithful
//! columnar transposition*:
//!
//! - each lookup table becomes its OTAP interned table with row order
//!   preserved verbatim and `id` equal to the row position `0..n`, and
//! - every index/strindex value is copied as-is (index `0` is a valid table
//!   reference everywhere, so no zero-to-null mapping is ever applied to
//!   index columns).
//!
//! Nothing here re-interns, dedups, sorts or reorders any interned table —
//! byte-identical OTLP round-tripping requires index identity. Only the
//! `Profiles` root and its `Sample` child are "real" row streams keyed by
//! `id`/`parent_id`, and only those (plus the resource/scope attrs
//! side-tables) participate in transport optimization; their id columns are
//! stamped with `plain` encoding metadata so the receive path knows they are
//! not delta-encoded.

use std::sync::Arc;

use arrow::{
    array::{
        Array, ArrayRef, Int32Builder, Int64Builder, ListArray, ListBuilder, NullBufferBuilder,
        PrimitiveBuilder, RecordBatch, StructArray, StructBuilder,
    },
    datatypes::{DataType, Field, Fields, Int32Type, Int64Type, Schema, UInt64Type},
    error::ArrowError,
};

use crate::{
    encode::record::{
        array::{
            ArrayAppend, ArrayAppendNulls, ArrayAppendSlice, ArrayOptions, BinaryArrayBuilder,
            CheckedArrayAppendSlice, FixedSizeBinaryArrayBuilder, Int32ArrayBuilder,
            Int64ArrayBuilder, TimestampNanosecondArrayBuilder, UInt16ArrayBuilder,
            UInt32ArrayBuilder, UInt64ArrayBuilder, binary_to_utf8_array,
            boolean::{AdaptiveBooleanArrayBuilder, BooleanBuilderOptions},
            dictionary::DictionaryOptions,
        },
        attributes::AnyValuesRecordsBuilder,
        logs::{ResourceBuilder, ScopeBuilder},
    },
    schema::{FieldExt, consts},
};

/// Array options for an `id`-like column: the column is only produced when at
/// least one row was appended, and id values equal to the type default (`0`)
/// are never elided — `0` is a real row identity for profiles.
fn id_column_options() -> ArrayOptions {
    ArrayOptions {
        optional: true,
        dictionary_options: None,
        default_values_optional: false,
    }
}

/// Builder for a nullable `ValueType` struct column (`period_type`):
/// `Struct{type_strindex, unit_strindex, aggregation_temporality}`.
struct ValueTypeStructBuilder {
    type_strindex: Int32Builder,
    unit_strindex: Int32Builder,
    aggregation_temporality: Int32Builder,
    nulls: NullBufferBuilder,
    value_count: usize,
}

impl ValueTypeStructBuilder {
    fn new() -> Self {
        Self {
            type_strindex: Int32Builder::new(),
            unit_strindex: Int32Builder::new(),
            aggregation_temporality: Int32Builder::new(),
            nulls: NullBufferBuilder::new(0),
            value_count: 0,
        }
    }

    /// Append a `(type_strindex, unit_strindex, aggregation_temporality)` row,
    /// or a null row when the underlying proto message is not present.
    fn append(&mut self, val: Option<(i32, i32, i32)>) {
        match val {
            Some((type_strindex, unit_strindex, aggregation_temporality)) => {
                self.type_strindex.append_value(type_strindex);
                self.unit_strindex.append_value(unit_strindex);
                self.aggregation_temporality
                    .append_value(aggregation_temporality);
                self.nulls.append(true);
                self.value_count += 1;
            }
            None => {
                self.type_strindex.append_null();
                self.unit_strindex.append_null();
                self.aggregation_temporality.append_null();
                self.nulls.append_null();
            }
        }
    }

    /// Build the resulting `StructArray`. Returns `None` when every row is
    /// null (the column is omitted entirely in that case).
    fn finish(&mut self) -> Option<Result<StructArray, ArrowError>> {
        let nulls = self.nulls.finish();
        if self.value_count == 0 {
            return None;
        }

        let fields = Fields::from(vec![
            Field::new(consts::TYPE_STRINDEX, DataType::Int32, true),
            Field::new(consts::UNIT_STRINDEX, DataType::Int32, true),
            Field::new(consts::AGGREGATION_TEMPORALITY, DataType::Int32, true),
        ]);
        let columns: Vec<ArrayRef> = vec![
            Arc::new(self.type_strindex.finish()),
            Arc::new(self.unit_strindex.finish()),
            Arc::new(self.aggregation_temporality.finish()),
        ];

        Some(StructArray::try_new(fields, columns, nulls))
    }
}

/// Builder for the `sample_type` column:
/// `List<Struct{type_strindex, unit_strindex, aggregation_temporality}>`.
struct SampleTypesListBuilder {
    lists: ListBuilder<StructBuilder>,
}

impl SampleTypesListBuilder {
    fn new() -> Self {
        Self {
            lists: ListBuilder::new(StructBuilder::from_fields(
                vec![
                    Field::new(consts::TYPE_STRINDEX, DataType::Int32, false),
                    Field::new(consts::UNIT_STRINDEX, DataType::Int32, false),
                    Field::new(consts::AGGREGATION_TEMPORALITY, DataType::Int32, false),
                ],
                3,
            )),
        }
    }

    /// Append a (possibly empty) sequence of
    /// `(type_strindex, unit_strindex, aggregation_temporality)` entries as
    /// one list row. The list row itself is always valid, never null.
    fn append(&mut self, val: impl Iterator<Item = (i32, i32, i32)>) {
        let values = self.lists.values();
        for (type_strindex, unit_strindex, aggregation_temporality) in val {
            // SAFETY: these `expect`s can never fire: by construction the
            // struct builder has exactly three `Int32` fields.
            values
                .field_builder::<Int32Builder>(0)
                .expect("field 0 should be Int32")
                .append_value(type_strindex);
            values
                .field_builder::<Int32Builder>(1)
                .expect("field 1 should be Int32")
                .append_value(unit_strindex);
            values
                .field_builder::<Int32Builder>(2)
                .expect("field 2 should be Int32")
                .append_value(aggregation_temporality);
            values.append(true);
        }

        self.lists.append(true);
    }

    fn finish(&mut self) -> Option<ListArray> {
        let array = self.lists.finish();
        (!array.is_empty()).then_some(array)
    }
}

/// Builder for the location table's `line` column:
/// `List<Struct{function_index, line, column}>`.
struct LinesListBuilder {
    lists: ListBuilder<StructBuilder>,
}

impl LinesListBuilder {
    fn new() -> Self {
        Self {
            lists: ListBuilder::new(StructBuilder::from_fields(
                vec![
                    Field::new(consts::FUNCTION_INDEX, DataType::Int32, false),
                    Field::new(consts::LINE, DataType::Int64, false),
                    Field::new(consts::COLUMN, DataType::Int64, false),
                ],
                3,
            )),
        }
    }

    /// Append a (possibly empty) sequence of `(function_index, line, column)`
    /// entries as one list row. The list row itself is always valid.
    fn append(&mut self, val: impl Iterator<Item = (i32, i64, i64)>) {
        let values = self.lists.values();
        for (function_index, line, column) in val {
            // SAFETY: these `expect`s can never fire: by construction the
            // struct builder has exactly these three fields.
            values
                .field_builder::<Int32Builder>(0)
                .expect("field 0 should be Int32")
                .append_value(function_index);
            values
                .field_builder::<Int64Builder>(1)
                .expect("field 1 should be Int64")
                .append_value(line);
            values
                .field_builder::<Int64Builder>(2)
                .expect("field 2 should be Int64")
                .append_value(column);
            values.append(true);
        }

        self.lists.append(true);
    }

    fn finish(&mut self) -> Option<ListArray> {
        let array = self.lists.finish();
        (!array.is_empty()).then_some(array)
    }
}

/// Builder for a `List<Int32>` index column (`location_indices`,
/// `comment_strindices`, `attribute_indices`). Index values are copied
/// verbatim; list rows are always valid, never null.
struct IndicesListBuilder {
    lists: ListBuilder<PrimitiveBuilder<Int32Type>>,
}

impl IndicesListBuilder {
    fn new() -> Self {
        Self {
            lists: ListBuilder::new(PrimitiveBuilder::new()),
        }
    }

    fn append(&mut self, val: impl Iterator<Item = i32>) {
        self.lists.append_value(val.map(Some));
    }

    fn finish(&mut self) -> Option<ListArray> {
        let array = self.lists.finish();
        (!array.is_empty()).then_some(array)
    }
}

/// Record batch builder for the OTAP `Profiles` (root) record: one row per
/// OTLP `Profile`, with resource/scope/schema_url flattened onto the row
/// following the same convention as logs/spans/metrics.
pub struct ProfilesRecordBatchBuilder {
    id: UInt16ArrayBuilder,

    /// the builder for the resource struct of this profiles record batch
    pub resource: ResourceBuilder,

    /// the builder for the scope struct of this profiles record batch
    pub scope: ScopeBuilder,

    schema_url: BinaryArrayBuilder,
    time_nanos: TimestampNanosecondArrayBuilder,
    duration_nanos: Int64ArrayBuilder,
    period: Int64ArrayBuilder,
    period_type: ValueTypeStructBuilder,
    default_sample_type_index: Int32ArrayBuilder,
    profile_id: FixedSizeBinaryArrayBuilder,
    dropped_attributes_count: UInt32ArrayBuilder,
    original_payload_format: BinaryArrayBuilder,
    original_payload: BinaryArrayBuilder,
    sample_type: SampleTypesListBuilder,
    location_indices: IndicesListBuilder,
    comment_strindices: IndicesListBuilder,
    attribute_indices: IndicesListBuilder,
}

impl ProfilesRecordBatchBuilder {
    /// Create a new instance of `ProfilesRecordBatchBuilder`
    #[must_use]
    pub fn new() -> Self {
        Self {
            id: UInt16ArrayBuilder::new(id_column_options()),
            resource: ResourceBuilder::new(),
            scope: ScopeBuilder::new(),
            schema_url: BinaryArrayBuilder::new(ArrayOptions {
                optional: true,
                dictionary_options: Some(DictionaryOptions::dict8()),
                ..Default::default()
            }),
            time_nanos: TimestampNanosecondArrayBuilder::new(ArrayOptions {
                optional: true,
                dictionary_options: None,
                ..Default::default()
            }),
            duration_nanos: Int64ArrayBuilder::new(ArrayOptions {
                optional: true,
                dictionary_options: None,
                ..Default::default()
            }),
            period: Int64ArrayBuilder::new(ArrayOptions {
                optional: true,
                dictionary_options: None,
                ..Default::default()
            }),
            period_type: ValueTypeStructBuilder::new(),
            default_sample_type_index: Int32ArrayBuilder::new(ArrayOptions {
                optional: true,
                dictionary_options: None,
                ..Default::default()
            }),
            // note: no dictionary and no default-eliding here — an all-zero
            // profile_id, while invalid per the spec, must survive verbatim
            profile_id: FixedSizeBinaryArrayBuilder::new_with_args(id_column_options(), 16),
            dropped_attributes_count: UInt32ArrayBuilder::new(ArrayOptions {
                optional: true,
                dictionary_options: None,
                ..Default::default()
            }),
            original_payload_format: BinaryArrayBuilder::new(ArrayOptions {
                optional: true,
                dictionary_options: Some(DictionaryOptions::dict8()),
                ..Default::default()
            }),
            original_payload: BinaryArrayBuilder::new(ArrayOptions {
                optional: true,
                dictionary_options: None,
                ..Default::default()
            }),
            sample_type: SampleTypesListBuilder::new(),
            location_indices: IndicesListBuilder::new(),
            comment_strindices: IndicesListBuilder::new(),
            attribute_indices: IndicesListBuilder::new(),
        }
    }

    /// append a value to the `id` array
    pub fn append_id(&mut self, val: u16) {
        self.id.append_value(&val);
    }

    /// append a value to the `schema_url` array
    pub fn append_schema_url(&mut self, val: Option<&[u8]>) {
        if let Some(val) = val {
            self.schema_url.append_slice(val);
        } else {
            self.schema_url.append_null();
        }
    }

    /// append a value to the `time_nanos` array (verbatim, `0` included)
    pub fn append_time_nanos(&mut self, val: i64) {
        self.time_nanos.append_value(&val);
    }

    /// append a value to the `duration_nanos` array (verbatim, `0` included)
    pub fn append_duration_nanos(&mut self, val: i64) {
        self.duration_nanos.append_value(&val);
    }

    /// append a value to the `period` array (verbatim, `0` included)
    pub fn append_period(&mut self, val: i64) {
        self.period.append_value(&val);
    }

    /// append a `(type_strindex, unit_strindex, aggregation_temporality)`
    /// value to the `period_type` struct array, or null when the profile has
    /// no period type
    pub fn append_period_type(&mut self, val: Option<(i32, i32, i32)>) {
        self.period_type.append(val);
    }

    /// append the profile's sample types to the `sample_type` list array as
    /// `(type_strindex, unit_strindex, aggregation_temporality)` entries
    pub fn append_sample_types(&mut self, val: impl Iterator<Item = (i32, i32, i32)>) {
        self.sample_type.append(val);
    }

    /// append a value to the `default_sample_type_index` array (verbatim)
    pub fn append_default_sample_type_index(&mut self, val: i32) {
        self.default_sample_type_index.append_value(&val);
    }

    /// append a value to the `profile_id` array. Values that are not exactly
    /// 16 bytes long (e.g. the empty proto default) are appended as null.
    pub fn append_profile_id(&mut self, val: &[u8]) -> Result<(), ArrowError> {
        if val.len() == 16 {
            self.profile_id.append_slice(val)
        } else {
            self.profile_id.append_null();
            Ok(())
        }
    }

    /// append a value to the `dropped_attributes_count` array
    pub fn append_dropped_attributes_count(&mut self, val: u32) {
        self.dropped_attributes_count.append_value(&val);
    }

    /// append a value to the `original_payload_format` array
    pub fn append_original_payload_format(&mut self, val: Option<&[u8]>) {
        if let Some(val) = val {
            self.original_payload_format.append_slice(val);
        } else {
            self.original_payload_format.append_null();
        }
    }

    /// append a value to the `original_payload` array. The empty proto
    /// default is appended as null.
    pub fn append_original_payload(&mut self, val: &[u8]) {
        if val.is_empty() {
            self.original_payload.append_null();
        } else {
            self.original_payload.append_slice(val);
        }
    }

    /// append the profile's location indices as one list row (verbatim)
    pub fn append_location_indices(&mut self, val: impl Iterator<Item = i32>) {
        self.location_indices.append(val);
    }

    /// append the profile's comment string indices as one list row (verbatim)
    pub fn append_comment_strindices(&mut self, val: impl Iterator<Item = i32>) {
        self.comment_strindices.append(val);
    }

    /// append the profile's attribute indices as one list row (verbatim)
    pub fn append_attribute_indices(&mut self, val: impl Iterator<Item = i32>) {
        self.attribute_indices.append(val);
    }

    /// construct an OTAP Profiles record batch from the array builders
    pub fn finish(&mut self) -> Result<RecordBatch, ArrowError> {
        let mut fields = vec![];
        let mut columns = vec![];

        if let Some(array) = self.id.finish() {
            fields.push(
                Field::new(consts::ID, array.data_type().clone(), true).with_plain_encoding(),
            );
            columns.push(array);
        }

        let resources = self.resource.finish()?;
        fields.push(Field::new(
            consts::RESOURCE,
            resources.data_type().clone(),
            true,
        ));
        columns.push(Arc::new(resources) as ArrayRef);

        let scopes = self.scope.finish()?;
        fields.push(Field::new(consts::SCOPE, scopes.data_type().clone(), true));
        columns.push(Arc::new(scopes) as ArrayRef);

        if let Some(array) = self.schema_url.finish() {
            let array = binary_to_utf8_array(&array)?;
            fields.push(Field::new(
                consts::SCHEMA_URL,
                array.data_type().clone(),
                true,
            ));
            columns.push(array);
        }

        if let Some(array) = self.time_nanos.finish() {
            fields.push(Field::new(
                consts::TIME_NANOS,
                array.data_type().clone(),
                true,
            ));
            columns.push(array);
        }

        if let Some(array) = self.duration_nanos.finish() {
            fields.push(Field::new(
                consts::DURATION_NANOS,
                array.data_type().clone(),
                true,
            ));
            columns.push(array);
        }

        if let Some(array) = self.period.finish() {
            fields.push(Field::new(consts::PERIOD, array.data_type().clone(), true));
            columns.push(array);
        }

        if let Some(array) = self.period_type.finish().transpose()? {
            fields.push(Field::new(
                consts::PERIOD_TYPE,
                array.data_type().clone(),
                true,
            ));
            columns.push(Arc::new(array) as ArrayRef);
        }

        if let Some(array) = self.default_sample_type_index.finish() {
            fields.push(Field::new(
                consts::DEFAULT_SAMPLE_TYPE_INDEX,
                array.data_type().clone(),
                true,
            ));
            columns.push(array);
        }

        if let Some(array) = self.profile_id.finish() {
            fields.push(Field::new(
                consts::PROFILE_ID,
                array.data_type().clone(),
                true,
            ));
            columns.push(array);
        }

        if let Some(array) = self.dropped_attributes_count.finish() {
            fields.push(Field::new(
                consts::DROPPED_ATTRIBUTES_COUNT,
                array.data_type().clone(),
                true,
            ));
            columns.push(array);
        }

        if let Some(array) = self.original_payload_format.finish() {
            let array = binary_to_utf8_array(&array)?;
            fields.push(Field::new(
                consts::ORIGINAL_PAYLOAD_FORMAT,
                array.data_type().clone(),
                true,
            ));
            columns.push(array);
        }

        if let Some(array) = self.original_payload.finish() {
            fields.push(Field::new(
                consts::ORIGINAL_PAYLOAD,
                array.data_type().clone(),
                true,
            ));
            columns.push(array);
        }

        if let Some(array) = self.sample_type.finish() {
            fields.push(Field::new(
                consts::SAMPLE_TYPE,
                array.data_type().clone(),
                false,
            ));
            columns.push(Arc::new(array) as ArrayRef);
        }

        if let Some(array) = self.location_indices.finish() {
            fields.push(Field::new(
                consts::LOCATION_INDICES,
                array.data_type().clone(),
                false,
            ));
            columns.push(Arc::new(array) as ArrayRef);
        }

        if let Some(array) = self.comment_strindices.finish() {
            fields.push(Field::new(
                consts::COMMENT_STRINDICES,
                array.data_type().clone(),
                false,
            ));
            columns.push(Arc::new(array) as ArrayRef);
        }

        if let Some(array) = self.attribute_indices.finish() {
            fields.push(Field::new(
                consts::ATTRIBUTE_INDICES,
                array.data_type().clone(),
                false,
            ));
            columns.push(Arc::new(array) as ArrayRef);
        }

        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
    }
}

/// Record batch builder for the OTAP `Sample` record: one row per OTLP
/// `Sample`, keyed to its owning profile via `parent_id`.
pub struct SampleRecordBatchBuilder {
    parent_id: UInt16ArrayBuilder,
    locations_start_index: Int32ArrayBuilder,
    locations_length: Int32ArrayBuilder,
    value: ListBuilder<PrimitiveBuilder<Int64Type>>,
    attribute_indices: IndicesListBuilder,
    link_index: Int32ArrayBuilder,
    timestamps_unix_nano: ListBuilder<PrimitiveBuilder<UInt64Type>>,
}

impl SampleRecordBatchBuilder {
    /// Create a new instance of `SampleRecordBatchBuilder`
    #[must_use]
    pub fn new() -> Self {
        Self {
            parent_id: UInt16ArrayBuilder::new(ArrayOptions {
                optional: false,
                dictionary_options: None,
                default_values_optional: false,
            }),
            locations_start_index: Int32ArrayBuilder::new(ArrayOptions {
                optional: true,
                dictionary_options: None,
                ..Default::default()
            }),
            locations_length: Int32ArrayBuilder::new(ArrayOptions {
                optional: true,
                dictionary_options: None,
                ..Default::default()
            }),
            value: ListBuilder::new(PrimitiveBuilder::new()),
            attribute_indices: IndicesListBuilder::new(),
            // `link_index` is `optional` in the proto: presence (including
            // `Some(0)`, a valid reference to link table row 0) is meaningful,
            // so default values must never be elided
            link_index: Int32ArrayBuilder::new(id_column_options()),
            timestamps_unix_nano: ListBuilder::new(PrimitiveBuilder::new()),
        }
    }

    /// append a value to the `parent_id` array
    pub fn append_parent_id(&mut self, val: u16) {
        self.parent_id.append_value(&val);
    }

    /// append a value to the `locations_start_index` array (verbatim)
    pub fn append_locations_start_index(&mut self, val: i32) {
        self.locations_start_index.append_value(&val);
    }

    /// append a value to the `locations_length` array (verbatim)
    pub fn append_locations_length(&mut self, val: i32) {
        self.locations_length.append_value(&val);
    }

    /// append the sample's values as one list row
    pub fn append_values(&mut self, val: impl Iterator<Item = i64>) {
        self.value.append_value(val.map(Some));
    }

    /// append the sample's attribute indices as one list row (verbatim)
    pub fn append_attribute_indices(&mut self, val: impl Iterator<Item = i32>) {
        self.attribute_indices.append(val);
    }

    /// append a value to the `link_index` array; `None` (field not present)
    /// is appended as null, `Some` values — including `Some(0)` — verbatim
    pub fn append_link_index(&mut self, val: Option<i32>) {
        if let Some(val) = val {
            self.link_index.append_value(&val);
        } else {
            self.link_index.append_null();
        }
    }

    /// append the sample's timestamps as one list row
    pub fn append_timestamps_unix_nano(&mut self, val: impl Iterator<Item = u64>) {
        self.timestamps_unix_nano.append_value(val.map(Some));
    }

    /// construct an OTAP Sample record batch from the array builders
    pub fn finish(&mut self) -> Result<RecordBatch, ArrowError> {
        let mut fields = vec![];
        let mut columns = vec![];

        // SAFETY: `expect` is safe here because `AdaptiveArrayBuilder` guarantees that for
        // non-optional arrays, `finish()` will always return an array, even if it is empty.
        let array = self
            .parent_id
            .finish()
            .expect("finish returns `Some(array)`");
        fields.push(
            Field::new(consts::PARENT_ID, array.data_type().clone(), false).with_plain_encoding(),
        );
        columns.push(array);

        if let Some(array) = self.locations_start_index.finish() {
            fields.push(Field::new(
                consts::LOCATIONS_START_INDEX,
                array.data_type().clone(),
                true,
            ));
            columns.push(array);
        }

        if let Some(array) = self.locations_length.finish() {
            fields.push(Field::new(
                consts::LOCATIONS_LENGTH,
                array.data_type().clone(),
                true,
            ));
            columns.push(array);
        }

        let value = self.value.finish();
        if !value.is_empty() {
            fields.push(Field::new(
                consts::SAMPLE_VALUE,
                value.data_type().clone(),
                false,
            ));
            columns.push(Arc::new(value) as ArrayRef);
        }

        if let Some(array) = self.attribute_indices.finish() {
            fields.push(Field::new(
                consts::ATTRIBUTE_INDICES,
                array.data_type().clone(),
                false,
            ));
            columns.push(Arc::new(array) as ArrayRef);
        }

        if let Some(array) = self.link_index.finish() {
            fields.push(Field::new(
                consts::LINK_INDEX,
                array.data_type().clone(),
                true,
            ));
            columns.push(array);
        }

        let timestamps = self.timestamps_unix_nano.finish();
        if !timestamps.is_empty() {
            fields.push(Field::new(
                consts::TIMESTAMPS_UNIX_NANO,
                timestamps.data_type().clone(),
                false,
            ));
            columns.push(Arc::new(timestamps) as ArrayRef);
        }

        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
    }
}

/// Record batch builder for the interned `StringTable`: one row per entry of
/// `ProfilesData.string_table`, in table order, with `id` = row position.
pub struct StringTableRecordBatchBuilder {
    id: UInt32ArrayBuilder,
    value: BinaryArrayBuilder,
}

impl StringTableRecordBatchBuilder {
    /// Create a new instance of `StringTableRecordBatchBuilder`
    #[must_use]
    pub fn new() -> Self {
        Self {
            id: UInt32ArrayBuilder::new(id_column_options()),
            // empty strings are valid, meaningful table entries (by
            // convention the table starts with one), so the default value
            // ("") must never be elided
            value: BinaryArrayBuilder::new(ArrayOptions {
                optional: true,
                dictionary_options: Some(DictionaryOptions::dict8()),
                default_values_optional: false,
            }),
        }
    }

    /// append a string table entry (verbatim, empty strings included)
    pub fn append(&mut self, id: u32, val: &[u8]) {
        self.id.append_value(&id);
        self.value.append_slice(val);
    }

    /// construct an OTAP StringTable record batch from the array builders
    pub fn finish(&mut self) -> Result<RecordBatch, ArrowError> {
        let mut fields = vec![];
        let mut columns = vec![];

        if let Some(array) = self.id.finish() {
            fields.push(
                Field::new(consts::ID, array.data_type().clone(), true).with_plain_encoding(),
            );
            columns.push(array);
        }

        if let Some(array) = self.value.finish() {
            let array = binary_to_utf8_array(&array)?;
            fields.push(Field::new(
                consts::STRING_TABLE_VALUE,
                array.data_type().clone(),
                false,
            ));
            columns.push(array);
        }

        if fields.is_empty() {
            return Ok(RecordBatch::new_empty(Arc::new(Schema::empty())));
        }
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
    }
}

/// Record batch builder for the interned `FunctionTable`: one row per entry
/// of `ProfilesData.function_table`, in table order, with `id` = row position.
pub struct FunctionTableRecordBatchBuilder {
    id: UInt32ArrayBuilder,
    name_strindex: Int32ArrayBuilder,
    system_name_strindex: Int32ArrayBuilder,
    filename_strindex: Int32ArrayBuilder,
    start_line: Int64ArrayBuilder,
}

impl FunctionTableRecordBatchBuilder {
    /// Create a new instance of `FunctionTableRecordBatchBuilder`
    #[must_use]
    pub fn new() -> Self {
        Self {
            id: UInt32ArrayBuilder::new(id_column_options()),
            name_strindex: Int32ArrayBuilder::new(ArrayOptions {
                optional: true,
                dictionary_options: None,
                ..Default::default()
            }),
            system_name_strindex: Int32ArrayBuilder::new(ArrayOptions {
                optional: true,
                dictionary_options: None,
                ..Default::default()
            }),
            filename_strindex: Int32ArrayBuilder::new(ArrayOptions {
                optional: true,
                dictionary_options: None,
                ..Default::default()
            }),
            start_line: Int64ArrayBuilder::new(ArrayOptions {
                optional: true,
                dictionary_options: None,
                ..Default::default()
            }),
        }
    }

    /// append a value to the `id` array
    pub fn append_id(&mut self, val: u32) {
        self.id.append_value(&val);
    }

    /// append a value to the `name_strindex` array (verbatim)
    pub fn append_name_strindex(&mut self, val: i32) {
        self.name_strindex.append_value(&val);
    }

    /// append a value to the `system_name_strindex` array (verbatim)
    pub fn append_system_name_strindex(&mut self, val: i32) {
        self.system_name_strindex.append_value(&val);
    }

    /// append a value to the `filename_strindex` array (verbatim)
    pub fn append_filename_strindex(&mut self, val: i32) {
        self.filename_strindex.append_value(&val);
    }

    /// append a value to the `start_line` array (verbatim)
    pub fn append_start_line(&mut self, val: i64) {
        self.start_line.append_value(&val);
    }

    /// construct an OTAP FunctionTable record batch from the array builders
    pub fn finish(&mut self) -> Result<RecordBatch, ArrowError> {
        let mut fields = vec![];
        let mut columns = vec![];

        if let Some(array) = self.id.finish() {
            fields.push(
                Field::new(consts::ID, array.data_type().clone(), true).with_plain_encoding(),
            );
            columns.push(array);
        }

        if let Some(array) = self.name_strindex.finish() {
            fields.push(Field::new(
                consts::NAME_STRINDEX,
                array.data_type().clone(),
                true,
            ));
            columns.push(array);
        }

        if let Some(array) = self.system_name_strindex.finish() {
            fields.push(Field::new(
                consts::SYSTEM_NAME_STRINDEX,
                array.data_type().clone(),
                true,
            ));
            columns.push(array);
        }

        if let Some(array) = self.filename_strindex.finish() {
            fields.push(Field::new(
                consts::FILENAME_STRINDEX,
                array.data_type().clone(),
                true,
            ));
            columns.push(array);
        }

        if let Some(array) = self.start_line.finish() {
            fields.push(Field::new(
                consts::START_LINE,
                array.data_type().clone(),
                true,
            ));
            columns.push(array);
        }

        if fields.is_empty() {
            return Ok(RecordBatch::new_empty(Arc::new(Schema::empty())));
        }
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
    }
}

/// Record batch builder for the interned `MappingTable`: one row per entry of
/// `ProfilesData.mapping_table`, in table order, with `id` = row position.
pub struct MappingTableRecordBatchBuilder {
    id: UInt32ArrayBuilder,
    memory_start: UInt64ArrayBuilder,
    memory_limit: UInt64ArrayBuilder,
    file_offset: UInt64ArrayBuilder,
    filename_strindex: Int32ArrayBuilder,
    has_functions: AdaptiveBooleanArrayBuilder,
    has_filenames: AdaptiveBooleanArrayBuilder,
    has_line_numbers: AdaptiveBooleanArrayBuilder,
    has_inline_frames: AdaptiveBooleanArrayBuilder,
    attribute_indices: IndicesListBuilder,
}

impl MappingTableRecordBatchBuilder {
    /// Create a new instance of `MappingTableRecordBatchBuilder`
    #[must_use]
    pub fn new() -> Self {
        let bool_options = || BooleanBuilderOptions {
            optional: true,
            // structural flags where `false` is the proto default: an
            // all-false column is omitted and losslessly decodes back to false
            skip_all_false: true,
        };
        Self {
            id: UInt32ArrayBuilder::new(id_column_options()),
            memory_start: UInt64ArrayBuilder::new(ArrayOptions {
                optional: true,
                dictionary_options: None,
                ..Default::default()
            }),
            memory_limit: UInt64ArrayBuilder::new(ArrayOptions {
                optional: true,
                dictionary_options: None,
                ..Default::default()
            }),
            file_offset: UInt64ArrayBuilder::new(ArrayOptions {
                optional: true,
                dictionary_options: None,
                ..Default::default()
            }),
            filename_strindex: Int32ArrayBuilder::new(ArrayOptions {
                optional: true,
                dictionary_options: None,
                ..Default::default()
            }),
            has_functions: AdaptiveBooleanArrayBuilder::new(bool_options()),
            has_filenames: AdaptiveBooleanArrayBuilder::new(bool_options()),
            has_line_numbers: AdaptiveBooleanArrayBuilder::new(bool_options()),
            has_inline_frames: AdaptiveBooleanArrayBuilder::new(bool_options()),
            attribute_indices: IndicesListBuilder::new(),
        }
    }

    /// append a value to the `id` array
    pub fn append_id(&mut self, val: u32) {
        self.id.append_value(&val);
    }

    /// append a value to the `memory_start` array (verbatim)
    pub fn append_memory_start(&mut self, val: u64) {
        self.memory_start.append_value(&val);
    }

    /// append a value to the `memory_limit` array (verbatim)
    pub fn append_memory_limit(&mut self, val: u64) {
        self.memory_limit.append_value(&val);
    }

    /// append a value to the `file_offset` array (verbatim)
    pub fn append_file_offset(&mut self, val: u64) {
        self.file_offset.append_value(&val);
    }

    /// append a value to the `filename_strindex` array (verbatim)
    pub fn append_filename_strindex(&mut self, val: i32) {
        self.filename_strindex.append_value(&val);
    }

    /// append a value to the `has_functions` array
    pub fn append_has_functions(&mut self, val: bool) {
        self.has_functions.append_value(val);
    }

    /// append a value to the `has_filenames` array
    pub fn append_has_filenames(&mut self, val: bool) {
        self.has_filenames.append_value(val);
    }

    /// append a value to the `has_line_numbers` array
    pub fn append_has_line_numbers(&mut self, val: bool) {
        self.has_line_numbers.append_value(val);
    }

    /// append a value to the `has_inline_frames` array
    pub fn append_has_inline_frames(&mut self, val: bool) {
        self.has_inline_frames.append_value(val);
    }

    /// append the mapping's attribute indices as one list row (verbatim)
    pub fn append_attribute_indices(&mut self, val: impl Iterator<Item = i32>) {
        self.attribute_indices.append(val);
    }

    /// construct an OTAP MappingTable record batch from the array builders
    pub fn finish(&mut self) -> Result<RecordBatch, ArrowError> {
        let mut fields = vec![];
        let mut columns = vec![];

        if let Some(array) = self.id.finish() {
            fields.push(
                Field::new(consts::ID, array.data_type().clone(), true).with_plain_encoding(),
            );
            columns.push(array);
        }

        if let Some(array) = self.memory_start.finish() {
            fields.push(Field::new(
                consts::MEMORY_START,
                array.data_type().clone(),
                true,
            ));
            columns.push(array);
        }

        if let Some(array) = self.memory_limit.finish() {
            fields.push(Field::new(
                consts::MEMORY_LIMIT,
                array.data_type().clone(),
                true,
            ));
            columns.push(array);
        }

        if let Some(array) = self.file_offset.finish() {
            fields.push(Field::new(
                consts::FILE_OFFSET,
                array.data_type().clone(),
                true,
            ));
            columns.push(array);
        }

        if let Some(array) = self.filename_strindex.finish() {
            fields.push(Field::new(
                consts::FILENAME_STRINDEX,
                array.data_type().clone(),
                true,
            ));
            columns.push(array);
        }

        if let Some(array) = self.has_functions.finish() {
            fields.push(Field::new(
                consts::HAS_FUNCTIONS,
                array.data_type().clone(),
                true,
            ));
            columns.push(array);
        }

        if let Some(array) = self.has_filenames.finish() {
            fields.push(Field::new(
                consts::HAS_FILENAMES,
                array.data_type().clone(),
                true,
            ));
            columns.push(array);
        }

        if let Some(array) = self.has_line_numbers.finish() {
            fields.push(Field::new(
                consts::HAS_LINE_NUMBERS,
                array.data_type().clone(),
                true,
            ));
            columns.push(array);
        }

        if let Some(array) = self.has_inline_frames.finish() {
            fields.push(Field::new(
                consts::HAS_INLINE_FRAMES,
                array.data_type().clone(),
                true,
            ));
            columns.push(array);
        }

        if let Some(array) = self.attribute_indices.finish() {
            fields.push(Field::new(
                consts::ATTRIBUTE_INDICES,
                array.data_type().clone(),
                false,
            ));
            columns.push(Arc::new(array) as ArrayRef);
        }

        if fields.is_empty() {
            return Ok(RecordBatch::new_empty(Arc::new(Schema::empty())));
        }
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
    }
}

/// Record batch builder for the interned `LocationTable`: one row per entry
/// of `ProfilesData.location_table`, in table order, with `id` = row position.
pub struct LocationTableRecordBatchBuilder {
    id: UInt32ArrayBuilder,
    mapping_index: Int32ArrayBuilder,
    address: UInt64ArrayBuilder,
    is_folded: AdaptiveBooleanArrayBuilder,
    attribute_indices: IndicesListBuilder,
    line: LinesListBuilder,
}

impl LocationTableRecordBatchBuilder {
    /// Create a new instance of `LocationTableRecordBatchBuilder`
    #[must_use]
    pub fn new() -> Self {
        Self {
            id: UInt32ArrayBuilder::new(id_column_options()),
            // `mapping_index` is `optional` in the proto: presence (including
            // `Some(0)`, a valid reference to mapping table row 0) is
            // meaningful, so default values must never be elided
            mapping_index: Int32ArrayBuilder::new(id_column_options()),
            address: UInt64ArrayBuilder::new(ArrayOptions {
                optional: true,
                dictionary_options: None,
                ..Default::default()
            }),
            is_folded: AdaptiveBooleanArrayBuilder::new(BooleanBuilderOptions {
                optional: true,
                skip_all_false: true,
            }),
            attribute_indices: IndicesListBuilder::new(),
            line: LinesListBuilder::new(),
        }
    }

    /// append a value to the `id` array
    pub fn append_id(&mut self, val: u32) {
        self.id.append_value(&val);
    }

    /// append a value to the `mapping_index` array; `None` (field not
    /// present) is appended as null, `Some` values — including `Some(0)` —
    /// verbatim
    pub fn append_mapping_index(&mut self, val: Option<i32>) {
        if let Some(val) = val {
            self.mapping_index.append_value(&val);
        } else {
            self.mapping_index.append_null();
        }
    }

    /// append a value to the `address` array (verbatim)
    pub fn append_address(&mut self, val: u64) {
        self.address.append_value(&val);
    }

    /// append a value to the `is_folded` array
    pub fn append_is_folded(&mut self, val: bool) {
        self.is_folded.append_value(val);
    }

    /// append the location's attribute indices as one list row (verbatim)
    pub fn append_attribute_indices(&mut self, val: impl Iterator<Item = i32>) {
        self.attribute_indices.append(val);
    }

    /// append the location's line entries as one list row of
    /// `(function_index, line, column)` structs
    pub fn append_lines(&mut self, val: impl Iterator<Item = (i32, i64, i64)>) {
        self.line.append(val);
    }

    /// construct an OTAP LocationTable record batch from the array builders
    pub fn finish(&mut self) -> Result<RecordBatch, ArrowError> {
        let mut fields = vec![];
        let mut columns = vec![];

        if let Some(array) = self.id.finish() {
            fields.push(
                Field::new(consts::ID, array.data_type().clone(), true).with_plain_encoding(),
            );
            columns.push(array);
        }

        if let Some(array) = self.mapping_index.finish() {
            fields.push(Field::new(
                consts::MAPPING_INDEX,
                array.data_type().clone(),
                true,
            ));
            columns.push(array);
        }

        if let Some(array) = self.address.finish() {
            fields.push(Field::new(consts::ADDRESS, array.data_type().clone(), true));
            columns.push(array);
        }

        if let Some(array) = self.is_folded.finish() {
            fields.push(Field::new(
                consts::IS_FOLDED,
                array.data_type().clone(),
                true,
            ));
            columns.push(array);
        }

        if let Some(array) = self.attribute_indices.finish() {
            fields.push(Field::new(
                consts::ATTRIBUTE_INDICES,
                array.data_type().clone(),
                false,
            ));
            columns.push(Arc::new(array) as ArrayRef);
        }

        if let Some(array) = self.line.finish() {
            fields.push(Field::new(consts::LINE, array.data_type().clone(), false));
            columns.push(Arc::new(array) as ArrayRef);
        }

        if fields.is_empty() {
            return Ok(RecordBatch::new_empty(Arc::new(Schema::empty())));
        }
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
    }
}

/// Record batch builder for the interned `LinkTable`: one row per entry of
/// `ProfilesData.link_table`, in table order, with `id` = row position.
pub struct LinkTableRecordBatchBuilder {
    id: UInt32ArrayBuilder,
    trace_id: FixedSizeBinaryArrayBuilder,
    span_id: FixedSizeBinaryArrayBuilder,
}

impl LinkTableRecordBatchBuilder {
    /// Create a new instance of `LinkTableRecordBatchBuilder`
    #[must_use]
    pub fn new() -> Self {
        Self {
            id: UInt32ArrayBuilder::new(id_column_options()),
            // no dictionary and no default-eliding: all-zero ids, while
            // invalid per the spec, must survive verbatim
            trace_id: FixedSizeBinaryArrayBuilder::new_with_args(id_column_options(), 16),
            span_id: FixedSizeBinaryArrayBuilder::new_with_args(id_column_options(), 8),
        }
    }

    /// append a value to the `id` array
    pub fn append_id(&mut self, val: u32) {
        self.id.append_value(&val);
    }

    /// append a value to the `trace_id` array. Values that are not exactly
    /// 16 bytes long (e.g. the empty proto default) are appended as null.
    pub fn append_trace_id(&mut self, val: &[u8]) -> Result<(), ArrowError> {
        if val.len() == 16 {
            self.trace_id.append_slice(val)
        } else {
            self.trace_id.append_null();
            Ok(())
        }
    }

    /// append a value to the `span_id` array. Values that are not exactly
    /// 8 bytes long (e.g. the empty proto default) are appended as null.
    pub fn append_span_id(&mut self, val: &[u8]) -> Result<(), ArrowError> {
        if val.len() == 8 {
            self.span_id.append_slice(val)
        } else {
            self.span_id.append_null();
            Ok(())
        }
    }

    /// construct an OTAP LinkTable record batch from the array builders
    pub fn finish(&mut self) -> Result<RecordBatch, ArrowError> {
        let mut fields = vec![];
        let mut columns = vec![];

        if let Some(array) = self.id.finish() {
            fields.push(
                Field::new(consts::ID, array.data_type().clone(), true).with_plain_encoding(),
            );
            columns.push(array);
        }

        if let Some(array) = self.trace_id.finish() {
            fields.push(Field::new(
                consts::TRACE_ID,
                array.data_type().clone(),
                true,
            ));
            columns.push(array);
        }

        if let Some(array) = self.span_id.finish() {
            fields.push(Field::new(consts::SPAN_ID, array.data_type().clone(), true));
            columns.push(array);
        }

        if fields.is_empty() {
            return Ok(RecordBatch::new_empty(Arc::new(Schema::empty())));
        }
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
    }
}

/// Record batch builder for the interned `AttributeTable`: one row per entry
/// of `ProfilesData.attribute_table`, in table order, with `id` = row
/// position. Unlike the parent-id-joined attribute side tables, rows here are
/// referenced by absolute position, so `id` is a row identity, not a join key.
/// The value lanes reuse the shared attribute value encoding.
pub struct AttributeTableRecordBatchBuilder {
    id: UInt32ArrayBuilder,
    keys: BinaryArrayBuilder,

    /// builder for the attribute value lanes (type/str/int/double/bool/bytes/ser)
    pub any_values_builder: AnyValuesRecordsBuilder,
}

impl AttributeTableRecordBatchBuilder {
    /// Create a new instance of `AttributeTableRecordBatchBuilder`
    #[must_use]
    pub fn new() -> Self {
        Self {
            id: UInt32ArrayBuilder::new(id_column_options()),
            keys: BinaryArrayBuilder::new(ArrayOptions {
                optional: false,
                dictionary_options: Some(DictionaryOptions::dict8()),
                ..Default::default()
            }),
            any_values_builder: AnyValuesRecordsBuilder::new(),
        }
    }

    /// append a value to the `id` array
    pub fn append_id(&mut self, val: u32) {
        self.id.append_value(&val);
    }

    /// append an attribute key to the `key` array
    pub fn append_key(&mut self, val: &[u8]) {
        self.keys.append_slice(val);
    }

    /// construct an OTAP AttributeTable record batch from the array builders
    pub fn finish(&mut self) -> Result<RecordBatch, ArrowError> {
        let mut fields = vec![];
        let mut columns = vec![];

        if let Some(array) = self.id.finish() {
            fields.push(
                Field::new(consts::ID, array.data_type().clone(), true).with_plain_encoding(),
            );
            columns.push(array);
        }

        if let Some(array) = self.keys.finish() {
            let array = binary_to_utf8_array(&array)?;
            fields.push(Field::new(
                consts::ATTRIBUTE_KEY,
                array.data_type().clone(),
                false,
            ));
            columns.push(array);
        }

        self.any_values_builder.finish(&mut columns, &mut fields)?;

        if fields.is_empty() {
            return Ok(RecordBatch::new_empty(Arc::new(Schema::empty())));
        }
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
    }
}

/// Record batch builder for the interned `AttributeUnits` table: one row per
/// entry of `ProfilesData.attribute_units`, in table order, with `id` = row
/// position.
pub struct AttributeUnitsRecordBatchBuilder {
    id: UInt32ArrayBuilder,
    attribute_key_strindex: Int32ArrayBuilder,
    unit_strindex: Int32ArrayBuilder,
}

impl AttributeUnitsRecordBatchBuilder {
    /// Create a new instance of `AttributeUnitsRecordBatchBuilder`
    #[must_use]
    pub fn new() -> Self {
        Self {
            id: UInt32ArrayBuilder::new(id_column_options()),
            attribute_key_strindex: Int32ArrayBuilder::new(ArrayOptions {
                optional: true,
                dictionary_options: None,
                ..Default::default()
            }),
            unit_strindex: Int32ArrayBuilder::new(ArrayOptions {
                optional: true,
                dictionary_options: None,
                ..Default::default()
            }),
        }
    }

    /// append an attribute unit entry (verbatim)
    pub fn append(&mut self, id: u32, attribute_key_strindex: i32, unit_strindex: i32) {
        self.id.append_value(&id);
        self.attribute_key_strindex
            .append_value(&attribute_key_strindex);
        self.unit_strindex.append_value(&unit_strindex);
    }

    /// construct an OTAP AttributeUnits record batch from the array builders
    pub fn finish(&mut self) -> Result<RecordBatch, ArrowError> {
        let mut fields = vec![];
        let mut columns = vec![];

        if let Some(array) = self.id.finish() {
            fields.push(
                Field::new(consts::ID, array.data_type().clone(), true).with_plain_encoding(),
            );
            columns.push(array);
        }

        if let Some(array) = self.attribute_key_strindex.finish() {
            fields.push(Field::new(
                consts::ATTRIBUTE_KEY_STRINDEX,
                array.data_type().clone(),
                true,
            ));
            columns.push(array);
        }

        if let Some(array) = self.unit_strindex.finish() {
            fields.push(Field::new(
                consts::UNIT_STRINDEX,
                array.data_type().clone(),
                true,
            ));
            columns.push(array);
        }

        if fields.is_empty() {
            return Ok(RecordBatch::new_empty(Arc::new(Schema::empty())));
        }
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
    }
}
