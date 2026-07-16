// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! OTAP -> OTLP conversion for the profiles signal.
//!
//! This module reconstructs an OTLP [`ProfilesData`] message from the OTAP
//! profiles record batches produced by
//! [`encode_profiles_otap_batch`](crate::encode::encode_profiles_otap_batch):
//!
//! - the seven interned lookup tables (string/function/mapping/location/link/
//!   attribute/attribute-units) are transposed back verbatim — row order is
//!   index identity (`id` = row position), so every index reference regains
//!   its original meaning without any re-interning, and
//! - the `resource_profiles` -> `scope_profiles` -> `profiles` -> `sample`
//!   tree is regrouped from the flattened `Profiles` root rows (visited in
//!   `(resource id, scope id, id)` order, splitting groups on resource/scope
//!   id changes exactly like the logs decoder) plus the `Sample` child batch
//!   and the `ResourceAttrs`/`ScopeAttrs` side tables, joined by parent id.
//!
//! # Which wire message does this produce?
//!
//! Unlike the other signals, the profiles encoder emits **`ProfilesData`**
//! bytes (not `ExportProfilesServiceRequest` bytes). In the pinned
//! `v1development` proto, `ExportProfilesServiceRequest` only carries
//! `resource_profiles` (field 1) — it has no fields for the dictionary
//! tables, so emitting request bytes would silently drop every interned
//! table. `ProfilesData` is wire-compatible on field 1 (`resource_profiles`
//! in both messages) and additionally carries the dictionary tables as
//! fields 2..=8, and the ingest direction
//! (`OtlpProtoBytes::ExportProfilesRequest` -> `OtapArrowRecords`) already
//! decodes the request bytes as `ProfilesData` for the same reason. Emitting
//! `ProfilesData` bytes here is what makes the round trip lossless (and
//! byte-identical); a reader that only understands
//! `ExportProfilesServiceRequest` still parses field 1 correctly and skips
//! the table fields as unknown fields.
//!
//! # Byte-identical round-tripping
//!
//! The other signals' decoders stream hand-written proto bytes directly from
//! the arrow columns. Profiles instead materializes the [`ProfilesData`]
//! prost struct and serializes it with prost: prost's encoding is
//! deterministic (fields in tag order, packed repeated scalars, minimal
//! varints, defaults elided), so byte-identity with the original
//! prost-encoded input reduces to reconstructing an equal struct — which the
//! decode below guarantees by honoring the encode-side elision contract:
//!
//! - absent/null optional columns decode to the proto default (`0`, `""`,
//!   empty bytes, `false`),
//! - proto `optional` fields (`Sample.link_index`, `Location.mapping_index`,
//!   `Profile.period_type`) preserve presence: column null => `None`,
//!   value (including `0`) => `Some(value)`, and
//! - null `FixedSizeBinary` ids (`profile_id`, link `trace_id`/`span_id`,
//!   which the encoder nulls for wrong-length input) decode to empty bytes.
//!
//! One OTAP-inherent canonicalization applies: `Some(Resource::default())` /
//! `Some(InstrumentationScope::default())` are not distinguishable from
//! `None` in the OTAP columns (no presence bit exists), so a resource/scope
//! with no observable content decodes to `None`.
//!
//! # Index validation
//!
//! Every table-referencing index column (`*_strindex`, `mapping_index`,
//! `link_index`, `function_index`, `attribute_indices`, `location_indices`)
//! is validated against the referenced table's length after reconstruction:
//! a negative or out-of-range index yields a typed
//! [`Error::InvalidProfilesTableIndex`] instead of a panic or a silent
//! wrap-around. (`Sample.locations_start_index`/`locations_length` and
//! `Profile.default_sample_type_index` reference positions inside
//! *per-profile* repeated fields, not interned tables, and `0` is their
//! proto default even when those fields are empty, so they are copied
//! verbatim without a table check.)

use std::collections::HashMap;

use arrow::array::{
    Array, Int32Array, Int64Array, ListArray, RecordBatch, StringArray, StructArray,
    TimestampNanosecondArray, UInt16Array, UInt32Array, UInt64Array,
};
use arrow::datatypes::{DataType, Field, Int32Type, Int64Type};
use prost::Message;

use crate::arrays::{
    ByteArrayAccessor, MaybeDictArrayAccessor, NullableArrayAccessor, StringArrayAccessor,
    StructColumnAccessor, get_bool_array_opt, get_i32_array_opt, get_i64_array_opt,
    get_required_array, get_timestamp_nanosecond_array_opt, get_u16_array_opt, get_u32_array_opt,
    get_u64_array_opt,
};
use crate::error::{Error, Result};
use crate::otap::OtapArrowRecords;
use crate::otlp::ProtoBytesEncoder;
use crate::otlp::attributes::{Attribute16Arrays, AttributeValueType, encode_any_value};
use crate::otlp::common::{
    AnyValueArrays, BatchSorter, BoundedBuf, ProtoBuffer, ResourceArrays, ScopeArrays,
    SortedBatchCursor,
};
use crate::proto::consts::field_num::common::{KEY_VALUE_KEY, KEY_VALUE_VALUE};
use crate::proto::opentelemetry::arrow::v1::ArrowPayloadType;
use crate::proto::opentelemetry::common::v1::{InstrumentationScope, KeyValue};
use crate::proto::opentelemetry::profiles::v1development::{
    AttributeUnit, Function, Line, Link, Location, Mapping, Profile, ProfilesData,
    ResourceProfiles, Sample, ScopeProfiles, ValueType,
};
use crate::proto::opentelemetry::resource::v1::Resource;
use crate::schema::consts;

/* ─────────────────────────── column access helpers ───────────────────── */

/// Downcast an optional column to a [`ListArray`].
fn get_list_array_opt<'a>(rb: &'a RecordBatch, name: &str) -> Result<Option<&'a ListArray>> {
    rb.column_by_name(name)
        .map(|arr| {
            arr.as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| Error::ColumnDataTypeMismatch {
                    name: name.into(),
                    expect: DataType::List(Field::new_list_field(DataType::Null, true).into()),
                    actual: arr.data_type().clone(),
                })
        })
        .transpose()
}

/// Downcast one list row into a primitive array and collect its values,
/// mapping null items (which the encoder never produces) to the default.
macro_rules! impl_list_at {
    ($name:ident, $array_type:ident, $native:ty) => {
        fn $name(list: Option<&ListArray>, index: usize) -> Result<Vec<$native>> {
            let Some(list) = list else {
                return Ok(Vec::new());
            };
            if list.is_null(index) {
                return Ok(Vec::new());
            }
            let values = list.value(index);
            let values = values
                .as_any()
                .downcast_ref::<$array_type>()
                .ok_or_else(|| Error::ColumnDataTypeMismatch {
                    name: "list item".into(),
                    expect: DataType::Null,
                    actual: values.data_type().clone(),
                })?;
            Ok(values.iter().map(Option::unwrap_or_default).collect())
        }
    };
}

impl_list_at!(i32_list_at, Int32Array, i32);
impl_list_at!(i64_list_at, Int64Array, i64);
impl_list_at!(u64_list_at, UInt64Array, u64);

/// Downcast one list row into a struct array.
fn struct_list_at(list: &ListArray, index: usize) -> Result<Option<arrow::array::ArrayRef>> {
    if list.is_null(index) {
        return Ok(None);
    }
    Ok(Some(list.value(index)))
}

/* ─────────────────────────── interned table decode ───────────────────── */

/// Compute the order in which to visit the rows of an interned table so that
/// visit position == OTLP table position.
///
/// The encode contract pins `id` = row position, but a transported batch is
/// only trusted after checking: rows are visited in ascending `id` order and
/// the ids must be exactly `0..num_rows` (index identity). A missing `id`
/// column falls back to natural row order.
fn interned_table_row_order(rb: &RecordBatch, table: &'static str) -> Result<Vec<usize>> {
    let num_rows = rb.num_rows();
    let Some(ids) = get_u32_array_opt(rb, consts::ID)? else {
        return Ok((0..num_rows).collect());
    };

    let mut order: Vec<usize> = (0..num_rows).collect();
    order.sort_by_key(|&row| {
        if ids.is_null(row) {
            u64::MAX
        } else {
            ids.value(row) as u64
        }
    });

    for (position, &row) in order.iter().enumerate() {
        if ids.is_null(row) || ids.value(row) as usize != position {
            return Err(Error::UnexpectedRecordBatchState {
                reason: format!(
                    "profiles {table} table: interned ids must be exactly the row positions 0..{num_rows}"
                ),
            });
        }
    }

    Ok(order)
}

/// Reconstruct the `string_table` verbatim from the `StringTable` batch.
fn decode_string_table(rb: &RecordBatch) -> Result<Vec<String>> {
    let order = interned_table_row_order(rb, "string")?;
    let values = rb
        .column_by_name(consts::STRING_TABLE_VALUE)
        .map(StringArrayAccessor::try_new)
        .transpose()?;

    Ok(order
        .iter()
        .map(|&row| {
            values
                .as_ref()
                .and_then(|col| col.str_at(row))
                .unwrap_or_default()
                .to_string()
        })
        .collect())
}

/// Reconstruct the `function_table` verbatim from the `FunctionTable` batch.
fn decode_function_table(rb: &RecordBatch) -> Result<Vec<Function>> {
    let order = interned_table_row_order(rb, "function")?;
    let name_strindex = get_i32_array_opt(rb, consts::NAME_STRINDEX)?;
    let system_name_strindex = get_i32_array_opt(rb, consts::SYSTEM_NAME_STRINDEX)?;
    let filename_strindex = get_i32_array_opt(rb, consts::FILENAME_STRINDEX)?;
    let start_line = get_i64_array_opt(rb, consts::START_LINE)?;

    Ok(order
        .iter()
        .map(|&row| Function {
            name_strindex: name_strindex.value_at(row).unwrap_or_default(),
            system_name_strindex: system_name_strindex.value_at(row).unwrap_or_default(),
            filename_strindex: filename_strindex.value_at(row).unwrap_or_default(),
            start_line: start_line.value_at(row).unwrap_or_default(),
        })
        .collect())
}

/// Reconstruct the `mapping_table` verbatim from the `MappingTable` batch.
fn decode_mapping_table(rb: &RecordBatch) -> Result<Vec<Mapping>> {
    let order = interned_table_row_order(rb, "mapping")?;
    let memory_start = get_u64_array_opt(rb, consts::MEMORY_START)?;
    let memory_limit = get_u64_array_opt(rb, consts::MEMORY_LIMIT)?;
    let file_offset = get_u64_array_opt(rb, consts::FILE_OFFSET)?;
    let filename_strindex = get_i32_array_opt(rb, consts::FILENAME_STRINDEX)?;
    let has_functions = get_bool_array_opt(rb, consts::HAS_FUNCTIONS)?;
    let has_filenames = get_bool_array_opt(rb, consts::HAS_FILENAMES)?;
    let has_line_numbers = get_bool_array_opt(rb, consts::HAS_LINE_NUMBERS)?;
    let has_inline_frames = get_bool_array_opt(rb, consts::HAS_INLINE_FRAMES)?;
    let attribute_indices = get_list_array_opt(rb, consts::ATTRIBUTE_INDICES)?;

    order
        .iter()
        .map(|&row| {
            Ok(Mapping {
                memory_start: memory_start.value_at(row).unwrap_or_default(),
                memory_limit: memory_limit.value_at(row).unwrap_or_default(),
                file_offset: file_offset.value_at(row).unwrap_or_default(),
                filename_strindex: filename_strindex.value_at(row).unwrap_or_default(),
                attribute_indices: i32_list_at(attribute_indices, row)?,
                has_functions: has_functions.value_at(row).unwrap_or_default(),
                has_filenames: has_filenames.value_at(row).unwrap_or_default(),
                has_line_numbers: has_line_numbers.value_at(row).unwrap_or_default(),
                has_inline_frames: has_inline_frames.value_at(row).unwrap_or_default(),
            })
        })
        .collect()
}

/// Decode one location's `line` list row into `Line` structs.
fn lines_at(list: Option<&ListArray>, index: usize) -> Result<Vec<Line>> {
    let Some(list) = list else {
        return Ok(Vec::new());
    };
    let Some(values) = struct_list_at(list, index)? else {
        return Ok(Vec::new());
    };
    let structs = values
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| Error::ColumnDataTypeMismatch {
            name: consts::LINE.into(),
            expect: DataType::Struct(arrow::datatypes::Fields::empty()),
            actual: values.data_type().clone(),
        })?;
    let accessor = StructColumnAccessor::new(structs);
    let function_index = accessor.primitive_column_op::<Int32Type>(consts::FUNCTION_INDEX)?;
    let line = accessor.primitive_column_op::<Int64Type>(consts::LINE)?;
    let column = accessor.primitive_column_op::<Int64Type>(consts::COLUMN)?;

    Ok((0..structs.len())
        .map(|i| Line {
            function_index: function_index.value_at(i).unwrap_or_default(),
            line: line.value_at(i).unwrap_or_default(),
            column: column.value_at(i).unwrap_or_default(),
        })
        .collect())
}

/// Reconstruct the `location_table` verbatim from the `LocationTable` batch.
fn decode_location_table(rb: &RecordBatch) -> Result<Vec<Location>> {
    let order = interned_table_row_order(rb, "location")?;
    // `mapping_index` is proto `optional`: null => `None`, value (incl. `0`)
    // => `Some(value)` — presence survives the round trip.
    let mapping_index = get_i32_array_opt(rb, consts::MAPPING_INDEX)?;
    let address = get_u64_array_opt(rb, consts::ADDRESS)?;
    let is_folded = get_bool_array_opt(rb, consts::IS_FOLDED)?;
    let attribute_indices = get_list_array_opt(rb, consts::ATTRIBUTE_INDICES)?;
    let line = get_list_array_opt(rb, consts::LINE)?;

    order
        .iter()
        .map(|&row| {
            Ok(Location {
                mapping_index: mapping_index.value_at(row),
                address: address.value_at(row).unwrap_or_default(),
                line: lines_at(line, row)?,
                is_folded: is_folded.value_at(row).unwrap_or_default(),
                attribute_indices: i32_list_at(attribute_indices, row)?,
            })
        })
        .collect()
}

/// Reconstruct the `link_table` verbatim from the `LinkTable` batch. Null
/// fixed-size-binary ids (the encoder's representation of wrong-length or
/// empty input) decode back to empty bytes.
fn decode_link_table(rb: &RecordBatch) -> Result<Vec<Link>> {
    let order = interned_table_row_order(rb, "link")?;
    let trace_id = rb
        .column_by_name(consts::TRACE_ID)
        .map(ByteArrayAccessor::try_new)
        .transpose()?;
    let span_id = rb
        .column_by_name(consts::SPAN_ID)
        .map(ByteArrayAccessor::try_new)
        .transpose()?;

    Ok(order
        .iter()
        .map(|&row| Link {
            trace_id: trace_id
                .as_ref()
                .and_then(|col| col.slice_at(row))
                .map(<[u8]>::to_vec)
                .unwrap_or_default(),
            span_id: span_id
                .as_ref()
                .and_then(|col| col.slice_at(row))
                .map(<[u8]>::to_vec)
                .unwrap_or_default(),
        })
        .collect())
}

/// Reconstruct a single `KeyValue` from the shared attribute value lanes.
///
/// This deliberately reuses [`encode_any_value`] — the shared attribute
/// decode used by every signal, including the CBOR `ser` lane for
/// `Map`/`Slice` values — by serializing one `KeyValue` message into a
/// scratch buffer and prost-decoding it back into the struct.
fn decode_key_value_at(
    attr_key: &MaybeDictArrayAccessor<'_, StringArray>,
    anyval_arrays: &AnyValueArrays<'_>,
    index: usize,
    scratch: &mut ProtoBuffer,
) -> Result<KeyValue> {
    scratch.clear();
    if let Some(key) = attr_key.str_at(index) {
        scratch.encode_string(KEY_VALUE_KEY, key)?;
    }
    if let Some(value_type) = anyval_arrays.attr_type.value_at(index) {
        if let Ok(value_type) = AttributeValueType::try_from(value_type) {
            scratch.encode_len_delimited(KEY_VALUE_VALUE, |scratch| {
                encode_any_value(anyval_arrays, index, value_type, scratch)
            })?;
        }
    }

    KeyValue::decode(scratch.as_ref()).map_err(|e| Error::UnexpectedRecordBatchState {
        reason: format!("failed to decode reconstructed profiles attribute: {e}"),
    })
}

/// Reconstruct the `attribute_table` verbatim from the `AttributeTable`
/// batch.
fn decode_attribute_table(rb: &RecordBatch) -> Result<Vec<KeyValue>> {
    let order = interned_table_row_order(rb, "attribute")?;
    let attr_key = MaybeDictArrayAccessor::<StringArray>::try_new(get_required_array(
        rb,
        consts::ATTRIBUTE_KEY,
    )?)?;
    let anyval_arrays = AnyValueArrays::try_from(rb)?;
    let mut scratch = ProtoBuffer::default();

    order
        .iter()
        .map(|&row| decode_key_value_at(&attr_key, &anyval_arrays, row, &mut scratch))
        .collect()
}

/// Reconstruct the `attribute_units` table verbatim from the
/// `AttributeUnits` batch.
fn decode_attribute_units(rb: &RecordBatch) -> Result<Vec<AttributeUnit>> {
    let order = interned_table_row_order(rb, "attribute-units")?;
    let attribute_key_strindex = get_i32_array_opt(rb, consts::ATTRIBUTE_KEY_STRINDEX)?;
    let unit_strindex = get_i32_array_opt(rb, consts::UNIT_STRINDEX)?;

    Ok(order
        .iter()
        .map(|&row| AttributeUnit {
            attribute_key_strindex: attribute_key_strindex.value_at(row).unwrap_or_default(),
            unit_strindex: unit_strindex.value_at(row).unwrap_or_default(),
        })
        .collect())
}

/* ─────────────────────────── root + sample arrays ────────────────────── */

/// Accessors for the nullable `period_type` struct column.
struct PeriodTypeArrays<'a> {
    structs: &'a StructArray,
    type_strindex: Option<&'a Int32Array>,
    unit_strindex: Option<&'a Int32Array>,
    aggregation_temporality: Option<&'a Int32Array>,
}

impl<'a> TryFrom<&'a StructArray> for PeriodTypeArrays<'a> {
    type Error = Error;

    fn try_from(structs: &'a StructArray) -> Result<Self> {
        let accessor = StructColumnAccessor::new(structs);
        Ok(Self {
            structs,
            type_strindex: accessor.primitive_column_op::<Int32Type>(consts::TYPE_STRINDEX)?,
            unit_strindex: accessor.primitive_column_op::<Int32Type>(consts::UNIT_STRINDEX)?,
            aggregation_temporality: accessor
                .primitive_column_op::<Int32Type>(consts::AGGREGATION_TEMPORALITY)?,
        })
    }
}

impl PeriodTypeArrays<'_> {
    /// `period_type` is a proto `optional` message: a null struct row means
    /// "not present" and decodes to `None`, a valid row to `Some(value)`.
    fn value_at(&self, index: usize) -> Option<ValueType> {
        if self.structs.is_null(index) {
            return None;
        }
        Some(ValueType {
            type_strindex: self.type_strindex.value_at(index).unwrap_or_default(),
            unit_strindex: self.unit_strindex.value_at(index).unwrap_or_default(),
            aggregation_temporality: self
                .aggregation_temporality
                .value_at(index)
                .unwrap_or_default(),
        })
    }
}

/// Accessors for the columns of the `Profiles` root record batch (the
/// resource/scope struct columns are accessed through the shared
/// [`ResourceArrays`]/[`ScopeArrays`]).
struct ProfilesArrays<'a> {
    id: Option<&'a UInt16Array>,
    /// the `ScopeProfiles.schema_url`, flattened onto each profile row
    schema_url: Option<StringArrayAccessor<'a>>,
    time_nanos: Option<&'a TimestampNanosecondArray>,
    duration_nanos: Option<&'a Int64Array>,
    period: Option<&'a Int64Array>,
    period_type: Option<PeriodTypeArrays<'a>>,
    default_sample_type_index: Option<&'a Int32Array>,
    profile_id: Option<ByteArrayAccessor<'a>>,
    dropped_attributes_count: Option<&'a UInt32Array>,
    original_payload_format: Option<StringArrayAccessor<'a>>,
    original_payload: Option<ByteArrayAccessor<'a>>,
    sample_type: Option<&'a ListArray>,
    location_indices: Option<&'a ListArray>,
    comment_strindices: Option<&'a ListArray>,
    attribute_indices: Option<&'a ListArray>,
}

impl<'a> TryFrom<&'a RecordBatch> for ProfilesArrays<'a> {
    type Error = Error;

    fn try_from(rb: &'a RecordBatch) -> Result<Self> {
        let period_type = rb
            .column_by_name(consts::PERIOD_TYPE)
            .map(|arr| {
                let structs = arr.as_any().downcast_ref::<StructArray>().ok_or_else(|| {
                    Error::ColumnDataTypeMismatch {
                        name: consts::PERIOD_TYPE.into(),
                        expect: DataType::Struct(arrow::datatypes::Fields::empty()),
                        actual: arr.data_type().clone(),
                    }
                })?;
                PeriodTypeArrays::try_from(structs)
            })
            .transpose()?;

        Ok(Self {
            id: get_u16_array_opt(rb, consts::ID)?,
            schema_url: rb
                .column_by_name(consts::SCHEMA_URL)
                .map(StringArrayAccessor::try_new)
                .transpose()?,
            time_nanos: get_timestamp_nanosecond_array_opt(rb, consts::TIME_NANOS)?,
            duration_nanos: get_i64_array_opt(rb, consts::DURATION_NANOS)?,
            period: get_i64_array_opt(rb, consts::PERIOD)?,
            period_type,
            default_sample_type_index: get_i32_array_opt(rb, consts::DEFAULT_SAMPLE_TYPE_INDEX)?,
            profile_id: rb
                .column_by_name(consts::PROFILE_ID)
                .map(ByteArrayAccessor::try_new)
                .transpose()?,
            dropped_attributes_count: get_u32_array_opt(rb, consts::DROPPED_ATTRIBUTES_COUNT)?,
            original_payload_format: rb
                .column_by_name(consts::ORIGINAL_PAYLOAD_FORMAT)
                .map(StringArrayAccessor::try_new)
                .transpose()?,
            original_payload: rb
                .column_by_name(consts::ORIGINAL_PAYLOAD)
                .map(ByteArrayAccessor::try_new)
                .transpose()?,
            sample_type: get_list_array_opt(rb, consts::SAMPLE_TYPE)?,
            location_indices: get_list_array_opt(rb, consts::LOCATION_INDICES)?,
            comment_strindices: get_list_array_opt(rb, consts::COMMENT_STRINDICES)?,
            attribute_indices: get_list_array_opt(rb, consts::ATTRIBUTE_INDICES)?,
        })
    }
}

/// Accessors for the columns of the `Sample` child record batch.
struct SampleArrays<'a> {
    parent_id: MaybeDictArrayAccessor<'a, UInt16Array>,
    locations_start_index: Option<&'a Int32Array>,
    locations_length: Option<&'a Int32Array>,
    value: Option<&'a ListArray>,
    attribute_indices: Option<&'a ListArray>,
    link_index: Option<&'a Int32Array>,
    timestamps_unix_nano: Option<&'a ListArray>,
}

impl<'a> TryFrom<&'a RecordBatch> for SampleArrays<'a> {
    type Error = Error;

    fn try_from(rb: &'a RecordBatch) -> Result<Self> {
        Ok(Self {
            parent_id: MaybeDictArrayAccessor::<UInt16Array>::try_new(get_required_array(
                rb,
                consts::PARENT_ID,
            )?)?,
            locations_start_index: get_i32_array_opt(rb, consts::LOCATIONS_START_INDEX)?,
            locations_length: get_i32_array_opt(rb, consts::LOCATIONS_LENGTH)?,
            value: get_list_array_opt(rb, consts::SAMPLE_VALUE)?,
            attribute_indices: get_list_array_opt(rb, consts::ATTRIBUTE_INDICES)?,
            // `link_index` is proto `optional`: null => `None`, value (incl.
            // `0`, a valid link table row) => `Some(value)`.
            link_index: get_i32_array_opt(rb, consts::LINK_INDEX)?,
            timestamps_unix_nano: get_list_array_opt(rb, consts::TIMESTAMPS_UNIX_NANO)?,
        })
    }
}

impl SampleArrays<'_> {
    fn sample_at(&self, index: usize) -> Result<Sample> {
        Ok(Sample {
            locations_start_index: self
                .locations_start_index
                .value_at(index)
                .unwrap_or_default(),
            locations_length: self.locations_length.value_at(index).unwrap_or_default(),
            value: i64_list_at(self.value, index)?,
            attribute_indices: i32_list_at(self.attribute_indices, index)?,
            link_index: self.link_index.value_at(index),
            timestamps_unix_nano: u64_list_at(self.timestamps_unix_nano, index)?,
        })
    }
}

/// A parent-id joined child batch together with its parent-id -> row-indices
/// grouping.
///
/// Unlike the byte-streaming decoders (which use `ChildIndexIter` over a
/// sorted cursor), the grouping here is built with a single stable pass in
/// row order, so the relative order of a parent's children is preserved
/// exactly — a requirement for byte-identical round trips.
struct Grouped<T> {
    arrays: T,
    groups: HashMap<u16, Vec<usize>>,
}

fn group_by_parent_id(
    parent_ids: &MaybeDictArrayAccessor<'_, UInt16Array>,
    num_rows: usize,
) -> HashMap<u16, Vec<usize>> {
    let mut groups: HashMap<u16, Vec<usize>> = HashMap::new();
    for row in 0..num_rows {
        if let Some(parent_id) = parent_ids.value_at(row) {
            groups.entry(parent_id).or_default().push(row);
        }
    }
    groups
}

/* ─────────────────────────── tree reconstruction ─────────────────────── */

/// Reconstructs the resource -> scope -> profile -> sample tree from the
/// root batch (visited in `(resource id, scope id, id)` cursor order) and
/// the parent-id joined child batches.
struct TreeDecoder<'a> {
    profiles: ProfilesArrays<'a>,
    resource: ResourceArrays<'a>,
    scope: ScopeArrays<'a>,
    resource_attrs: Option<Grouped<Attribute16Arrays<'a>>>,
    scope_attrs: Option<Grouped<Attribute16Arrays<'a>>>,
    samples: Option<Grouped<SampleArrays<'a>>>,
    scratch: ProtoBuffer,
}

impl<'a> TreeDecoder<'a> {
    fn try_new(otap_batch: &'a OtapArrowRecords, root_rb: &'a RecordBatch) -> Result<Self> {
        let attrs_arrays = |payload_type| -> Result<Option<Grouped<Attribute16Arrays<'a>>>> {
            otap_batch
                .get(payload_type)
                .map(|rb| {
                    let arrays = Attribute16Arrays::try_from(rb)?;
                    let groups = group_by_parent_id(&arrays.parent_id, rb.num_rows());
                    Ok(Grouped { arrays, groups })
                })
                .transpose()
        };

        let samples = otap_batch
            .get(ArrowPayloadType::Sample)
            .map(|rb| {
                let arrays = SampleArrays::try_from(rb)?;
                let groups = group_by_parent_id(&arrays.parent_id, rb.num_rows());
                Ok::<_, Error>(Grouped { arrays, groups })
            })
            .transpose()?;

        Ok(Self {
            profiles: ProfilesArrays::try_from(root_rb)?,
            resource: ResourceArrays::try_from(root_rb)?,
            scope: ScopeArrays::try_from(root_rb)?,
            resource_attrs: attrs_arrays(ArrowPayloadType::ResourceAttrs)?,
            scope_attrs: attrs_arrays(ArrowPayloadType::ScopeAttrs)?,
            samples,
            scratch: ProtoBuffer::default(),
        })
    }

    /// Decode all attributes belonging to `parent_id` from a parent-id
    /// joined attrs batch, in stored row order.
    fn attrs_for(
        attrs: Option<&Grouped<Attribute16Arrays<'_>>>,
        parent_id: Option<u16>,
        scratch: &mut ProtoBuffer,
    ) -> Result<Vec<KeyValue>> {
        let (Some(attrs), Some(parent_id)) = (attrs, parent_id) else {
            return Ok(Vec::new());
        };
        let Some(rows) = attrs.groups.get(&parent_id) else {
            return Ok(Vec::new());
        };
        rows.iter()
            .map(|&row| {
                decode_key_value_at(
                    &attrs.arrays.attr_key,
                    &attrs.arrays.anyval_arrays,
                    row,
                    scratch,
                )
            })
            .collect()
    }

    /// Decode the `Resource` for the root row at `index`.
    ///
    /// OTAP has no presence bit for the resource: `None` and
    /// `Some(Resource::default())` encode identically (the encoder's
    /// default-eliding builders may even materialize default values as
    /// literal zeros once a column exists for other rows), so a resource
    /// with no observable content — no attributes and a zero dropped count —
    /// decodes to `None`.
    fn resource_at(&mut self, index: usize) -> Result<Option<Resource>> {
        let resource_id = self.resource.id.value_at(index);
        let attributes =
            Self::attrs_for(self.resource_attrs.as_ref(), resource_id, &mut self.scratch)?;
        let dropped_attributes_count = self
            .resource
            .dropped_attributes_count
            .value_at(index)
            .unwrap_or_default();

        if attributes.is_empty() && dropped_attributes_count == 0 {
            return Ok(None);
        }
        Ok(Some(Resource {
            attributes,
            dropped_attributes_count,
            ..Default::default()
        }))
    }

    /// Decode the `InstrumentationScope` for the root row at `index`, with
    /// the same no-observable-content => `None` canonicalization as
    /// [`Self::resource_at`].
    fn scope_at(&mut self, index: usize) -> Result<Option<InstrumentationScope>> {
        let scope_id = self.scope.id.value_at(index);
        let name = self
            .scope
            .name
            .as_ref()
            .and_then(|col| col.str_at(index))
            .unwrap_or_default();
        let version = self
            .scope
            .version
            .as_ref()
            .and_then(|col| col.str_at(index))
            .unwrap_or_default();
        let attributes = Self::attrs_for(self.scope_attrs.as_ref(), scope_id, &mut self.scratch)?;
        let dropped_attributes_count = self
            .scope
            .dropped_attributes_count
            .value_at(index)
            .unwrap_or_default();

        if name.is_empty()
            && version.is_empty()
            && attributes.is_empty()
            && dropped_attributes_count == 0
        {
            return Ok(None);
        }
        Ok(Some(InstrumentationScope {
            name: name.to_string(),
            version: version.to_string(),
            attributes,
            dropped_attributes_count,
        }))
    }

    /// Decode the profile's `sample_type` list row into `ValueType`s.
    fn sample_types_at(&self, index: usize) -> Result<Vec<ValueType>> {
        let Some(list) = self.profiles.sample_type else {
            return Ok(Vec::new());
        };
        let Some(values) = struct_list_at(list, index)? else {
            return Ok(Vec::new());
        };
        let structs = values
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| Error::ColumnDataTypeMismatch {
                name: consts::SAMPLE_TYPE.into(),
                expect: DataType::Struct(arrow::datatypes::Fields::empty()),
                actual: values.data_type().clone(),
            })?;
        let accessor = StructColumnAccessor::new(structs);
        let type_strindex = accessor.primitive_column_op::<Int32Type>(consts::TYPE_STRINDEX)?;
        let unit_strindex = accessor.primitive_column_op::<Int32Type>(consts::UNIT_STRINDEX)?;
        let aggregation_temporality =
            accessor.primitive_column_op::<Int32Type>(consts::AGGREGATION_TEMPORALITY)?;

        Ok((0..structs.len())
            .map(|i| ValueType {
                type_strindex: type_strindex.value_at(i).unwrap_or_default(),
                unit_strindex: unit_strindex.value_at(i).unwrap_or_default(),
                aggregation_temporality: aggregation_temporality.value_at(i).unwrap_or_default(),
            })
            .collect())
    }

    /// Decode the `Profile` at root row `index` (including its samples).
    fn profile_at(&self, index: usize) -> Result<Profile> {
        let samples = match (&self.samples, self.profiles.id.value_at(index)) {
            (Some(samples), Some(profile_row_id)) => samples
                .groups
                .get(&profile_row_id)
                .map(|rows| {
                    rows.iter()
                        .map(|&row| samples.arrays.sample_at(row))
                        .collect::<Result<Vec<_>>>()
                })
                .transpose()?
                .unwrap_or_default(),
            _ => Vec::new(),
        };

        Ok(Profile {
            sample_type: self.sample_types_at(index)?,
            sample: samples,
            location_indices: i32_list_at(self.profiles.location_indices, index)?,
            time_nanos: self.profiles.time_nanos.value_at(index).unwrap_or_default(),
            duration_nanos: self
                .profiles
                .duration_nanos
                .value_at(index)
                .unwrap_or_default(),
            period_type: self
                .profiles
                .period_type
                .as_ref()
                .and_then(|arrays| arrays.value_at(index)),
            period: self.profiles.period.value_at(index).unwrap_or_default(),
            comment_strindices: i32_list_at(self.profiles.comment_strindices, index)?,
            default_sample_type_index: self
                .profiles
                .default_sample_type_index
                .value_at(index)
                .unwrap_or_default(),
            profile_id: self
                .profiles
                .profile_id
                .as_ref()
                .and_then(|col| col.slice_at(index))
                .map(<[u8]>::to_vec)
                .unwrap_or_default(),
            dropped_attributes_count: self
                .profiles
                .dropped_attributes_count
                .value_at(index)
                .unwrap_or_default(),
            original_payload_format: self
                .profiles
                .original_payload_format
                .as_ref()
                .and_then(|col| col.str_at(index))
                .unwrap_or_default()
                .to_string(),
            original_payload: self
                .profiles
                .original_payload
                .as_ref()
                .and_then(|col| col.slice_at(index))
                .map(<[u8]>::to_vec)
                .unwrap_or_default(),
            attribute_indices: i32_list_at(self.profiles.attribute_indices, index)?,
        })
    }

    /// Decode one `ScopeProfiles` group: consumes root rows from the cursor
    /// until the scope id (or the enclosing resource id) changes.
    fn next_scope_profiles(
        &mut self,
        cursor: &mut SortedBatchCursor,
        resource_id: Option<u16>,
    ) -> Result<ScopeProfiles> {
        let index = cursor.curr_index().expect("cursor not finished");
        let scope_id = self.scope.id.value_at(index);

        let mut profiles = Vec::new();
        loop {
            let profile_index = cursor.curr_index().expect("cursor not finished");
            profiles.push(self.profile_at(profile_index)?);
            cursor.advance();

            if cursor.finished() {
                break;
            }
            // Safety: we've just checked above that the cursor isn't finished
            let next_index = cursor.curr_index().expect("cursor not finished");
            if self.scope.id.value_at(next_index) != scope_id
                || self.resource.id.value_at(next_index) != resource_id
            {
                break;
            }
        }

        Ok(ScopeProfiles {
            scope: self.scope_at(index)?,
            profiles,
            schema_url: self
                .profiles
                .schema_url
                .as_ref()
                .and_then(|col| col.str_at(index))
                .unwrap_or_default()
                .to_string(),
        })
    }

    /// Decode one `ResourceProfiles` group: consumes root rows from the
    /// cursor until the resource id changes.
    fn next_resource_profiles(
        &mut self,
        cursor: &mut SortedBatchCursor,
    ) -> Result<ResourceProfiles> {
        let index = cursor.curr_index().expect("cursor not finished");
        let resource_id = self.resource.id.value_at(index);

        let mut scope_profiles = Vec::new();
        loop {
            scope_profiles.push(self.next_scope_profiles(cursor, resource_id)?);

            if cursor.finished() {
                break;
            }
            // Safety: we've just checked above that the cursor isn't finished
            let next_index = cursor.curr_index().expect("cursor not finished");
            if self.resource.id.value_at(next_index) != resource_id {
                break;
            }
        }

        Ok(ResourceProfiles {
            resource: self.resource_at(index)?,
            scope_profiles,
            schema_url: self
                .resource
                .schema_url
                .as_ref()
                .and_then(|col| col.str_at(index))
                .unwrap_or_default()
                .to_string(),
        })
    }
}

/* ─────────────────────────── index validation ────────────────────────── */

fn check_index(
    index: i32,
    table_len: usize,
    column: &'static str,
    table: &'static str,
) -> Result<()> {
    // non-negative widening conversion: negative indices fail `try_from`
    // instead of wrapping around
    let in_range = usize::try_from(index)
        .map(|idx| idx < table_len)
        .unwrap_or(false);
    if in_range {
        Ok(())
    } else {
        Err(Error::InvalidProfilesTableIndex {
            column,
            index: i64::from(index),
            table,
            table_len,
        })
    }
}

fn check_value_type(vt: &ValueType, n_strings: usize, column: &'static str) -> Result<()> {
    check_index(vt.type_strindex, n_strings, column, "string")?;
    check_index(vt.unit_strindex, n_strings, column, "string")
}

/// Validate every table-referencing index of the reconstructed message
/// against the length of the table it references.
fn validate_table_indices(data: &ProfilesData) -> Result<()> {
    let n_strings = data.string_table.len();
    let n_functions = data.function_table.len();
    let n_mappings = data.mapping_table.len();
    let n_locations = data.location_table.len();
    let n_links = data.link_table.len();
    let n_attributes = data.attribute_table.len();

    for resource_profiles in &data.resource_profiles {
        for scope_profiles in &resource_profiles.scope_profiles {
            for profile in &scope_profiles.profiles {
                for value_type in &profile.sample_type {
                    check_value_type(value_type, n_strings, "sample_type")?;
                }
                if let Some(period_type) = &profile.period_type {
                    check_value_type(period_type, n_strings, "period_type")?;
                }
                for &index in &profile.location_indices {
                    check_index(index, n_locations, "location_indices", "location")?;
                }
                for &index in &profile.comment_strindices {
                    check_index(index, n_strings, "comment_strindices", "string")?;
                }
                for &index in &profile.attribute_indices {
                    check_index(index, n_attributes, "attribute_indices", "attribute")?;
                }
                for sample in &profile.sample {
                    for &index in &sample.attribute_indices {
                        check_index(index, n_attributes, "attribute_indices", "attribute")?;
                    }
                    if let Some(index) = sample.link_index {
                        check_index(index, n_links, "link_index", "link")?;
                    }
                }
            }
        }
    }

    for mapping in &data.mapping_table {
        check_index(
            mapping.filename_strindex,
            n_strings,
            "filename_strindex",
            "string",
        )?;
        for &index in &mapping.attribute_indices {
            check_index(index, n_attributes, "attribute_indices", "attribute")?;
        }
    }

    for location in &data.location_table {
        if let Some(index) = location.mapping_index {
            check_index(index, n_mappings, "mapping_index", "mapping")?;
        }
        for &index in &location.attribute_indices {
            check_index(index, n_attributes, "attribute_indices", "attribute")?;
        }
        for line in &location.line {
            check_index(
                line.function_index,
                n_functions,
                "function_index",
                "function",
            )?;
        }
    }

    for function in &data.function_table {
        check_index(function.name_strindex, n_strings, "name_strindex", "string")?;
        check_index(
            function.system_name_strindex,
            n_strings,
            "system_name_strindex",
            "string",
        )?;
        check_index(
            function.filename_strindex,
            n_strings,
            "filename_strindex",
            "string",
        )?;
    }

    for unit in &data.attribute_units {
        check_index(
            unit.attribute_key_strindex,
            n_strings,
            "attribute_key_strindex",
            "string",
        )?;
        check_index(unit.unit_strindex, n_strings, "unit_strindex", "string")?;
    }

    Ok(())
}

/* ─────────────────────────── public entry points ─────────────────────── */

/// Reconstruct the OTLP [`ProfilesData`] message from an OTAP profiles batch.
///
/// Any transport-optimized encodings are removed first (via the shared
/// [`OtapArrowRecords::decode_transport_optimized_ids`] path — a no-op for
/// batches whose id columns are stamped `plain`). A missing `Profiles` root
/// is treated as zero profile rows (matching the other signals); the
/// interned tables are still reconstructed verbatim if present.
pub fn profiles_data_from(otap_batch: &mut OtapArrowRecords) -> Result<ProfilesData> {
    otap_batch.decode_transport_optimized_ids()?;

    let mut profiles_data = ProfilesData {
        resource_profiles: Vec::new(),
        mapping_table: otap_batch
            .get(ArrowPayloadType::MappingTable)
            .map(decode_mapping_table)
            .transpose()?
            .unwrap_or_default(),
        location_table: otap_batch
            .get(ArrowPayloadType::LocationTable)
            .map(decode_location_table)
            .transpose()?
            .unwrap_or_default(),
        function_table: otap_batch
            .get(ArrowPayloadType::FunctionTable)
            .map(decode_function_table)
            .transpose()?
            .unwrap_or_default(),
        link_table: otap_batch
            .get(ArrowPayloadType::LinkTable)
            .map(decode_link_table)
            .transpose()?
            .unwrap_or_default(),
        string_table: otap_batch
            .get(ArrowPayloadType::StringTable)
            .map(decode_string_table)
            .transpose()?
            .unwrap_or_default(),
        attribute_table: otap_batch
            .get(ArrowPayloadType::AttributeTable)
            .map(decode_attribute_table)
            .transpose()?
            .unwrap_or_default(),
        attribute_units: otap_batch
            .get(ArrowPayloadType::AttributeUnits)
            .map(decode_attribute_units)
            .transpose()?
            .unwrap_or_default(),
    };

    if let Some(root_rb) = otap_batch.get(ArrowPayloadType::Profiles) {
        let mut decoder = TreeDecoder::try_new(otap_batch, root_rb)?;

        let mut batch_sorter = BatchSorter::new();
        let mut root_cursor = SortedBatchCursor::new();
        batch_sorter.init_cursor_for_root_batch(root_rb, &mut root_cursor)?;

        while !root_cursor.finished() {
            profiles_data
                .resource_profiles
                .push(decoder.next_resource_profiles(&mut root_cursor)?);
        }
    }

    validate_table_indices(&profiles_data)?;

    Ok(profiles_data)
}

/// Encoder from OTAP profiles record batches to OTLP proto bytes.
///
/// Emits **`ProfilesData`** bytes (see the module docs for why this is the
/// wire-compatible, lossless choice). Unlike the other signals' encoders,
/// which stream hand-written proto bytes, this reconstructs the
/// [`ProfilesData`] struct and serializes it with prost, so the output is
/// always in prost's canonical form — the property the byte-identical
/// round-trip tests pin. The reconstruction is stateless, so this type
/// carries no cursors or buffers.
#[derive(Default)]
pub struct ProfilesProtoBytesEncoder;

impl ProfilesProtoBytesEncoder {
    /// Create a new instance of `ProfilesProtoBytesEncoder`
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

impl ProtoBytesEncoder for ProfilesProtoBytesEncoder {
    fn encode(
        &mut self,
        otap_batch: &mut OtapArrowRecords,
        result_buf: &mut ProtoBuffer,
    ) -> Result<()> {
        let profiles_data = profiles_data_from(otap_batch)?;
        let bytes = profiles_data.encode_to_vec();
        result_buf.extend_from_slice(&bytes)?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use arrow::array::{Int32Builder, ListBuilder};
    use bytes::Bytes;
    use pretty_assertions::assert_eq;

    use super::*;
    use crate::Consumer;
    use crate::encode::encode_profiles_otap_batch;
    use crate::encode::producer::Producer;
    use crate::otap::Profiles;
    use crate::testing::fixtures::profiles_data_full_fidelity;
    use crate::{OtapPayload, OtlpProtoBytes, TryIntoWithOptions};

    /// THE round-trip: OTLP `ProfilesData` prost bytes -> OTAP record
    /// batches -> decode -> re-encoded prost bytes must be byte-identical to
    /// the original, through the same `OtlpProtoBytes`/`OtapArrowRecords`
    /// conversions the pipeline uses.
    #[test]
    fn test_profiles_otlp_round_trip_is_byte_identical() {
        let profiles_data = profiles_data_full_fidelity();
        let original_bytes = profiles_data.encode_to_vec();

        // OTLP bytes -> OTAP record batches (the Stage 1 ingest direction)
        let payload: OtapPayload =
            OtlpProtoBytes::ExportProfilesRequest(Bytes::from(original_bytes.clone())).into();
        let otap_batch: OtapArrowRecords = payload.try_into_with_default().unwrap();
        assert!(matches!(otap_batch, OtapArrowRecords::Profiles(_)));

        // OTAP record batches -> OTLP bytes (this change)
        let payload: OtapPayload = otap_batch.into();
        let otlp_bytes: OtlpProtoBytes = payload.try_into_with_default().unwrap();
        assert!(matches!(
            otlp_bytes,
            OtlpProtoBytes::ExportProfilesRequest(_)
        ));

        // compare the structs first: on failure this pinpoints the exact
        // field, which a raw byte diff cannot
        let reconstructed = ProfilesData::decode(otlp_bytes.as_bytes()).unwrap();
        assert_eq!(profiles_data, reconstructed);

        // the crowning assertion: byte-identical round trip
        assert_eq!(original_bytes.as_slice(), otlp_bytes.as_bytes());

        // pin the wire-compatibility claim from the module docs: a reader
        // that only understands `ExportProfilesServiceRequest` still parses
        // `resource_profiles` (field 1) from the emitted `ProfilesData`
        // bytes, skipping the dictionary table fields as unknown fields
        let as_request =
            crate::proto::opentelemetry::collector::profiles::v1development::ExportProfilesServiceRequest::decode(
                otlp_bytes.as_bytes(),
            )
            .unwrap();
        assert_eq!(
            as_request.resource_profiles,
            profiles_data.resource_profiles
        );
    }

    /// The same round trip through the REAL wire: the OTAP batches travel
    /// through `Producer::produce_bar` (which applies the
    /// transport-optimized encodings and serializes to Arrow IPC) and
    /// `Consumer::consume_profiles_batches`, and the result must still
    /// re-encode to the original bytes.
    #[test]
    fn test_profiles_wire_round_trip_is_byte_identical() {
        let profiles_data = profiles_data_full_fidelity();
        let original_bytes = profiles_data.encode_to_vec();

        let mut otap_batch = encode_profiles_otap_batch(&profiles_data).unwrap();
        let mut producer = Producer::new();
        let mut bar = producer.produce_bar(&mut otap_batch).unwrap();

        let mut consumer = Consumer::default();
        let result = consumer.consume_profiles_batches(&mut bar).unwrap();

        assert_eq!(profiles_data, result);
        assert_eq!(original_bytes, result.encode_to_vec());
    }

    /// An empty OTAP profiles batch decodes to the default (empty) message,
    /// which serializes to zero bytes — matching the encode of an empty
    /// `ProfilesData`.
    #[test]
    fn test_profiles_decode_empty() {
        let mut otap_batch = OtapArrowRecords::Profiles(Profiles::default());
        let result = profiles_data_from(&mut otap_batch).unwrap();
        assert_eq!(result, ProfilesData::default());
        assert!(result.encode_to_vec().is_empty());
    }

    /// Replace one column of the `Sample` record batch, keeping the schema
    /// otherwise intact.
    fn patch_sample_column(
        otap_batch: &mut OtapArrowRecords,
        column: &str,
        new_column: arrow::array::ArrayRef,
    ) {
        let rb = otap_batch.get(ArrowPayloadType::Sample).expect("samples");
        let schema = rb.schema();
        let (column_index, _) = schema.column_with_name(column).expect("column exists");
        let mut columns = rb.columns().to_vec();
        columns[column_index] = new_column;
        let rb = RecordBatch::try_new(schema, columns).expect("valid patched batch");
        otap_batch
            .set(ArrowPayloadType::Sample, rb)
            .expect("set patched batch");
    }

    /// A negative index in a plain index column must produce the typed
    /// out-of-range error — never a panic or a wrap-around.
    #[test]
    fn test_profiles_decode_negative_index_is_typed_error() {
        let profiles_data = profiles_data_full_fidelity();
        let mut otap_batch = encode_profiles_otap_batch(&profiles_data).unwrap();

        patch_sample_column(
            &mut otap_batch,
            consts::LINK_INDEX,
            Arc::new(Int32Array::from(vec![Some(-1), None, None, None])),
        );

        let result = profiles_data_from(&mut otap_batch);
        assert!(
            matches!(
                result,
                Err(Error::InvalidProfilesTableIndex {
                    column: "link_index",
                    index: -1,
                    table: "link",
                    ..
                })
            ),
            "expected typed out-of-range error, got: {result:?}"
        );
    }

    /// An index pointing past the end of the referenced table must produce
    /// the typed out-of-range error.
    #[test]
    fn test_profiles_decode_out_of_range_index_is_typed_error() {
        let profiles_data = profiles_data_full_fidelity();
        let n_links = profiles_data.link_table.len();
        let mut otap_batch = encode_profiles_otap_batch(&profiles_data).unwrap();

        patch_sample_column(
            &mut otap_batch,
            consts::LINK_INDEX,
            Arc::new(Int32Array::from(vec![Some(999), None, None, None])),
        );

        let result = profiles_data_from(&mut otap_batch);
        match result {
            Err(Error::InvalidProfilesTableIndex {
                column: "link_index",
                index: 999,
                table: "link",
                table_len,
            }) => assert_eq!(table_len, n_links),
            other => panic!("expected typed out-of-range error, got: {other:?}"),
        }
    }

    /// A negative index inside a `List<Int32>` index column must also
    /// produce the typed error.
    #[test]
    fn test_profiles_decode_negative_list_index_is_typed_error() {
        let profiles_data = profiles_data_full_fidelity();
        let mut otap_batch = encode_profiles_otap_batch(&profiles_data).unwrap();

        // 4 sample rows: give the first a negative attribute table index
        let mut builder = ListBuilder::new(Int32Builder::new());
        builder.append_value([Some(-3)]);
        for _ in 1..4 {
            builder.append_value(std::iter::empty::<Option<i32>>());
        }
        patch_sample_column(
            &mut otap_batch,
            consts::ATTRIBUTE_INDICES,
            Arc::new(builder.finish()),
        );

        let result = profiles_data_from(&mut otap_batch);
        assert!(
            matches!(
                result,
                Err(Error::InvalidProfilesTableIndex {
                    column: "attribute_indices",
                    index: -3,
                    table: "attribute",
                    ..
                })
            ),
            "expected typed out-of-range error, got: {result:?}"
        );
    }

    /// Fixture-coverage audit: assert that every proto field of
    /// Profile/Sample/Mapping/Location/Line/Function/Link/ValueType/
    /// AttributeUnit is exercised by at least one fixture element — both a
    /// non-default value (so the field demonstrably round-trips) and, where
    /// the encode contract elides defaults, a default value (so the elision
    /// demonstrably decodes back). If a field is added to the proto without
    /// extending the fixture, the byte-identical round trip above would pass
    /// vacuously; this test is the guard against such silent blind spots.
    #[test]
    fn test_fixture_exercises_every_profiles_proto_field() {
        let data = profiles_data_full_fidelity();

        // ProfilesData: every table populated
        assert!(data.resource_profiles.len() >= 2);
        assert!(!data.mapping_table.is_empty());
        assert!(!data.location_table.is_empty());
        assert!(!data.function_table.is_empty());
        assert!(!data.link_table.is_empty());
        assert!(!data.string_table.is_empty());
        assert!(!data.attribute_table.is_empty());
        assert!(!data.attribute_units.is_empty());

        // ResourceProfiles / ScopeProfiles: presence + default coverage
        let rps = &data.resource_profiles;
        assert!(rps.iter().any(|rp| rp.resource.is_some()));
        assert!(rps.iter().any(|rp| rp.resource.is_none()));
        assert!(rps.iter().any(|rp| !rp.schema_url.is_empty()));
        assert!(rps.iter().any(|rp| rp.schema_url.is_empty()));
        let sps: Vec<_> = rps.iter().flat_map(|rp| &rp.scope_profiles).collect();
        assert!(sps.iter().any(|sp| sp.scope.is_some()));
        assert!(sps.iter().any(|sp| sp.scope.is_none()));
        assert!(sps.iter().any(|sp| !sp.schema_url.is_empty()));
        assert!(sps.iter().any(|sp| sp.schema_url.is_empty()));
        let scope = sps
            .iter()
            .find_map(|sp| sp.scope.as_ref())
            .expect("some scope");
        assert!(!scope.name.is_empty());
        assert!(!scope.version.is_empty());
        assert!(!scope.attributes.is_empty());
        assert!(scope.dropped_attributes_count != 0);
        let resource = rps
            .iter()
            .find_map(|rp| rp.resource.as_ref())
            .expect("some resource");
        assert!(!resource.attributes.is_empty());
        assert!(resource.dropped_attributes_count != 0);

        // Profile: every field non-default somewhere AND default somewhere
        let profiles: Vec<_> = sps.iter().flat_map(|sp| &sp.profiles).collect();
        assert!(profiles.iter().any(|p| !p.sample_type.is_empty()));
        assert!(profiles.iter().any(|p| p.sample_type.is_empty()));
        assert!(profiles.iter().all(|p| !p.sample.is_empty()));
        assert!(profiles.iter().any(|p| !p.location_indices.is_empty()));
        assert!(profiles.iter().any(|p| p.location_indices.is_empty()));
        assert!(profiles.iter().any(|p| p.time_nanos != 0));
        assert!(profiles.iter().any(|p| p.time_nanos == 0));
        assert!(profiles.iter().any(|p| p.duration_nanos != 0));
        assert!(profiles.iter().any(|p| p.duration_nanos == 0));
        assert!(profiles.iter().any(|p| p.period_type.is_some()));
        assert!(profiles.iter().any(|p| p.period_type.is_none()));
        assert!(profiles.iter().any(|p| p.period != 0));
        assert!(profiles.iter().any(|p| p.period == 0));
        assert!(profiles.iter().any(|p| !p.comment_strindices.is_empty()));
        assert!(profiles.iter().any(|p| p.comment_strindices.is_empty()));
        assert!(profiles.iter().any(|p| p.default_sample_type_index != 0));
        assert!(profiles.iter().any(|p| p.default_sample_type_index == 0));
        assert!(profiles.iter().any(|p| p.profile_id.len() == 16));
        assert!(profiles.iter().any(|p| p.profile_id.is_empty()));
        assert!(profiles.iter().any(|p| p.dropped_attributes_count != 0));
        assert!(profiles.iter().any(|p| p.dropped_attributes_count == 0));
        assert!(
            profiles
                .iter()
                .any(|p| !p.original_payload_format.is_empty())
        );
        assert!(
            profiles
                .iter()
                .any(|p| p.original_payload_format.is_empty())
        );
        assert!(profiles.iter().any(|p| !p.original_payload.is_empty()));
        assert!(profiles.iter().any(|p| p.original_payload.is_empty()));
        assert!(profiles.iter().any(|p| !p.attribute_indices.is_empty()));
        assert!(profiles.iter().any(|p| p.attribute_indices.is_empty()));

        // Sample: every field, incl. the `link_index` presence pins
        let samples: Vec<_> = profiles.iter().flat_map(|p| &p.sample).collect();
        assert!(samples.iter().any(|s| s.locations_start_index != 0));
        assert!(samples.iter().any(|s| s.locations_start_index == 0));
        assert!(samples.iter().any(|s| s.locations_length != 0));
        assert!(samples.iter().any(|s| s.locations_length == 0));
        assert!(samples.iter().any(|s| !s.value.is_empty()));
        assert!(samples.iter().any(|s| s.value.is_empty()));
        assert!(samples.iter().any(|s| !s.attribute_indices.is_empty()));
        assert!(samples.iter().any(|s| s.attribute_indices.is_empty()));
        assert!(samples.iter().any(|s| s.link_index == Some(0)));
        assert!(
            samples
                .iter()
                .any(|s| matches!(s.link_index, Some(idx) if idx != 0))
        );
        assert!(samples.iter().any(|s| s.link_index.is_none()));
        assert!(samples.iter().any(|s| !s.timestamps_unix_nano.is_empty()));
        assert!(samples.iter().any(|s| s.timestamps_unix_nano.is_empty()));

        // Mapping: every field, incl. an all-false boolean column
        // (`has_filenames`/`has_inline_frames` pin the skip_all_false
        // omission decoding back to false)
        let mappings = &data.mapping_table;
        assert!(mappings.iter().any(|m| m.memory_start != 0));
        assert!(mappings.iter().any(|m| m.memory_start == 0));
        assert!(mappings.iter().any(|m| m.memory_limit != 0));
        assert!(mappings.iter().any(|m| m.memory_limit == 0));
        assert!(mappings.iter().any(|m| m.file_offset != 0));
        assert!(mappings.iter().any(|m| m.file_offset == 0));
        assert!(mappings.iter().any(|m| m.filename_strindex != 0));
        assert!(mappings.iter().any(|m| m.filename_strindex == 0));
        assert!(mappings.iter().any(|m| !m.attribute_indices.is_empty()));
        assert!(mappings.iter().any(|m| m.attribute_indices.is_empty()));
        assert!(mappings.iter().any(|m| m.has_functions));
        assert!(mappings.iter().any(|m| !m.has_functions));
        assert!(mappings.iter().all(|m| !m.has_filenames));
        assert!(mappings.iter().any(|m| m.has_line_numbers));
        assert!(mappings.iter().any(|m| !m.has_line_numbers));
        assert!(mappings.iter().all(|m| !m.has_inline_frames));

        // Location: every field, incl. the `mapping_index` presence pins
        let locations = &data.location_table;
        assert!(locations.iter().any(|l| l.mapping_index == Some(0)));
        assert!(
            locations
                .iter()
                .any(|l| matches!(l.mapping_index, Some(idx) if idx != 0))
        );
        assert!(locations.iter().any(|l| l.mapping_index.is_none()));
        assert!(locations.iter().any(|l| l.address != 0));
        assert!(locations.iter().any(|l| l.address == 0));
        assert!(locations.iter().any(|l| l.line.len() > 1));
        assert!(locations.iter().any(|l| l.line.is_empty()));
        assert!(locations.iter().any(|l| l.is_folded));
        assert!(locations.iter().any(|l| !l.is_folded));
        assert!(locations.iter().any(|l| !l.attribute_indices.is_empty()));
        assert!(locations.iter().any(|l| l.attribute_indices.is_empty()));

        // Line: every field, zeros included
        let lines: Vec<_> = locations.iter().flat_map(|l| &l.line).collect();
        assert!(lines.iter().any(|l| l.function_index != 0));
        assert!(lines.iter().any(|l| l.function_index == 0));
        assert!(lines.iter().all(|l| l.line != 0));
        assert!(lines.iter().any(|l| l.column != 0));
        assert!(lines.iter().any(|l| l.column == 0));

        // Function: every field, zeros included
        let functions = &data.function_table;
        assert!(functions.iter().any(|f| f.name_strindex != 0));
        assert!(functions.iter().any(|f| f.system_name_strindex != 0));
        assert!(functions.iter().any(|f| f.system_name_strindex == 0));
        assert!(functions.iter().any(|f| f.filename_strindex != 0));
        assert!(functions.iter().any(|f| f.filename_strindex == 0));
        assert!(functions.iter().any(|f| f.start_line != 0));
        assert!(functions.iter().any(|f| f.start_line == 0));

        // Link: nonzero / empty / all-zero-but-well-formed ids
        let links = &data.link_table;
        assert!(
            links
                .iter()
                .any(|l| l.trace_id.len() == 16 && l.trace_id.iter().any(|&b| b != 0))
        );
        assert!(links.iter().any(|l| l.trace_id.is_empty()));
        assert!(
            links
                .iter()
                .any(|l| l.trace_id.len() == 16 && l.trace_id.iter().all(|&b| b == 0))
        );
        assert!(
            links
                .iter()
                .any(|l| l.span_id.len() == 8 && l.span_id.iter().any(|&b| b != 0))
        );
        assert!(links.iter().any(|l| l.span_id.is_empty()));

        // ValueType: all three aggregation temporalities, strindex coverage
        let value_types: Vec<_> = profiles
            .iter()
            .flat_map(|p| p.sample_type.iter().chain(p.period_type.iter()))
            .collect();
        assert!(value_types.iter().any(|vt| vt.type_strindex != 0));
        assert!(value_types.iter().any(|vt| vt.unit_strindex != 0));
        for temporality in [0, 1, 2] {
            assert!(
                value_types
                    .iter()
                    .any(|vt| vt.aggregation_temporality == temporality),
                "aggregation_temporality {temporality} not exercised"
            );
        }

        // AttributeUnit: non-default rows and the all-default row
        let units = &data.attribute_units;
        assert!(units.iter().any(|u| u.attribute_key_strindex != 0));
        assert!(units.iter().any(|u| u.attribute_key_strindex == 0));
        assert!(units.iter().any(|u| u.unit_strindex != 0));
        assert!(units.iter().any(|u| u.unit_strindex == 0));

        // attribute value lanes: all round-trippable AnyValue variants incl.
        // the exactly-zero int (default-elision pin); Map/Slice exercise the
        // CBOR ser lane
        use crate::proto::opentelemetry::common::v1::any_value::Value;
        let attr_values: Vec<_> = data
            .attribute_table
            .iter()
            .chain(resource.attributes.iter())
            .chain(scope.attributes.iter())
            .filter_map(|kv| kv.value.as_ref().and_then(|v| v.value.as_ref()))
            .collect();
        assert!(
            attr_values
                .iter()
                .any(|v| matches!(v, Value::StringValue(s) if !s.is_empty()))
        );
        assert!(
            attr_values
                .iter()
                .any(|v| matches!(v, Value::IntValue(i) if *i != 0))
        );
        assert!(attr_values.iter().any(|v| matches!(v, Value::IntValue(0))));
        assert!(
            attr_values
                .iter()
                .any(|v| matches!(v, Value::DoubleValue(d) if *d != 0.0))
        );
        assert!(
            attr_values
                .iter()
                .any(|v| matches!(v, Value::BoolValue(true)))
        );
        assert!(
            attr_values
                .iter()
                .any(|v| matches!(v, Value::BytesValue(b) if !b.is_empty()))
        );
        assert!(
            attr_values
                .iter()
                .any(|v| matches!(v, Value::KvlistValue(_)))
        );
        assert!(
            attr_values
                .iter()
                .any(|v| matches!(v, Value::ArrayValue(_)))
        );

        // string table: the conventional "" at index 0
        assert_eq!(data.string_table[0], "");
    }
}
