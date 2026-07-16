// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

pub const ID: &str = "id";
pub const PARENT_ID: &str = "parent_id";

pub const METRIC_TYPE: &str = "metric_type";

pub const RESOURCE_METRICS: &str = "resource_metrics";
pub const TIME_UNIX_NANO: &str = "time_unix_nano";
pub const START_TIME_UNIX_NANO: &str = "start_time_unix_nano";
pub const DURATION_TIME_UNIX_NANO: &str = "duration_time_unix_nano";
pub const OBSERVED_TIME_UNIX_NANO: &str = "observed_time_unix_nano";
pub const SEVERITY_NUMBER: &str = "severity_number";
pub const SEVERITY_TEXT: &str = "severity_text";
pub const DROPPED_ATTRIBUTES_COUNT: &str = "dropped_attributes_count";
pub const DROPPED_EVENTS_COUNT: &str = "dropped_events_count";
pub const DROPPED_LINKS_COUNT: &str = "dropped_links_count";
pub const EVENT_NAME: &str = "event_name";
pub const FLAGS: &str = "flags";
pub const TRACE_ID: &str = "trace_id";
pub const TRACE_STATE: &str = "trace_state";
pub const SPAN_ID: &str = "span_id";
pub const PARENT_SPAN_ID: &str = "parent_span_id";
pub const ATTRIBUTES: &str = "attributes";
pub const RESOURCE: &str = "resource";
pub const SCOPE_METRICS: &str = "scope_metrics";
pub const SCOPE: &str = "scope";
pub const NAME: &str = "name";
pub const KIND: &str = "kind";
pub const VERSION: &str = "version";
pub const BODY: &str = "body";
pub const STATUS: &str = "status";
pub const DESCRIPTION: &str = "description";
pub const UNIT: &str = "unit";
pub const DATA: &str = "data";
pub const STATUS_MESSAGE: &str = "status_message";
pub const STATUS_CODE: &str = "code";
pub const SUMMARY_COUNT: &str = "count";
pub const SUMMARY_SUM: &str = "sum";
pub const SUMMARY_QUANTILE_VALUES: &str = "quantile";
pub const SUMMARY_QUANTILE: &str = "quantile";
pub const SUMMARY_VALUE: &str = "value";
pub const METRIC_VALUE: &str = "value";
pub const INT_VALUE: &str = "int_value";
pub const DOUBLE_VALUE: &str = "double_value";
pub const HISTOGRAM_COUNT: &str = "count";
pub const HISTOGRAM_SUM: &str = "sum";
pub const HISTOGRAM_MIN: &str = "min";
pub const HISTOGRAM_MAX: &str = "max";
pub const HISTOGRAM_BUCKET_COUNTS: &str = "bucket_counts";
pub const HISTOGRAM_EXPLICIT_BOUNDS: &str = "explicit_bounds";
pub const EXP_HISTOGRAM_SCALE: &str = "scale";
pub const EXP_HISTOGRAM_ZERO_COUNT: &str = "zero_count";
pub const EXP_HISTOGRAM_ZERO_THRESHOLD: &str = "zero_threshold";
pub const EXP_HISTOGRAM_POSITIVE: &str = "positive";
pub const EXP_HISTOGRAM_NEGATIVE: &str = "negative";
pub const EXP_HISTOGRAM_OFFSET: &str = "offset";
pub const EXP_HISTOGRAM_BUCKET_COUNTS: &str = "bucket_counts";
pub const SCHEMA_URL: &str = "schema_url";
pub const I64_METRIC_VALUE: &str = "i64";
pub const F64_METRIC_VALUE: &str = "f64";
pub const EXEMPLARS: &str = "exemplars";
pub const IS_MONOTONIC: &str = "is_monotonic";
pub const AGGREGATION_TEMPORALITY: &str = "aggregation_temporality";

pub const ATTRIBUTE_KEY: &str = "key";
pub const ATTRIBUTE_TYPE: &str = "type";
pub const ATTRIBUTE_STR: &str = "str";
pub const ATTRIBUTE_INT: &str = "int";
pub const ATTRIBUTE_DOUBLE: &str = "double";
pub const ATTRIBUTE_BOOL: &str = "bool";
pub const ATTRIBUTE_BYTES: &str = "bytes";
pub const ATTRIBUTE_SER: &str = "ser";

// Profiles
pub const TIME_NANOS: &str = "time_nanos";
pub const DURATION_NANOS: &str = "duration_nanos";
pub const PERIOD: &str = "period";
pub const PERIOD_TYPE: &str = "period_type";
pub const TYPE_STRINDEX: &str = "type_strindex";
pub const UNIT_STRINDEX: &str = "unit_strindex";
pub const DEFAULT_SAMPLE_TYPE_INDEX: &str = "default_sample_type_index";
pub const PROFILE_ID: &str = "profile_id";
pub const ORIGINAL_PAYLOAD_FORMAT: &str = "original_payload_format";
pub const ORIGINAL_PAYLOAD: &str = "original_payload";
pub const SAMPLE_TYPE: &str = "sample_type";
pub const LOCATION_INDICES: &str = "location_indices";
pub const COMMENT_STRINDICES: &str = "comment_strindices";
pub const ATTRIBUTE_INDICES: &str = "attribute_indices";
pub const LOCATIONS_START_INDEX: &str = "locations_start_index";
pub const LOCATIONS_LENGTH: &str = "locations_length";
pub const SAMPLE_VALUE: &str = "value";
pub const LINK_INDEX: &str = "link_index";
pub const TIMESTAMPS_UNIX_NANO: &str = "timestamps_unix_nano";
pub const STRING_TABLE_VALUE: &str = "value";
pub const MEMORY_START: &str = "memory_start";
pub const MEMORY_LIMIT: &str = "memory_limit";
pub const FILE_OFFSET: &str = "file_offset";
pub const FILENAME_STRINDEX: &str = "filename_strindex";
pub const HAS_FUNCTIONS: &str = "has_functions";
pub const HAS_FILENAMES: &str = "has_filenames";
pub const HAS_LINE_NUMBERS: &str = "has_line_numbers";
pub const HAS_INLINE_FRAMES: &str = "has_inline_frames";
pub const MAPPING_INDEX: &str = "mapping_index";
pub const ADDRESS: &str = "address";
pub const IS_FOLDED: &str = "is_folded";
pub const LINE: &str = "line";
pub const FUNCTION_INDEX: &str = "function_index";
pub const COLUMN: &str = "column";
pub const NAME_STRINDEX: &str = "name_strindex";
pub const SYSTEM_NAME_STRINDEX: &str = "system_name_strindex";
pub const START_LINE: &str = "start_line";
pub const ATTRIBUTE_KEY_STRINDEX: &str = "attribute_key_strindex";

pub mod metadata {
    /// schema metadata for which columns the record batch is sorted by
    pub const SORT_COLUMNS: &str = "sort_columns";

    /// field metadata key for the encoding of some column
    pub const COLUMN_ENCODING: &str = "encoding";

    pub mod encodings {
        /// delta encoding
        pub const DELTA: &str = "delta";

        /// plain encoding - e.g. the values in the array are not encoded
        pub const PLAIN: &str = "plain";

        /// quasi-delta encoding - in this encoding scheme subsequent runs of matching columns
        /// will have the parent_id field delta encoded.
        pub const QUASI_DELTA: &str = "quasidelta";
    }
}
