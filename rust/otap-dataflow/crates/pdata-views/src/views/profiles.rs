// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! **Backend-agnostic, zero-copy view traits for OTLP Profiles.**
//!
//! ```text
//! ProfilesDataView
//! ├─ ResourceProfilesView
//! │  │  resource::ResourceView
//! │  └─ ScopeProfilesView
//! │     │  common::InstrumentationScopeView
//! │     └─ ProfileView
//! │        ├─ ValueTypeView (sample_type / period_type)
//! │        └─ SampleView
//! └─ dictionary tables (referenced by index from the profile tree):
//!    ├─ MappingView
//!    ├─ LocationView ─ LineView
//!    ├─ FunctionView
//!    ├─ LinkView
//!    ├─ string_table (flat strings)
//!    ├─ attribute_table (common::AttributeView)
//!    └─ AttributeUnitView
//! ```
//!
//! Unlike the other signals, OTLP profiles is already a dictionary-normalized
//! model: `ProfilesData` carries flat lookup tables and the profile/sample
//! tree references entries by index (`*_strindex`, `*_index`, `*_indices`
//! fields). Index `0` is a valid table reference everywhere, so — in contrast
//! to some of the other signal views which map proto3 zero values to `None` —
//! these traits expose all index and numeric fields verbatim. Only fields that
//! are `optional` in the proto (and therefore carry real presence semantics,
//! like [`SampleView::link_index`] and [`LocationView::mapping_index`]) are
//! surfaced as `Option`.

use crate::views::{
    common::{AttributeView, InstrumentationScopeView, Str},
    resource::ResourceView,
};

/// View for the top level ProfilesData.
///
/// In addition to the `resource_profiles` tree, this view exposes the flat
/// dictionary tables that the profile tree references by index. Implementations
/// must preserve table iteration order: the position of an entry within its
/// table is its identity.
pub trait ProfilesDataView {
    /// The `ResourceProfilesView` trait associated with this impl of the `ProfilesDataView` trait.
    type ResourceProfiles<'res>: ResourceProfilesView
    where
        Self: 'res;

    /// The associated iterator type for the resources. The iterator will yield borrowed
    /// references that must live as long as the input lifetime 'res
    type ResourcesIter<'res>: Iterator<Item = Self::ResourceProfiles<'res>>
    where
        Self: 'res;

    /// The `MappingView` trait associated with this impl of the `ProfilesDataView` trait.
    type Mapping<'map>: MappingView
    where
        Self: 'map;

    /// The associated iterator type for the mapping table.
    type MappingsIter<'map>: Iterator<Item = Self::Mapping<'map>>
    where
        Self: 'map;

    /// The `LocationView` trait associated with this impl of the `ProfilesDataView` trait.
    type Location<'loc>: LocationView
    where
        Self: 'loc;

    /// The associated iterator type for the location table.
    type LocationsIter<'loc>: Iterator<Item = Self::Location<'loc>>
    where
        Self: 'loc;

    /// The `FunctionView` trait associated with this impl of the `ProfilesDataView` trait.
    type Function<'fun>: FunctionView
    where
        Self: 'fun;

    /// The associated iterator type for the function table.
    type FunctionsIter<'fun>: Iterator<Item = Self::Function<'fun>>
    where
        Self: 'fun;

    /// The `LinkView` trait associated with this impl of the `ProfilesDataView` trait.
    type Link<'lnk>: LinkView
    where
        Self: 'lnk;

    /// The associated iterator type for the link table.
    type LinksIter<'lnk>: Iterator<Item = Self::Link<'lnk>>
    where
        Self: 'lnk;

    /// The associated iterator type for the string table.
    type StringsIter<'str>: Iterator<Item = Str<'str>>
    where
        Self: 'str;

    /// The `AttributeView` trait associated with this impl of the `ProfilesDataView` trait,
    /// used for the entries of the attribute table.
    type Attribute<'att>: AttributeView
    where
        Self: 'att;

    /// The associated iterator type for the attribute table.
    type AttributesIter<'att>: Iterator<Item = Self::Attribute<'att>>
    where
        Self: 'att;

    /// The `AttributeUnitView` trait associated with this impl of the `ProfilesDataView` trait.
    type AttributeUnit<'unt>: AttributeUnitView
    where
        Self: 'unt;

    /// The associated iterator type for the attribute units table.
    type AttributeUnitsIter<'unt>: Iterator<Item = Self::AttributeUnit<'unt>>
    where
        Self: 'unt;

    /// Iterator yielding the `ResourceProfiles` contained in this `ProfilesData`.
    fn resources(&self) -> Self::ResourcesIter<'_>;

    /// Iterator yielding the entries of the mapping lookup table, in table order.
    fn mapping_table(&self) -> Self::MappingsIter<'_>;

    /// Iterator yielding the entries of the location lookup table, in table order.
    fn location_table(&self) -> Self::LocationsIter<'_>;

    /// Iterator yielding the entries of the function lookup table, in table order.
    fn function_table(&self) -> Self::FunctionsIter<'_>;

    /// Iterator yielding the entries of the link lookup table, in table order.
    fn link_table(&self) -> Self::LinksIter<'_>;

    /// Iterator yielding the entries of the string lookup table, in table order.
    /// Empty strings are valid entries (by convention the table starts with one)
    /// and must be yielded verbatim.
    fn string_table(&self) -> Self::StringsIter<'_>;

    /// Iterator yielding the entries of the attribute lookup table, in table order.
    fn attribute_table(&self) -> Self::AttributesIter<'_>;

    /// Iterator yielding the entries of the attribute units table, in table order.
    fn attribute_units(&self) -> Self::AttributeUnitsIter<'_>;
}

/// View for ResourceProfiles
pub trait ResourceProfilesView {
    /// The `ResourceView` trait associated with this impl of the `ResourceProfilesView` trait.
    type Resource<'res>: ResourceView
    where
        Self: 'res;

    /// The `ScopeProfilesView` trait associated with this impl of the `ResourceProfilesView` trait.
    type ScopeProfiles<'scp>: ScopeProfilesView
    where
        Self: 'scp;

    /// The associated iterator type for this impl of the trait. The iterator will yield
    /// borrowed references that must live as long as the lifetime 'scp
    type ScopesIter<'scp>: Iterator<Item = Self::ScopeProfiles<'scp>>
    where
        Self: 'scp;

    /// Access the resource for the profiles contained in this `ResourceProfiles`. If this
    /// returns `None` it means the resource info is unknown.
    fn resource(&self) -> Option<Self::Resource<'_>>;

    /// Iterator yielding the `ScopeProfiles` that originate from this resource.
    fn scopes(&self) -> Self::ScopesIter<'_>;

    /// The schema URL for the resource. If the schema is not known, this returns `None`
    fn schema_url(&self) -> Option<Str<'_>>;
}

/// View for ScopeProfiles
pub trait ScopeProfilesView {
    /// The `InstrumentationScopeView` trait associated with this impl of the `ScopeProfilesView`
    /// trait.
    type Scope<'scp>: InstrumentationScopeView
    where
        Self: 'scp;

    /// The `ProfileView` trait associated with this impl of the `ScopeProfilesView` trait.
    type Profile<'prf>: ProfileView
    where
        Self: 'prf;

    /// The associated iterator type for this impl of the trait. The iterator will yield
    /// borrowed references that must live as long as the lifetime 'prf
    type ProfilesIter<'prf>: Iterator<Item = Self::Profile<'prf>>
    where
        Self: 'prf;

    /// Access the instrumentation scope for the profiles contained in this scope. If this
    /// returns `None` it means the scope is unknown
    fn scope(&self) -> Option<Self::Scope<'_>>;

    /// Iterator yielding the `Profile`s contained in this scope.
    fn profiles(&self) -> Self::ProfilesIter<'_>;

    /// The schema URL. This schema URL applies to all profiles returned by the `profiles`
    /// iterator. This method returns `None` if the schema URL is not known.
    fn schema_url(&self) -> Option<Str<'_>>;
}

/// View for a single Profile
pub trait ProfileView {
    /// The `ValueTypeView` trait associated with this impl of the `ProfileView` trait, used
    /// for both `sample_type` entries and `period_type`.
    type ValueType<'vty>: ValueTypeView
    where
        Self: 'vty;

    /// The associated iterator type for the sample types.
    type SampleTypesIter<'vty>: Iterator<Item = Self::ValueType<'vty>>
    where
        Self: 'vty;

    /// The `SampleView` trait associated with this impl of the `ProfileView` trait.
    type Sample<'smp>: SampleView
    where
        Self: 'smp;

    /// The associated iterator type for the samples.
    type SamplesIter<'smp>: Iterator<Item = Self::Sample<'smp>>
    where
        Self: 'smp;

    /// The associated iterator type for index lists (`location_indices`,
    /// `comment_strindices` and `attribute_indices`).
    type IndicesIter<'idx>: Iterator<Item = i32>
    where
        Self: 'idx;

    /// Iterator yielding the value types of the sample values, in order. There is one entry
    /// per dimension of [`SampleView::values`].
    fn sample_types(&self) -> Self::SampleTypesIter<'_>;

    /// Iterator yielding the samples of this profile.
    fn samples(&self) -> Self::SamplesIter<'_>;

    /// Indices into the location table for the locations referenced by this profile's samples
    /// (via `locations_start_index`/`locations_length`), yielded verbatim.
    fn location_indices(&self) -> Self::IndicesIter<'_>;

    /// The time of the profile in nanoseconds since the Unix epoch, verbatim (0 means unset
    /// in the proto but is copied as-is).
    fn time_nanos(&self) -> i64;

    /// Duration of the profile in nanoseconds, verbatim.
    fn duration_nanos(&self) -> i64;

    /// The kind of events between sampled occurrences. Returns `None` when the underlying
    /// message is not present.
    fn period_type(&self) -> Option<Self::ValueType<'_>>;

    /// The number of events between sampled occurrences, verbatim.
    fn period(&self) -> i64;

    /// Indices into the string table for free-form profile comments, yielded verbatim.
    fn comment_strindices(&self) -> Self::IndicesIter<'_>;

    /// Index into `sample_types` of the preferred sample value, verbatim.
    fn default_sample_type_index(&self) -> i32;

    /// The globally unique identifier of the profile, as raw bytes. A valid id is exactly
    /// 16 bytes; implementations return whatever the backend holds (possibly empty) and
    /// leave validation to the caller.
    fn profile_id(&self) -> &[u8];

    /// Access this profile's dropped attributes count. The value is 0 when no attributes
    /// were dropped.
    fn dropped_attributes_count(&self) -> u32;

    /// The format of `original_payload`. Returns `None` when unset (empty).
    fn original_payload_format(&self) -> Option<Str<'_>>;

    /// The original payload of the profile in its native format (possibly empty).
    fn original_payload(&self) -> &[u8];

    /// Indices into the attribute table for this profile's attributes, yielded verbatim.
    fn attribute_indices(&self) -> Self::IndicesIter<'_>;
}

/// View for a Sample
pub trait SampleView {
    /// The associated iterator type for `attribute_indices`.
    type IndicesIter<'idx>: Iterator<Item = i32>
    where
        Self: 'idx;

    /// The associated iterator type for the sample values.
    type ValuesIter<'val>: Iterator<Item = i64>
    where
        Self: 'val;

    /// The associated iterator type for the sample timestamps.
    type TimestampsIter<'tsp>: Iterator<Item = u64>
    where
        Self: 'tsp;

    /// Start index of this sample's stack within the owning profile's `location_indices`,
    /// verbatim.
    fn locations_start_index(&self) -> i32;

    /// Number of entries of this sample's stack within the owning profile's
    /// `location_indices`, verbatim.
    fn locations_length(&self) -> i32;

    /// Iterator yielding the values of this sample, one per entry of the owning profile's
    /// `sample_type`.
    fn values(&self) -> Self::ValuesIter<'_>;

    /// Indices into the attribute table for this sample's attributes, yielded verbatim.
    fn attribute_indices(&self) -> Self::IndicesIter<'_>;

    /// Index into the link table. `None` when the field is not present (this field is
    /// `optional` in the proto, so presence — including `Some(0)` — is meaningful).
    fn link_index(&self) -> Option<i32>;

    /// Iterator yielding the timestamps of when this sample was collected, in nanoseconds
    /// since the Unix epoch.
    fn timestamps_unix_nano(&self) -> Self::TimestampsIter<'_>;
}

/// View for a ValueType (the type/unit of a sample value or of the profile period)
pub trait ValueTypeView {
    /// Index into the string table of the type description, verbatim.
    fn type_strindex(&self) -> i32;

    /// Index into the string table of the unit description, verbatim.
    fn unit_strindex(&self) -> i32;

    /// The aggregation temporality of the values, as the raw proto enum value.
    fn aggregation_temporality(&self) -> i32;
}

/// View for an entry of the mapping lookup table
pub trait MappingView {
    /// The associated iterator type for `attribute_indices`.
    type IndicesIter<'idx>: Iterator<Item = i32>
    where
        Self: 'idx;

    /// Address at which the binary (or DLL) is loaded into memory, verbatim.
    fn memory_start(&self) -> u64;

    /// The limit of the address range occupied by this mapping, verbatim.
    fn memory_limit(&self) -> u64;

    /// Offset in the binary that corresponds to the first mapped address, verbatim.
    fn file_offset(&self) -> u64;

    /// Index into the string table of the object file name, verbatim.
    fn filename_strindex(&self) -> i32;

    /// Indices into the attribute table for this mapping's attributes, yielded verbatim.
    fn attribute_indices(&self) -> Self::IndicesIter<'_>;

    /// Whether the mapping has function information.
    fn has_functions(&self) -> bool;

    /// Whether the mapping has filename information.
    fn has_filenames(&self) -> bool;

    /// Whether the mapping has line number information.
    fn has_line_numbers(&self) -> bool;

    /// Whether the mapping has inline frame information.
    fn has_inline_frames(&self) -> bool;
}

/// View for an entry of the location lookup table
pub trait LocationView {
    /// The `LineView` trait associated with this impl of the `LocationView` trait.
    type Line<'lin>: LineView
    where
        Self: 'lin;

    /// The associated iterator type for the lines of this location.
    type LinesIter<'lin>: Iterator<Item = Self::Line<'lin>>
    where
        Self: 'lin;

    /// The associated iterator type for `attribute_indices`.
    type IndicesIter<'idx>: Iterator<Item = i32>
    where
        Self: 'idx;

    /// Index into the mapping table. `None` when the field is not present (this field is
    /// `optional` in the proto, so presence — including `Some(0)` — is meaningful).
    fn mapping_index(&self) -> Option<i32>;

    /// The instruction address of this location, verbatim.
    fn address(&self) -> u64;

    /// Iterator yielding the line information for this location (multiple entries describe
    /// inlined functions).
    fn lines(&self) -> Self::LinesIter<'_>;

    /// Whether multiple symbols map to this location as a result of identical-code-folding.
    fn is_folded(&self) -> bool;

    /// Indices into the attribute table for this location's attributes, yielded verbatim.
    fn attribute_indices(&self) -> Self::IndicesIter<'_>;
}

/// View for the line information of a location
pub trait LineView {
    /// Index into the function table of the function of this line, verbatim.
    fn function_index(&self) -> i32;

    /// The source code line number, verbatim.
    fn line(&self) -> i64;

    /// The source code column number, verbatim.
    fn column(&self) -> i64;
}

/// View for an entry of the function lookup table
pub trait FunctionView {
    /// Index into the string table of the function name, verbatim.
    fn name_strindex(&self) -> i32;

    /// Index into the string table of the mangled (system) function name, verbatim.
    fn system_name_strindex(&self) -> i32;

    /// Index into the string table of the source file name, verbatim.
    fn filename_strindex(&self) -> i32;

    /// The source code line number of the function's start, verbatim.
    fn start_line(&self) -> i64;
}

/// View for an entry of the link lookup table (a connection from a profile sample to a trace span)
pub trait LinkView {
    /// The trace id of the linked span, as raw bytes. A valid trace id is exactly 16 bytes;
    /// implementations return whatever the backend holds (possibly empty) and leave
    /// validation to the caller.
    fn trace_id(&self) -> &[u8];

    /// The span id of the linked span, as raw bytes. A valid span id is exactly 8 bytes;
    /// implementations return whatever the backend holds (possibly empty) and leave
    /// validation to the caller.
    fn span_id(&self) -> &[u8];
}

/// View for an entry of the attribute units table (the unit associated with an attribute key)
pub trait AttributeUnitView {
    /// Index into the string table of the attribute key, verbatim.
    fn attribute_key_strindex(&self) -> i32;

    /// Index into the string table of the unit of the attribute value, verbatim.
    fn unit_strindex(&self) -> i32;
}
