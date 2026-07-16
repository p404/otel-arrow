// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! This module contains the implementation of the pdata View traits for proto message structs
//! from otlp profiles.proto (v1development).
//!
//! Note that unlike the other signals, the profiles views expose numeric and index fields
//! verbatim (0 included) because OTLP profiles is a dictionary-normalized model where index
//! `0` is a valid table reference. See the trait docs in
//! `otap_df_pdata_views::views::profiles` for details.

use crate::proto::opentelemetry::profiles::v1development::{
    AttributeUnit, Function, Line, Link, Location, Mapping, Profile, ProfilesData,
    ResourceProfiles, Sample, ScopeProfiles, ValueType as ProtoValueType,
};

use crate::views::{
    otlp::proto::common::{KeyValueIter, ObjInstrumentationScope, ObjKeyValue},
    otlp::proto::resource::ObjResource,
    otlp::proto::wrappers::{GenericIterator, GenericObj, Wraps},
};
use otap_df_pdata_views::views::common::Str;
use otap_df_pdata_views::views::profiles::{
    AttributeUnitView, FunctionView, LineView, LinkView, LocationView, MappingView, ProfileView,
    ProfilesDataView, ResourceProfilesView, SampleView, ScopeProfilesView, ValueTypeView,
};

/* ───────────────────────────── VIEW WRAPPERS (zero-alloc) ────────────── */

/// Lightweight wrapper around `ResourceProfiles` that implements `ResourceProfilesView`
pub type ObjResourceProfiles<'a> = GenericObj<'a, ResourceProfiles>;

/// Lightweight wrapper around `ScopeProfiles` that implements `ScopeProfilesView`
pub type ObjScopeProfiles<'a> = GenericObj<'a, ScopeProfiles>;

/// Lightweight wrapper around `Profile` that implements `ProfileView`
pub type ObjProfile<'a> = GenericObj<'a, Profile>;

/// Lightweight wrapper around `Sample` that implements `SampleView`
pub type ObjSample<'a> = GenericObj<'a, Sample>;

/// Lightweight wrapper around `ValueType` that implements `ValueTypeView`
pub type ObjValueType<'a> = GenericObj<'a, ProtoValueType>;

/// Lightweight wrapper around `Mapping` that implements `MappingView`
pub type ObjMapping<'a> = GenericObj<'a, Mapping>;

/// Lightweight wrapper around `Location` that implements `LocationView`
pub type ObjLocation<'a> = GenericObj<'a, Location>;

/// Lightweight wrapper around `Line` that implements `LineView`
pub type ObjLine<'a> = GenericObj<'a, Line>;

/// Lightweight wrapper around `Function` that implements `FunctionView`
pub type ObjFunction<'a> = GenericObj<'a, Function>;

/// Lightweight wrapper around `Link` that implements `LinkView`
pub type ObjLink<'a> = GenericObj<'a, Link>;

/// Lightweight wrapper around `AttributeUnit` that implements `AttributeUnitView`
pub type ObjAttributeUnit<'a> = GenericObj<'a, AttributeUnit>;

/* ───────────────────────────── ADAPTER ITERATORS ─────────────────────── */

/// Iterator of `ObjResourceProfiles`. Used in the implementation of `ProfilesDataView` to get
/// an iterator of the resources contained in the profiles data.
pub type ResourceIter<'a> = GenericIterator<'a, ResourceProfiles, ObjResourceProfiles<'a>>;

/// Iterator of `ObjScopeProfiles`. Used in the implementation of `ResourceProfilesView` to get
/// an iterator of the instrumentation scopes for some resource.
pub type ScopeIter<'a> = GenericIterator<'a, ScopeProfiles, ObjScopeProfiles<'a>>;

/// Iterator of `ObjProfile`. Used in the implementation of `ScopeProfilesView` to get an
/// iterator of the profiles for some scope.
pub type ProfileIter<'a> = GenericIterator<'a, Profile, ObjProfile<'a>>;

/// Iterator of `ObjSample`. Used in the implementation of `ProfileView` to get an iterator of
/// the samples of some profile.
pub type SampleIter<'a> = GenericIterator<'a, Sample, ObjSample<'a>>;

/// Iterator of `ObjValueType`. Used in the implementation of `ProfileView` to get an iterator
/// of the sample types of some profile.
pub type ValueTypeIter<'a> = GenericIterator<'a, ProtoValueType, ObjValueType<'a>>;

/// Iterator of `ObjMapping`. Used in the implementation of `ProfilesDataView` to get an
/// iterator of the mapping table entries.
pub type MappingIter<'a> = GenericIterator<'a, Mapping, ObjMapping<'a>>;

/// Iterator of `ObjLocation`. Used in the implementation of `ProfilesDataView` to get an
/// iterator of the location table entries.
pub type LocationIter<'a> = GenericIterator<'a, Location, ObjLocation<'a>>;

/// Iterator of `ObjLine`. Used in the implementation of `LocationView` to get an iterator of
/// the lines of some location.
pub type LineIter<'a> = GenericIterator<'a, Line, ObjLine<'a>>;

/// Iterator of `ObjFunction`. Used in the implementation of `ProfilesDataView` to get an
/// iterator of the function table entries.
pub type FunctionIter<'a> = GenericIterator<'a, Function, ObjFunction<'a>>;

/// Iterator of `ObjLink`. Used in the implementation of `ProfilesDataView` to get an iterator
/// of the link table entries.
pub type LinkIter<'a> = GenericIterator<'a, Link, ObjLink<'a>>;

/// Iterator of `ObjAttributeUnit`. Used in the implementation of `ProfilesDataView` to get an
/// iterator of the attribute units table entries.
pub type AttributeUnitIter<'a> = GenericIterator<'a, AttributeUnit, ObjAttributeUnit<'a>>;

/// Iterator of verbatim `i32` index values copied out of a repeated proto field.
pub type IndicesIter<'a> = std::iter::Copied<std::slice::Iter<'a, i32>>;

/// Iterator of verbatim `i64` values copied out of a repeated proto field.
pub type I64sIter<'a> = std::iter::Copied<std::slice::Iter<'a, i64>>;

/// Iterator of verbatim `u64` values copied out of a repeated proto field.
pub type U64sIter<'a> = std::iter::Copied<std::slice::Iter<'a, u64>>;

/// Iterator over the string table yielding each entry as bytes, in table order.
pub type StringsIter<'a> = std::iter::Map<
    std::slice::Iter<'a, ::prost::alloc::string::String>,
    fn(&'a ::prost::alloc::string::String) -> Str<'a>,
>;

fn string_as_bytes(s: &::prost::alloc::string::String) -> Str<'_> {
    s.as_bytes()
}

/* ───────────────────────────── TRAIT IMPLEMENTATIONS ─────────────────── */

impl ProfilesDataView for ProfilesData {
    type ResourceProfiles<'a>
        = ObjResourceProfiles<'a>
    where
        Self: 'a;

    type ResourcesIter<'a>
        = ResourceIter<'a>
    where
        Self: 'a;

    type Mapping<'a>
        = ObjMapping<'a>
    where
        Self: 'a;

    type MappingsIter<'a>
        = MappingIter<'a>
    where
        Self: 'a;

    type Location<'a>
        = ObjLocation<'a>
    where
        Self: 'a;

    type LocationsIter<'a>
        = LocationIter<'a>
    where
        Self: 'a;

    type Function<'a>
        = ObjFunction<'a>
    where
        Self: 'a;

    type FunctionsIter<'a>
        = FunctionIter<'a>
    where
        Self: 'a;

    type Link<'a>
        = ObjLink<'a>
    where
        Self: 'a;

    type LinksIter<'a>
        = LinkIter<'a>
    where
        Self: 'a;

    type StringsIter<'a>
        = StringsIter<'a>
    where
        Self: 'a;

    type Attribute<'a>
        = ObjKeyValue<'a>
    where
        Self: 'a;

    type AttributesIter<'a>
        = KeyValueIter<'a>
    where
        Self: 'a;

    type AttributeUnit<'a>
        = ObjAttributeUnit<'a>
    where
        Self: 'a;

    type AttributeUnitsIter<'a>
        = AttributeUnitIter<'a>
    where
        Self: 'a;

    #[inline]
    fn resources(&self) -> Self::ResourcesIter<'_> {
        ResourceIter::new(self.resource_profiles.iter())
    }

    #[inline]
    fn mapping_table(&self) -> Self::MappingsIter<'_> {
        MappingIter::new(self.mapping_table.iter())
    }

    #[inline]
    fn location_table(&self) -> Self::LocationsIter<'_> {
        LocationIter::new(self.location_table.iter())
    }

    #[inline]
    fn function_table(&self) -> Self::FunctionsIter<'_> {
        FunctionIter::new(self.function_table.iter())
    }

    #[inline]
    fn link_table(&self) -> Self::LinksIter<'_> {
        LinkIter::new(self.link_table.iter())
    }

    #[inline]
    fn string_table(&self) -> Self::StringsIter<'_> {
        self.string_table.iter().map(string_as_bytes)
    }

    #[inline]
    fn attribute_table(&self) -> Self::AttributesIter<'_> {
        KeyValueIter::new(self.attribute_table.iter())
    }

    #[inline]
    fn attribute_units(&self) -> Self::AttributeUnitsIter<'_> {
        AttributeUnitIter::new(self.attribute_units.iter())
    }
}

impl ResourceProfilesView for ObjResourceProfiles<'_> {
    type Resource<'a>
        = ObjResource<'a>
    where
        Self: 'a;

    type ScopeProfiles<'a>
        = ObjScopeProfiles<'a>
    where
        Self: 'a;

    type ScopesIter<'a>
        = ScopeIter<'a>
    where
        Self: 'a;

    #[inline]
    fn resource(&self) -> Option<Self::Resource<'_>> {
        self.inner.resource.as_ref().map(ObjResource::new)
    }

    #[inline]
    fn scopes(&self) -> Self::ScopesIter<'_> {
        ScopeIter::new(self.inner.scope_profiles.iter())
    }

    #[inline]
    fn schema_url(&self) -> Option<Str<'_>> {
        if self.inner.schema_url.is_empty() {
            None
        } else {
            Some(self.inner.schema_url.as_bytes())
        }
    }
}

impl ScopeProfilesView for ObjScopeProfiles<'_> {
    type Scope<'a>
        = ObjInstrumentationScope<'a>
    where
        Self: 'a;

    type Profile<'a>
        = ObjProfile<'a>
    where
        Self: 'a;

    type ProfilesIter<'a>
        = ProfileIter<'a>
    where
        Self: 'a;

    #[inline]
    fn scope(&self) -> Option<Self::Scope<'_>> {
        self.inner.scope.as_ref().map(ObjInstrumentationScope::new)
    }

    #[inline]
    fn profiles(&self) -> Self::ProfilesIter<'_> {
        ProfileIter::new(self.inner.profiles.iter())
    }

    #[inline]
    fn schema_url(&self) -> Option<Str<'_>> {
        if self.inner.schema_url.is_empty() {
            None
        } else {
            Some(self.inner.schema_url.as_bytes())
        }
    }
}

impl ProfileView for ObjProfile<'_> {
    type ValueType<'a>
        = ObjValueType<'a>
    where
        Self: 'a;

    type SampleTypesIter<'a>
        = ValueTypeIter<'a>
    where
        Self: 'a;

    type Sample<'a>
        = ObjSample<'a>
    where
        Self: 'a;

    type SamplesIter<'a>
        = SampleIter<'a>
    where
        Self: 'a;

    type IndicesIter<'a>
        = IndicesIter<'a>
    where
        Self: 'a;

    #[inline]
    fn sample_types(&self) -> Self::SampleTypesIter<'_> {
        ValueTypeIter::new(self.inner.sample_type.iter())
    }

    #[inline]
    fn samples(&self) -> Self::SamplesIter<'_> {
        SampleIter::new(self.inner.sample.iter())
    }

    #[inline]
    fn location_indices(&self) -> Self::IndicesIter<'_> {
        self.inner.location_indices.iter().copied()
    }

    #[inline]
    fn time_nanos(&self) -> i64 {
        self.inner.time_nanos
    }

    #[inline]
    fn duration_nanos(&self) -> i64 {
        self.inner.duration_nanos
    }

    #[inline]
    fn period_type(&self) -> Option<Self::ValueType<'_>> {
        self.inner.period_type.as_ref().map(ObjValueType::new)
    }

    #[inline]
    fn period(&self) -> i64 {
        self.inner.period
    }

    #[inline]
    fn comment_strindices(&self) -> Self::IndicesIter<'_> {
        self.inner.comment_strindices.iter().copied()
    }

    #[inline]
    fn default_sample_type_index(&self) -> i32 {
        self.inner.default_sample_type_index
    }

    #[inline]
    fn profile_id(&self) -> &[u8] {
        &self.inner.profile_id
    }

    #[inline]
    fn dropped_attributes_count(&self) -> u32 {
        self.inner.dropped_attributes_count
    }

    #[inline]
    fn original_payload_format(&self) -> Option<Str<'_>> {
        if self.inner.original_payload_format.is_empty() {
            None
        } else {
            Some(self.inner.original_payload_format.as_bytes())
        }
    }

    #[inline]
    fn original_payload(&self) -> &[u8] {
        &self.inner.original_payload
    }

    #[inline]
    fn attribute_indices(&self) -> Self::IndicesIter<'_> {
        self.inner.attribute_indices.iter().copied()
    }
}

impl SampleView for ObjSample<'_> {
    type IndicesIter<'a>
        = IndicesIter<'a>
    where
        Self: 'a;

    type ValuesIter<'a>
        = I64sIter<'a>
    where
        Self: 'a;

    type TimestampsIter<'a>
        = U64sIter<'a>
    where
        Self: 'a;

    #[inline]
    fn locations_start_index(&self) -> i32 {
        self.inner.locations_start_index
    }

    #[inline]
    fn locations_length(&self) -> i32 {
        self.inner.locations_length
    }

    #[inline]
    fn values(&self) -> Self::ValuesIter<'_> {
        self.inner.value.iter().copied()
    }

    #[inline]
    fn attribute_indices(&self) -> Self::IndicesIter<'_> {
        self.inner.attribute_indices.iter().copied()
    }

    #[inline]
    fn link_index(&self) -> Option<i32> {
        self.inner.link_index
    }

    #[inline]
    fn timestamps_unix_nano(&self) -> Self::TimestampsIter<'_> {
        self.inner.timestamps_unix_nano.iter().copied()
    }
}

impl ValueTypeView for ObjValueType<'_> {
    #[inline]
    fn type_strindex(&self) -> i32 {
        self.inner.type_strindex
    }

    #[inline]
    fn unit_strindex(&self) -> i32 {
        self.inner.unit_strindex
    }

    #[inline]
    fn aggregation_temporality(&self) -> i32 {
        self.inner.aggregation_temporality
    }
}

impl MappingView for ObjMapping<'_> {
    type IndicesIter<'a>
        = IndicesIter<'a>
    where
        Self: 'a;

    #[inline]
    fn memory_start(&self) -> u64 {
        self.inner.memory_start
    }

    #[inline]
    fn memory_limit(&self) -> u64 {
        self.inner.memory_limit
    }

    #[inline]
    fn file_offset(&self) -> u64 {
        self.inner.file_offset
    }

    #[inline]
    fn filename_strindex(&self) -> i32 {
        self.inner.filename_strindex
    }

    #[inline]
    fn attribute_indices(&self) -> Self::IndicesIter<'_> {
        self.inner.attribute_indices.iter().copied()
    }

    #[inline]
    fn has_functions(&self) -> bool {
        self.inner.has_functions
    }

    #[inline]
    fn has_filenames(&self) -> bool {
        self.inner.has_filenames
    }

    #[inline]
    fn has_line_numbers(&self) -> bool {
        self.inner.has_line_numbers
    }

    #[inline]
    fn has_inline_frames(&self) -> bool {
        self.inner.has_inline_frames
    }
}

impl LocationView for ObjLocation<'_> {
    type Line<'a>
        = ObjLine<'a>
    where
        Self: 'a;

    type LinesIter<'a>
        = LineIter<'a>
    where
        Self: 'a;

    type IndicesIter<'a>
        = IndicesIter<'a>
    where
        Self: 'a;

    #[inline]
    fn mapping_index(&self) -> Option<i32> {
        self.inner.mapping_index
    }

    #[inline]
    fn address(&self) -> u64 {
        self.inner.address
    }

    #[inline]
    fn lines(&self) -> Self::LinesIter<'_> {
        LineIter::new(self.inner.line.iter())
    }

    #[inline]
    fn is_folded(&self) -> bool {
        self.inner.is_folded
    }

    #[inline]
    fn attribute_indices(&self) -> Self::IndicesIter<'_> {
        self.inner.attribute_indices.iter().copied()
    }
}

impl LineView for ObjLine<'_> {
    #[inline]
    fn function_index(&self) -> i32 {
        self.inner.function_index
    }

    #[inline]
    fn line(&self) -> i64 {
        self.inner.line
    }

    #[inline]
    fn column(&self) -> i64 {
        self.inner.column
    }
}

impl FunctionView for ObjFunction<'_> {
    #[inline]
    fn name_strindex(&self) -> i32 {
        self.inner.name_strindex
    }

    #[inline]
    fn system_name_strindex(&self) -> i32 {
        self.inner.system_name_strindex
    }

    #[inline]
    fn filename_strindex(&self) -> i32 {
        self.inner.filename_strindex
    }

    #[inline]
    fn start_line(&self) -> i64 {
        self.inner.start_line
    }
}

impl LinkView for ObjLink<'_> {
    #[inline]
    fn trace_id(&self) -> &[u8] {
        &self.inner.trace_id
    }

    #[inline]
    fn span_id(&self) -> &[u8] {
        &self.inner.span_id
    }
}

impl AttributeUnitView for ObjAttributeUnit<'_> {
    #[inline]
    fn attribute_key_strindex(&self) -> i32 {
        self.inner.attribute_key_strindex
    }

    #[inline]
    fn unit_strindex(&self) -> i32 {
        self.inner.unit_strindex
    }
}
