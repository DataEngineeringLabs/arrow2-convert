use proc_macro2::TokenStream;
use proc_macro_error::abort;
use quote::{format_ident, quote, quote_spanned};
use syn::spanned::Spanned;

use super::input::*;

struct Common<'a> {
    original_name: &'a proc_macro2::Ident,
    visibility: &'a syn::Visibility,
    field_members: Vec<syn::Member>,
    field_idents: Vec<syn::Ident>,
    skipped_field_names: Vec<syn::Member>,
    field_indices: Vec<syn::LitInt>,
    field_types: Vec<&'a syn::TypePath>,
}

impl<'a> From<&'a DeriveStruct> for Common<'a> {
    fn from(input: &'a DeriveStruct) -> Self {
        let original_name = &input.common.name;
        let visibility = &input.common.visibility;

        let (skipped_fields, fields): (Vec<_>, Vec<_>) =
            input.fields.iter().partition(|field| field.skip);

        let field_members = fields
            .iter()
            .enumerate()
            .map(|(id, field)| {
                field
                    .syn
                    .ident
                    .as_ref()
                    .cloned()
                    .map_or_else(|| syn::Member::Unnamed(id.into()), syn::Member::Named)
            })
            .collect::<Vec<_>>();

        let field_idents = field_members
            .iter()
            .map(|f| match f {
                // `Member` doesn't impl `IdentFragment` in a way that preserves the "r#" prefix stripping of `Ident`, so we go one level inside.
                syn::Member::Named(ident) => format_ident!("field_{}", ident),
                syn::Member::Unnamed(index) => format_ident!("field_{}", index),
            })
            .collect::<Vec<_>>();

        let skipped_field_names = skipped_fields
            .iter()
            .enumerate()
            .map(|(id, field)| {
                field
                    .syn
                    .ident
                    .as_ref()
                    .cloned()
                    .map_or_else(|| syn::Member::Unnamed(id.into()), syn::Member::Named)
            })
            .collect::<Vec<_>>();

        if field_members.is_empty() {
            abort!(
                original_name.span(),
                "Expected struct to have more than one field"
            );
        }

        let field_indices = field_members
            .iter()
            .enumerate()
            .map(|(idx, _ident)| {
                syn::LitInt::new(&format!("{}", idx), proc_macro2::Span::call_site())
            })
            .collect::<Vec<_>>();

        let field_types: Vec<&syn::TypePath> = fields
            .iter()
            .map(|field| match &field.field_type {
                syn::Type::Path(path) => path,
                _ => panic!("Only types are supported atm"),
            })
            .collect::<Vec<&syn::TypePath>>();

        Self {
            original_name,
            visibility,
            field_members,
            field_idents,
            skipped_field_names,
            field_indices,
            field_types,
        }
    }
}

pub fn expand_field(input: DeriveStruct) -> TokenStream {
    let Common {
        original_name,
        field_members,
        //field_names_str,
        field_types,
        ..
    } = (&input).into();

    let data_type_impl = {
        if input.fields.len() == 1 && input.is_transparent {
            // Special case for single-field (tuple) structs
            let field = &input.fields[0];
            let ty = &field.field_type;
            quote! (
                <#ty as arrow2_convert::field::ArrowField>::data_type()
            )
        } else {
            let field_names = field_members.iter().map(|field| match field {
                syn::Member::Named(ident) => format_ident!("{}", ident),
                syn::Member::Unnamed(index) => format_ident!("field_{}", index),
            });
            quote!(arrow2::datatypes::DataType::Struct(vec![
                #(
                    <#field_types as arrow2_convert::field::ArrowField>::field(stringify!(#field_names)),
                )*
            ]))
        }
    };

    quote!(
        impl arrow2_convert::field::ArrowField for #original_name {
            type Type = Self;

            fn data_type() -> arrow2::datatypes::DataType {
                #data_type_impl
            }
        }

        arrow2_convert::arrow_enable_vec_for_type!(#original_name);
    )
}

pub fn expand_serialize(input: DeriveStruct) -> TokenStream {
    let Common {
        original_name,
        visibility,
        field_members: field_names,
        field_idents,
        field_types,
        ..
    } = (&input).into();

    let first_field = &field_names[0];

    let mutable_array_name = &input.common.mutable_array_name();
    let mutable_field_array_types = field_types
        .iter()
        .map(|field_type| quote_spanned!( field_type.span() => <#field_type as arrow2_convert::serialize::ArrowSerialize>::MutableArrayType))
        .collect::<Vec<TokenStream>>();

    let array_decl = quote! {
        #[derive(Debug)]
        #visibility struct #mutable_array_name {
            #(
                #field_idents: #mutable_field_array_types,
            )*
            data_type: arrow2::datatypes::DataType,
            validity: Option<arrow2::bitmap::MutableBitmap>,
        }
    };

    let array_impl = quote! {
        impl #mutable_array_name {
            pub fn new() -> Self {
                Self {
                    #(#field_idents: <#field_types as arrow2_convert::serialize::ArrowSerialize>::new_array(),)*
                    data_type: <#original_name as arrow2_convert::field::ArrowField>::data_type(),
                    validity: None,
                }
            }

            fn init_validity(&mut self) {
                let mut validity = arrow2::bitmap::MutableBitmap::new();
                validity.extend_constant(<Self as arrow2::array::MutableArray>::len(self), true);
                validity.set(<Self as arrow2::array::MutableArray>::len(self) - 1, false);
                self.validity = Some(validity)
            }
        }
    };

    let array_default_impl = quote! {
        impl Default for #mutable_array_name {
            fn default() -> Self {
                Self::new()
            }
        }
    };

    let array_try_push_impl = quote! {
        impl<__T: std::borrow::Borrow<#original_name>> arrow2::array::TryPush<Option<__T>> for #mutable_array_name {
            fn try_push(&mut self, item: Option<__T>) -> arrow2::error::Result<()> {
                use arrow2::array::MutableArray;
                use std::borrow::Borrow;

                match item {
                    Some(i) =>  {
                        let i = i.borrow();
                        #(
                            <#field_types as arrow2_convert::serialize::ArrowSerialize>::arrow_serialize(i.#field_names.borrow(), &mut self.#field_idents)?;
                        )*;
                        match &mut self.validity {
                            Some(validity) => validity.push(true),
                            None => {}
                        }
                    },
                    None => {
                        #(
                            <#mutable_field_array_types as MutableArray>::push_null(&mut self.#field_idents);
                        )*;
                        match &mut self.validity {
                            Some(validity) => validity.push(false),
                            None => {
                                self.init_validity();
                            }
                        }
                    }
                }
                Ok(())
            }
        }
    };

    let array_try_extend_impl = quote! {
        impl<__T: std::borrow::Borrow<#original_name>> arrow2::array::TryExtend<Option<__T>> for #mutable_array_name {
            fn try_extend<I: IntoIterator<Item = Option<__T>>>(&mut self, iter: I) -> arrow2::error::Result<()> {
                use arrow2::array::TryPush;
                for i in iter {
                    self.try_push(i)?;
                }
                Ok(())
            }
        }
    };

    let first_ident = &field_idents[0];

    let array_mutable_array_impl = quote! {
        impl arrow2::array::MutableArray for #mutable_array_name {
            fn data_type(&self) -> &arrow2::datatypes::DataType {
                &self.data_type
            }

            fn len(&self) -> usize {
                self.#first_ident.len()
            }

            fn validity(&self) -> Option<&arrow2::bitmap::MutableBitmap> {
                self.validity.as_ref()
            }

            fn as_box(&mut self) -> Box<dyn arrow2::array::Array> {
                let values = vec![#(
                    <#mutable_field_array_types as arrow2::array::MutableArray>::as_box(&mut self.#field_idents),
                )*];

                    Box::new(arrow2::array::StructArray::new(
                    <#original_name as arrow2_convert::field::ArrowField>::data_type().clone(),
                    values,
                    std::mem::take(&mut self.validity).map(|x| x.into()),
                ))
            }

            fn as_arc(&mut self) -> std::sync::Arc<dyn arrow2::array::Array> {
                let values = vec![#(
                    <#mutable_field_array_types as arrow2::array::MutableArray>::as_box(&mut self.#field_idents),
                )*];

                    std::sync::Arc::new(arrow2::array::StructArray::new(
                    <#original_name as arrow2_convert::field::ArrowField>::data_type().clone(),
                    values,
                    std::mem::take(&mut self.validity).map(|x| x.into())
                ))
            }

            fn as_any(&self) -> &dyn std::any::Any {
                self
            }

            fn as_mut_any(&mut self) -> &mut dyn std::any::Any {
                self
            }

            fn push_null(&mut self) {
                use arrow2::array::TryPush;
                self.try_push(None::<#original_name>).unwrap();
            }

            fn shrink_to_fit(&mut self) {
                #(
                    <#mutable_field_array_types as arrow2::array::MutableArray>::shrink_to_fit(&mut self.#field_idents);
                )*
                if let Some(validity) = &mut self.validity {
                    validity.shrink_to_fit();
                }
            }

            fn reserve(&mut self, additional: usize) {
                if let Some(x) = self.validity.as_mut() {
                    x.reserve(additional)
                }
                #(<<#field_types as arrow2_convert::serialize::ArrowSerialize>::MutableArrayType as arrow2::array::MutableArray>::reserve(&mut self.#field_idents, additional);)*
            }
        }
    };

    // Special case for single-field (tuple) structs.
    if input.fields.len() == 1 && input.is_transparent {
        let first_type = &field_types[0];
        // Everything delegates to first field.
        quote! {
            impl arrow2_convert::serialize::ArrowSerialize for #original_name {
                type MutableArrayType = <#first_type as arrow2_convert::serialize::ArrowSerialize>::MutableArrayType;

                #[inline]
                fn new_array() -> Self::MutableArrayType {
                    <#first_type as arrow2_convert::serialize::ArrowSerialize>::new_array()
                }

                #[inline]
                fn arrow_serialize(v: &Self, array: &mut Self::MutableArrayType) -> arrow2::error::Result<()> {
                    <#first_type as arrow2_convert::serialize::ArrowSerialize>::arrow_serialize(&v.#first_field, array)
                }
            }
        }
    } else {
        let field_arrow_serialize_impl = quote! {
            impl arrow2_convert::serialize::ArrowSerialize for #original_name {
                type MutableArrayType = #mutable_array_name;

                #[inline]
                fn new_array() -> Self::MutableArrayType {
                    Self::MutableArrayType::default()
                }

                #[inline]
                fn arrow_serialize(v: &Self, array: &mut Self::MutableArrayType) -> arrow2::error::Result<()> {
                    use arrow2::array::TryPush;
                    array.try_push(Some(v))
                }
            }
        };
        TokenStream::from_iter([
            array_decl,
            array_impl,
            array_default_impl,
            array_try_push_impl,
            array_try_extend_impl,
            array_mutable_array_impl,
            field_arrow_serialize_impl,
        ])
    }
}

pub fn expand_deserialize(input: DeriveStruct) -> TokenStream {
    let Common {
        original_name,
        visibility,
        field_members: field_names,
        field_idents,
        skipped_field_names,
        field_indices,
        field_types,
        ..
    } = (&input).into();

    let array_name = &input.common.array_name();
    let iterator_name = &input.common.iterator_name();
    let is_tuple_struct = matches!(field_names[0], syn::Member::Unnamed(_));

    let array_decl = quote! {
        #visibility struct #array_name
        {}
    };

    let array_impl = quote! {
        impl arrow2_convert::deserialize::ArrowArray for #array_name
        {
            type BaseArrayType = arrow2::array::StructArray;

            #[inline]
            fn iter_from_array_ref<'a>(b: &'a dyn arrow2::array::Array)  -> <&'a Self as IntoIterator>::IntoIter
            {
                use core::ops::Deref;
                let arr = b.as_any().downcast_ref::<arrow2::array::StructArray>().unwrap();
                let values = arr.values();
                let validity = arr.validity();
                // for now do a straight comp
                #iterator_name {
                    #(
                        #field_idents: <<#field_types as arrow2_convert::deserialize::ArrowDeserialize>::ArrayType as arrow2_convert::deserialize::ArrowArray>::iter_from_array_ref(values[#field_indices].deref()),
                    )*
                    has_validity: validity.as_ref().is_some(),
                    validity_iter: validity.as_ref().map(|x| x.iter()).unwrap_or_else(|| arrow2::bitmap::utils::BitmapIter::new(&[], 0, 0))
                }
            }
        }
    };

    let array_into_iterator_impl = quote! {
        impl<'a> IntoIterator for &'a #array_name
        {
            type Item = Option<#original_name>;
            type IntoIter = #iterator_name<'a>;

            fn into_iter(self) -> Self::IntoIter {
                unimplemented!("Use iter_from_array_ref");
            }
        }
    };

    let iterator_decl = quote! {
        #visibility struct #iterator_name<'a> {
            #(
                #field_idents: <&'a <#field_types as arrow2_convert::deserialize::ArrowDeserialize>::ArrayType as IntoIterator>::IntoIter,
            )*
            validity_iter: arrow2::bitmap::utils::BitmapIter<'a>,
            has_validity: bool
        }
    };

    let struct_inst: syn::Pat = if is_tuple_struct {
        // If the fields are unnamed, we create a tuple-struct
        syn::parse_quote! {
            #original_name (
                #(<#field_types as arrow2_convert::deserialize::ArrowDeserialize>::arrow_deserialize_internal(#field_idents),)*
            )
        }
    } else {
        syn::parse_quote! {
            #original_name {
                #(#field_names: <#field_types as arrow2_convert::deserialize::ArrowDeserialize>::arrow_deserialize_internal(#field_idents),)*
                #(#skipped_field_names: std::default::Default::default(),)*
            }
        }
    };

    let iterator_impl = quote! {
        impl<'a> #iterator_name<'a> {
            #[inline]
            fn return_next(&mut self) -> Option<#original_name> {
                if let (#(
                    Some(#field_idents),
                )*) = (
                    #(self.#field_idents.next(),)*
                )
                { Some(#struct_inst) }
                else { None }
            }

            #[inline]
            fn consume_next(&mut self) {
                #(let _ = self.#field_idents.next();)*
            }
        }
    };

    let iterator_iterator_impl = quote! {
        impl<'a> Iterator for #iterator_name<'a> {
            type Item = Option<#original_name>;

            #[inline]
            fn next(&mut self) -> Option<Self::Item> {
                if !self.has_validity {
                    self.return_next().map(|y| Some(y))
                }
                else {
                    let is_valid = self.validity_iter.next();
                    is_valid.map(|x| if x { self.return_next() } else { self.consume_next(); None })
                }
            }
        }
    };

    // Special case for single-field (tuple) structs.
    if input.fields.len() == 1 && input.is_transparent {
        let first_type = &field_types[0];

        let deser_body_mapper = if is_tuple_struct {
            quote! { #original_name }
        } else {
            let first_name = &field_names[0];
            quote! { |v| #original_name { #first_name: v } }
        };

        // Everything delegates to first field.
        quote! {
            impl arrow2_convert::deserialize::ArrowDeserialize for #original_name {
                type ArrayType = <#first_type as arrow2_convert::deserialize::ArrowDeserialize>::ArrayType;

                #[inline]
                fn arrow_deserialize<'a>(v: <&Self::ArrayType as IntoIterator>::Item) -> Option<Self> {
                    <#first_type as arrow2_convert::deserialize::ArrowDeserialize>::arrow_deserialize(v).map(#deser_body_mapper)
                }
            }
        }
    } else {
        let field_arrow_deserialize_impl = quote! {
            impl arrow2_convert::deserialize::ArrowDeserialize for #original_name {
                type ArrayType = #array_name;

                #[inline]
                fn arrow_deserialize<'a>(v: Option<Self>) -> Option<Self> {
                    v
                }
            }
        };

        TokenStream::from_iter([
            array_decl,
            array_impl,
            array_into_iterator_impl,
            iterator_decl,
            iterator_impl,
            iterator_iterator_impl,
            field_arrow_deserialize_impl,
        ])
    }
}
