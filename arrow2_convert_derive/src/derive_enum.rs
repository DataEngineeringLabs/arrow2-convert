use proc_macro2::TokenStream;
use proc_macro_error::abort;
use quote::{quote, quote_spanned};
use syn::spanned::Spanned;

use crate::input::{DeriveEnum, DeriveVariant};

struct Common<'a> {
    original_name: &'a proc_macro2::Ident,
    original_name_str: String,
    visibility: &'a syn::Visibility,
    variants: &'a Vec<DeriveVariant>,
    union_type: TokenStream,
    variant_names: Vec<proc_macro2::Ident>,
    variant_names_str: Vec<syn::LitStr>,
    variant_indices: Vec<syn::LitInt>,
    variant_types: Vec<&'a syn::TypePath>,
}

impl<'a> From<&'a DeriveEnum> for Common<'a> {
    fn from(input: &'a DeriveEnum) -> Self {
        let original_name = &input.common.name;
        let original_name_str = format!("{original_name}");
        let visibility = &input.common.visibility;
        let is_dense = input.is_dense;
        let variants = &input.variants;

        let union_type = if is_dense {
            quote!(arrow2::datatypes::UnionMode::Dense)
        } else {
            quote!(arrow2::datatypes::UnionMode::Sparse)
        };

        let variant_names = variants
            .iter()
            .map(|v| v.syn.ident.clone())
            .collect::<Vec<_>>();

        if variant_names.is_empty() {
            abort!(
                original_name.span(),
                "Expected enum to have more than one field"
            );
        }

        let variant_names_str = variant_names
            .iter()
            .map(|v| syn::LitStr::new(&format!("{v}"), proc_macro2::Span::call_site()))
            .collect::<Vec<_>>();

        let variant_indices = variant_names
            .iter()
            .enumerate()
            .map(|(idx, _ident)| {
                syn::LitInt::new(&format!("{idx}"), proc_macro2::Span::call_site())
            })
            .collect::<Vec<_>>();

        let variant_types: Vec<&syn::TypePath> = variants
            .iter()
            .map(|v| match &v.field_type {
                syn::Type::Path(path) => path,
                _ => panic!("Only types are supported atm"),
            })
            .collect::<Vec<&syn::TypePath>>();

        Self {
            original_name,
            original_name_str,
            visibility,
            variants,
            union_type,
            variant_names,
            variant_names_str,
            variant_indices,
            variant_types,
        }
    }
}

pub fn expand_field(input: DeriveEnum) -> TokenStream {
    let Common {
        original_name,
        union_type,
        variant_names_str,
        variant_types,
        ..
    } = (&input).into();

    quote! {
        impl arrow2_convert::field::ArrowField for #original_name {
            type Type = Self;

            fn data_type() -> arrow2::datatypes::DataType {
                arrow2::datatypes::DataType::Union(
                    vec![
                        #(
                            <#variant_types as arrow2_convert::field::ArrowField>::field(#variant_names_str),
                        )*
                    ],
                    None,
                    #union_type,
                )
            }
        }

        arrow2_convert::arrow_enable_vec_for_type!(#original_name);
    }
}

pub fn expand_serialize(input: DeriveEnum) -> TokenStream {
    let Common {
        original_name,
        visibility,
        variants,
        variant_names,
        variant_indices,
        variant_types,
        ..
    } = (&input).into();

    let is_dense = input.is_dense;

    let mutable_array_name = &input.common.mutable_array_name();
    let mutable_variant_array_types = variant_types
        .iter()
        .map(|field_type| quote_spanned!( field_type.span() => <#field_type as arrow2_convert::serialize::ArrowSerialize>::MutableArrayType))
        .collect::<Vec<TokenStream>>();

    let (offsets_decl, offsets_init, offsets_reserve, offsets_take, offsets_shrink_to_fit) =
        if is_dense {
            (
                quote! { offsets: Vec<i32>, },
                quote! { offsets: vec![], },
                quote! { self.offsets.reserve(additional); },
                quote! { Some(std::mem::take(&mut self.offsets).into()), },
                quote! { self.offsets.shrink_to_fit(); },
            )
        } else {
            (quote! {}, quote! {}, quote! {}, quote! {None}, quote! {})
        };

    let try_push_match_blocks = variants
            .iter()
            .enumerate()
            .zip(&variant_indices)
            .zip(&variant_types)
            .map(|(((idx, v), lit_idx), variant_type)| {
                let name = &v.syn.ident;
                // - For dense unions, update the mutable array of the matched variant and also the offset.
                // - For sparse unions, update the mutable array of the matched variant, and push null for all
                //   the other variants. This unfortunately results in some large code blocks per match arm.
                //   There might be a better way of doing this.
                if is_dense {
                    let update_offset = quote! {
                        self.types.push(#lit_idx);
                        self.offsets.push((self.#name.len() - 1) as i32);
                    };
                    if v.is_unit {
                        quote! {
                            #original_name::#name => {
                                <#variant_type as arrow2_convert::serialize::ArrowSerialize>::arrow_serialize(&true, &mut self.#name)?;
                                #update_offset
                            }
                        }
                    }
                    else {
                        quote! {
                            #original_name::#name(v) => {
                                <#variant_type as arrow2_convert::serialize::ArrowSerialize>::arrow_serialize(v, &mut self.#name)?;
                                #update_offset
                            }
                        }
                    }
                }
                else {
                    let push_none = variants
                        .iter()
                        .enumerate()
                        .zip(&variant_types)
                        .map(|((nested_idx,y), variant_type)| {
                            let name = &y.syn.ident;
                            if nested_idx != idx {
                                quote! {
                                    <<#variant_type as arrow2_convert::serialize::ArrowSerialize>::MutableArrayType as MutableArray>::push_null(&mut self.#name);
                                }
                            }
                            else {
                                quote!{}
                            }
                        })
                        .collect::<Vec<TokenStream>>();

                    let update_offset = quote! {
                        self.types.push(#lit_idx);
                    };

                    if v.is_unit {
                        quote! {
                            #original_name::#name => {
                                <#variant_type as arrow2_convert::serialize::ArrowSerialize>::arrow_serialize(&true, &mut self.#name)?;
                                #(
                                    #push_none
                                )*
                                #update_offset
                            }
                        }
                    }
                    else {
                        quote! {
                            #original_name::#name(v) => {
                                <#variant_type as arrow2_convert::serialize::ArrowSerialize>::arrow_serialize(v, &mut self.#name)?;
                                #(
                                    #push_none
                                )*
                                #update_offset
                            }
                        }
                    }
                }
            })
            .collect::<Vec<TokenStream>>();

    let try_push_none = if is_dense {
        let first_array_type = &mutable_variant_array_types[0];
        let first_name = &variant_names[0];
        quote! {
            self.types.push(0);
            self.offsets.push((self.#first_name.len()) as i32);
            <#first_array_type as MutableArray>::push_null(&mut self.#first_name);
        }
    } else {
        quote! {
            self.types.push(0);
            #(
                <#mutable_variant_array_types as MutableArray>::push_null(&mut self.#variant_names);
            )*
        }
    };

    let array_decl = quote! {
        #[allow(non_snake_case)]
        #[derive(Debug)]
        #visibility struct #mutable_array_name {
            #(
                #variant_names: #mutable_variant_array_types,
            )*
            data_type: arrow2::datatypes::DataType,
            types: Vec<i8>,
            #offsets_decl
        }
    };

    let array_impl = quote! {
        impl #mutable_array_name {
            pub fn new() -> Self {
                Self {
                    #(#variant_names: <#variant_types as arrow2_convert::serialize::ArrowSerialize>::new_array(),)*
                    data_type: <#original_name as arrow2_convert::field::ArrowField>::data_type(),
                    types: vec![],
                    #offsets_init
                }
            }
        }
    };

    let array_try_push_impl = quote! {
        impl<__T: std::borrow::Borrow<#original_name>> arrow2::array::TryPush<Option<__T>> for #mutable_array_name {
            fn try_push(&mut self, item: Option<__T>) -> arrow2::error::Result<()> {
                use arrow2::array::MutableArray;

                match item {
                    Some(i) => {
                        match i.borrow() {
                            #(
                                #try_push_match_blocks
                            )*
                        }
                    },
                    None => {
                        #try_push_none
                    }
                }
                Ok(())
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

    let array_mutable_array_impl = quote! {
        impl arrow2::array::MutableArray for #mutable_array_name {
            fn data_type(&self) -> &arrow2::datatypes::DataType {
                &self.data_type
            }

            fn len(&self) -> usize {
                self.types.len()
            }

            fn validity(&self) -> Option<&arrow2::bitmap::MutableBitmap> {
                None
            }

            fn as_box(&mut self) -> Box<dyn arrow2::array::Array> {
                let values = vec![#(
                    <#mutable_variant_array_types as arrow2::array::MutableArray>::as_box(&mut self.#variant_names),
                )*];

                    Box::new(arrow2::array::UnionArray::new(
                    <#original_name as arrow2_convert::field::ArrowField>::data_type().clone(),
                    std::mem::take(&mut self.types).into(),
                    values,
                    #offsets_take
                ))
            }

            fn as_arc(&mut self) -> std::sync::Arc<dyn arrow2::array::Array> {
                let values = vec![#(
                    <#mutable_variant_array_types as arrow2::array::MutableArray>::as_box(&mut self.#variant_names),
                )*];

                    std::sync::Arc::new(arrow2::array::UnionArray::new(
                    <#original_name as arrow2_convert::field::ArrowField>::data_type().clone(),
                    std::mem::take(&mut self.types).into(),
                    values,
                    #offsets_take
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
                    <#mutable_variant_array_types as arrow2::array::MutableArray>::shrink_to_fit(&mut self.#variant_names);
                )*
                self.types.shrink_to_fit();
                #offsets_shrink_to_fit
            }

            fn reserve(&mut self, additional: usize) {
                #(<<#variant_types as arrow2_convert::serialize::ArrowSerialize>::MutableArrayType as arrow2::array::MutableArray>::reserve(&mut self.#variant_names, additional);)*
                self.types.reserve(additional);
                #offsets_reserve
            }
        }
    };

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
        array_try_push_impl,
        array_default_impl,
        array_try_extend_impl,
        array_mutable_array_impl,
        field_arrow_serialize_impl,
    ])
}

pub fn expand_deserialize(input: DeriveEnum) -> TokenStream {
    let Common {
        original_name,
        original_name_str,
        visibility,
        variants,
        variant_indices,
        variant_types,
        ..
    } = (&input).into();

    let array_name = &input.common.array_name();
    let iterator_name = &input.common.iterator_name();

    // For unit variants, return the variant directly. For non-unit variants, get the slice of the underlying field array
    // and deserialize to the variant type.
    let iter_next_match_block = {
        let candidates = variants.iter()
                    .zip(&variant_indices)
                    .zip(&variant_types)
                    .map(|((v, lit_idx), variant_type)| {
                        let name = &v.syn.ident;
                        if v.is_unit {
                            quote! {
                                #lit_idx => {
                                    Some(Some(#original_name::#name))
                                }
                            }
                        }
                        else {
                            quote! {
                                #lit_idx => {
                                    let mut slice_iter = <<#variant_type as arrow2_convert::deserialize::ArrowDeserialize> ::ArrayType as arrow2_convert::deserialize::ArrowArray> ::iter_from_array_ref(slice.deref());
                                    let v = slice_iter
                                        .next()
                                        .unwrap_or_else(|| panic!("Invalid offset for {}", #lit_idx));
                                    Some(<#variant_type as arrow2_convert::deserialize::ArrowDeserialize>::arrow_deserialize(v).map(|v| #original_name::#name(v)))
                                }
                            }
                        }
                    })
                    .collect::<Vec<TokenStream>>();
        quote! { #(#candidates)* }
    };

    let array_decl = quote! {
        #visibility struct #array_name
        {}
    };

    let array_impl = quote! {
        impl arrow2_convert::deserialize::ArrowArray for #array_name
        {
            type BaseArrayType = arrow2::array::UnionArray;

            #[inline]
            fn iter_from_array_ref<'a>(b: &'a dyn arrow2::array::Array)  -> <&'a Self as IntoIterator>::IntoIter
            {
                let arr = b.as_any().downcast_ref::<arrow2::array::UnionArray>().unwrap();

                #iterator_name {
                    arr,
                    index_iter: 0..arr.len(),
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

    let array_iterator_decl = quote! {
        #[allow(non_snake_case)]
        #visibility struct #iterator_name<'a> {
            arr: &'a arrow2::array::UnionArray,
            index_iter: std::ops::Range<usize>,
        }
    };

    let array_iterator_iterator_impl = quote! {
        impl<'a> Iterator for #iterator_name<'a> {
            type Item = Option<#original_name>;

            #[inline]
            fn next(&mut self) -> Option<Self::Item> {
                use core::ops::Deref;
                let Some(next_index) = self.index_iter.next() else {
                    return None;
                };
                let (type_idx, offset) = self.arr.index(next_index);
                let slice = self.arr.fields()[type_idx].slice(offset, 1);
                match type_idx {
                    #iter_next_match_block
                    _ => panic!("Invalid type for {}", #original_name_str)
                }
            }
        }
    };

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
        array_iterator_decl,
        array_iterator_iterator_impl,
        field_arrow_deserialize_impl,
    ])
}
