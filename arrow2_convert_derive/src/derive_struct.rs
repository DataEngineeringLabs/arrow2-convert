use proc_macro2::TokenStream;
use proc_macro_error::abort;
use quote::{quote, quote_spanned};
use syn::spanned::Spanned;

use super::input::*;

pub fn expand(input: DeriveStruct) -> TokenStream {
    let original_name = &input.common.name;
    let visibility = &input.common.visibility;
    let fields = &input.fields;

    let (gen_serialize, gen_deserialize) = input.common.traits_to_derive.to_flags();

    let field_names = fields
        .iter()
        .map(|field| field.syn.ident.as_ref().unwrap())
        .collect::<Vec<_>>();

    if field_names.is_empty() {
        abort!(
            original_name.span(),
            "Expected struct to have more than one field"
        );
    }

    let first_field = field_names[0];

    let field_names_str = field_names
        .iter()
        .map(|field| syn::LitStr::new(&format!("{}", field), proc_macro2::Span::call_site()))
        .collect::<Vec<_>>();

    let field_indices = field_names
        .iter()
        .enumerate()
        .map(|(idx, _ident)| syn::LitInt::new(&format!("{}", idx), proc_macro2::Span::call_site()))
        .collect::<Vec<_>>();

    let field_types: Vec<&syn::TypePath> = fields
        .iter()
        .map(|field| match &field.field_type {
            syn::Type::Path(path) => path,
            _ => panic!("Only types are supported atm"),
        })
        .collect::<Vec<&syn::TypePath>>();

    let mut generated = quote!(
        impl arrow2_convert::field::ArrowField for #original_name {
            type Type = Self;

            fn data_type() -> arrow2::datatypes::DataType {
                arrow2::datatypes::DataType::Struct(
                    vec![
                        #(
                            <#field_types as arrow2_convert::field::ArrowField>::field(#field_names_str),
                        )*
                    ]
                )
            }
        }

        arrow2_convert::arrow_enable_vec_for_type!(#original_name);
    );

    if gen_serialize {
        let mutable_array_name = &input.common.mutable_array_name();
        let mutable_field_array_types = field_types
        .iter()
        .map(|field_type| quote_spanned!( field_type.span() => <#field_type as arrow2_convert::serialize::ArrowSerialize>::MutableArrayType))
        .collect::<Vec<TokenStream>>();

        let array_decl = quote! {
            #[derive(Debug)]
            #visibility struct #mutable_array_name {
                #(
                    #field_names: #mutable_field_array_types,
                )*
                data_type: arrow2::datatypes::DataType,
                validity: Option<arrow2::bitmap::MutableBitmap>,
            }
        };

        let array_impl = quote! {
            impl #mutable_array_name {
                pub fn new() -> Self {
                    Self {
                        #(#field_names: <#field_types as arrow2_convert::serialize::ArrowSerialize>::new_array(),)*
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

        let array_arrow_mutable_array_impl = quote! {
            impl arrow2_convert::serialize::ArrowMutableArray for #mutable_array_name {
                fn reserve(&mut self, additional: usize, _additional_values: usize) {
                    if let Some(x) = self.validity.as_mut() {
                        x.reserve(additional)
                    }
                    #(<<#field_types as arrow2_convert::serialize::ArrowSerialize>::MutableArrayType as arrow2_convert::serialize::ArrowMutableArray>::reserve(&mut self.#field_names, additional, _additional_values);)*
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
                                <#field_types as arrow2_convert::serialize::ArrowSerialize>::arrow_serialize(i.#field_names.borrow(), &mut self.#field_names)?;
                            )*;
                            match &mut self.validity {
                                Some(validity) => validity.push(true),
                                None => {}
                            }
                        },
                        None => {
                            #(
                                <#mutable_field_array_types as MutableArray>::push_null(&mut self.#field_names);
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

        let array_mutable_array_impl = quote! {
            impl arrow2::array::MutableArray for #mutable_array_name {
                fn data_type(&self) -> &arrow2::datatypes::DataType {
                    &self.data_type
                }

                fn len(&self) -> usize {
                    self.#first_field.len()
                }

                fn validity(&self) -> Option<&arrow2::bitmap::MutableBitmap> {
                    self.validity.as_ref()
                }

                fn as_box(&mut self) -> Box<dyn arrow2::array::Array> {
                    let values = vec![#(
                        <#mutable_field_array_types as arrow2::array::MutableArray>::as_arc(&mut self.#field_names),
                    )*];

                    Box::new(arrow2::array::StructArray::from_data(
                        <#original_name as arrow2_convert::field::ArrowField>::data_type().clone(),
                        values,
                        std::mem::take(&mut self.validity).map(|x| x.into()),
                    ))
                }

                fn as_arc(&mut self) -> std::sync::Arc<dyn arrow2::array::Array> {
                    let values = vec![#(
                        <#mutable_field_array_types as arrow2::array::MutableArray>::as_arc(&mut self.#field_names),
                    )*];

                    std::sync::Arc::new(arrow2::array::StructArray::from_data(
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
                        <#mutable_field_array_types as arrow2::array::MutableArray>::shrink_to_fit(&mut self.#field_names);
                    )*
                    if let Some(validity) = &mut self.validity {
                        validity.shrink_to_fit();
                    }
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

        generated.extend([
            array_decl,
            array_impl,
            array_default_impl,
            array_arrow_mutable_array_impl,
            array_try_push_impl,
            array_try_extend_impl,
            array_mutable_array_impl,
            field_arrow_serialize_impl,
        ])
    }

    if gen_deserialize {
        let array_name = &input.common.array_name();
        let iterator_name = &input.common.iterator_name();

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
                            #field_names: <<#field_types as arrow2_convert::deserialize::ArrowDeserialize>::ArrayType as arrow2_convert::deserialize::ArrowArray>::iter_from_array_ref(values[#field_indices].deref()),
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
                    #field_names: <&'a <#field_types as arrow2_convert::deserialize::ArrowDeserialize>::ArrayType as IntoIterator>::IntoIter,
                )*
                validity_iter: arrow2::bitmap::utils::BitmapIter<'a>,
                has_validity: bool
            }
        };

        let iterator_impl = quote! {
            impl<'a> #iterator_name<'a> {
                #[inline]
                fn return_next(&mut self) -> Option<#original_name> {
                    if let (#(
                        Some(#field_names),
                    )*) = (
                        #(self.#field_names.next(),)*
                    )
                    {
                        Some(#original_name {
                            #(#field_names: <#field_types as arrow2_convert::deserialize::ArrowDeserialize>::arrow_deserialize_internal(#field_names),)*
                        })
                    }
                    else {
                        None
                    }
                }

                #[inline]
                fn consume_next(&mut self) {
                    #(let _ = self.#field_names.next();)*
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

        let field_arrow_deserialize_impl = quote! {
            impl arrow2_convert::deserialize::ArrowDeserialize for #original_name {
                type ArrayType = #array_name;

                #[inline]
                fn arrow_deserialize<'a>(v: Option<Self>) -> Option<Self> {
                    v
                }
            }
        };

        generated.extend([
            array_decl,
            array_impl,
            array_into_iterator_impl,
            iterator_decl,
            iterator_impl,
            iterator_iterator_impl,
            field_arrow_deserialize_impl,
        ])
    }

    generated
}
