use proc_macro2::TokenStream;
use quote::{quote, quote_spanned};
use syn::spanned::Spanned;
use proc_macro_error::abort;

use super::input::*;

// Helper method for identifying the traits to derive
fn traits_to_derive(t: &TraitsToDerive) -> (bool, bool) {
    let mut gen_serialize = true;
    let mut gen_deserialize = true;

    // setup the flags 
    match t {
        TraitsToDerive::All => { /* do nothing */ },
        TraitsToDerive::DeserializeOnly => {
            gen_serialize = false;
        },
        TraitsToDerive::SerializeOnly => {
            gen_deserialize = false;
        }
        TraitsToDerive::FieldOnly => {
            gen_deserialize = false;
            gen_serialize = false;
        }
    }

    (gen_serialize, gen_deserialize)
}

pub fn expand_derive(input: &Input) -> TokenStream {
    let original_name = &input.name;
    let original_name_str = format!("{}", original_name);
    let vec_name_str = format!("Vec<{}>", original_name);
    let visibility = &input.visibility;

    let mutable_array_name = &input.mutable_array_name();
    let array_name = &input.array_name();
    let iterator_name = &input.iterator_name();

    let (gen_serialize, gen_deserialize) = traits_to_derive(&input.traits_to_derive);

    let field_names = input
        .fields
        .iter()
        .map(|field| field.syn.ident.as_ref().unwrap())
        .collect::<Vec<_>>();

    if field_names.is_empty() {
        abort!(original_name.span(), "Expected struct to have more than one field");
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

    let field_docs = field_names
        .iter()
        .map(|field| {
            format!(
                "A vector of `{0}` from a [`{1}`](struct.{1}.html)",
                field, mutable_array_name
            )
        })
        .collect::<Vec<_>>();

    let field_types: Vec<&syn::TypePath> = input
        .fields
        .iter()
        .map(|field| match &field.field_type {
            syn::Type::Path(path) => {
                path
            },
            _ => panic!("Only types are supported atm"),
        })
        .collect::<Vec<&syn::TypePath>>();

    let mutable_field_array_types = field_types
        .iter()
        .map(|field_type| quote_spanned!( field_type.span() => <#field_type as arrow2_convert::serialize::ArrowSerialize>::MutableArrayType))
        .collect::<Vec<TokenStream>>();

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
        generated.extend(quote! {
            /// A mutable [`arrow2::StructArray`] for elements of 
            #[doc = #original_name_str]
            /// which is logically equivalent to a 
            #[doc = #vec_name_str]
            #[derive(Debug)]
            #visibility struct #mutable_array_name {
                #(
                    #[doc = #field_docs]
                    #field_names: #mutable_field_array_types,
                )*
                data_type: arrow2::datatypes::DataType,
                validity: Option<arrow2::bitmap::MutableBitmap>,
            }

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

            impl Default for #mutable_array_name {
                fn default() -> Self {
                    Self::new()
                }
            }

            impl arrow2_convert::serialize::ArrowMutableArray for #mutable_array_name {
                fn reserve(&mut self, additional: usize, _additional_values: usize) {
                    if let Some(x) = self.validity.as_mut() {
                        x.reserve(additional)
                    }
                    #(<<#field_types as arrow2_convert::serialize::ArrowSerialize>::MutableArrayType as arrow2_convert::serialize::ArrowMutableArray>::reserve(&mut self.#field_names, additional, _additional_values);)*        
                }
            }
            
            impl<T: std::borrow::Borrow<#original_name>> arrow2::array::TryPush<Option<T>> for #mutable_array_name {
                fn try_push(&mut self, item: Option<T>) -> arrow2::error::Result<()> {
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

            impl<T: std::borrow::Borrow<#original_name>> arrow2::array::TryExtend<Option<T>> for #mutable_array_name {
                fn try_extend<I: IntoIterator<Item = Option<T>>>(&mut self, iter: I) -> arrow2::error::Result<()> {
                    use arrow2::array::TryPush;
                    for i in iter {
                        self.try_push(i)?;
                    }
                    Ok(())
                }
            }

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

            impl arrow2_convert::serialize::ArrowSerialize for #original_name {
                type MutableArrayType = #mutable_array_name;
    
                #[inline]
                fn new_array() -> Self::MutableArrayType {
                    Self::MutableArrayType::default()
                }
    
                fn arrow_serialize(v: &Self, array: &mut Self::MutableArrayType) -> arrow2::error::Result<()> {
                    use arrow2::array::TryPush;
                    array.try_push(Some(v))
                }
            }    
        });
    }
            
    if gen_deserialize {
        generated.extend(quote! { 
            #visibility struct #array_name
            {
                array: Box<dyn arrow2::array::Array>
            }

            impl arrow2_convert::deserialize::ArrowArray for #array_name 
            {
                type BaseArrayType = arrow2::array::StructArray;

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

            impl<'a> IntoIterator for &'a #array_name 
            {
                type Item = Option<#original_name>;
                type IntoIter = #iterator_name<'a>;

                fn into_iter(self) -> Self::IntoIter {
                    unimplemented!("Use iter_from_array_ref");
                }
            }

            #visibility struct #iterator_name<'a> {
                #(
                    #field_names: <&'a <#field_types as arrow2_convert::deserialize::ArrowDeserialize>::ArrayType as IntoIterator>::IntoIter, 
                )*
                validity_iter: arrow2::bitmap::utils::BitmapIter<'a>,
                has_validity: bool
            }

            impl<'a> #iterator_name<'a> {
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

                fn consume_next(&mut self) {
                    #(let _ = self.#field_names.next();)*
                }
            }

            impl<'a> Iterator for #iterator_name<'a> {
                type Item = Option<#original_name>;

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

            impl arrow2_convert::deserialize::ArrowDeserialize for #original_name {
                type ArrayType = #array_name;
    
                fn arrow_deserialize<'a>(v: Option<Self>) -> Option<Self> {
                    v
                }
            }    
        });
    }

    generated
}
