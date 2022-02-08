use proc_macro2::TokenStream;
use quote::{quote, quote_spanned};
use syn::spanned::Spanned;

use super::input::Input;

/*
// likely needed for slices
fn type_to_ref(v: &str, is_nullable: bool) -> syn::Type {
    if is_nullable {
        match v {
            "u8" => parse_quote!(Option<u8>),
            "u16" => parse_quote!(Option<u16>),
            "u32" => parse_quote!(Option<u32>),
            "u64" => parse_quote!(Option<u64>),
            "i8" => parse_quote!(Option<i8>),
            "i16" => parse_quote!(Option<i16>),
            "i32" => parse_quote!(Option<i32>),
            "i64" => parse_quote!(Option<i64>),
            "f32" => parse_quote!(Option<f32>),
            "f64" => parse_quote!(Option<f64>),
            "String" => parse_quote!(Option<&str>),
            other => panic!("Type {} not supported", other),
        }
    } else {
        match v {
            "u8" => parse_quote!(u8),
            "u16" => parse_quote!(u16),
            "u32" => parse_quote!(u32),
            "u64" => parse_quote!(u64),
            "i8" => parse_quote!(i8),
            "i16" => parse_quote!(i16),
            "i32" => parse_quote!(i32),
            "i64" => parse_quote!(i64),
            "f32" => parse_quote!(f32),
            "f64" => parse_quote!(f64),
            "String" => parse_quote!(&str),
            other => panic!("Type {} not supported", other),
        }
    }
}

fn tree_to_ref(tree: &ParseTree) -> syn::Type {
    match tree {
        ParseTree::Type(arg, is_nullable) => type_to_ref(arg, *is_nullable),
        ParseTree::Vec(arg, is_nullable) => {
            if let ParseTree::Type(arg, inner_is_nullable) = arg.as_ref() {
                match (is_nullable, arg.as_ref()) {
                    (true, "u8") => parse_quote!(Option<&[u8]>),
                    (false, "u8") => parse_quote!(&[u8]),
                    (true, other) => {
                        let inner = type_to_ref(other, *inner_is_nullable);
                        parse_quote!(Option<Vec<#inner>>)
                    }
                    (false, other) => {
                        let inner = type_to_ref(other, *inner_is_nullable);
                        parse_quote!(Vec<#inner>)
                    }
                }
            } else {
                todo!("Vec<{:?}> is still not implemented", arg)
            }
        }
    }
}
 */

pub fn derive(input: &Input) -> TokenStream {
    let original_name = &input.name;
    let vec_name_str = format!("Vec<{}>", original_name);
    let other_derive = &input.derive();
    let visibility = &input.visibility;

    let mutable_array_name = &input.mutable_array_name();
    let array_name = &input.array_name();
    let iterator_name = &input.iterator_name();

    let field_names = input
        .fields
        .iter()
        .map(|field| field.ident.as_ref().unwrap())
        .collect::<Vec<_>>();

    if field_names.len() == 0 {
        panic!("Struct needs more than one field");
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
        .map(|field| match &field.ty {
            syn::Type::Path(path) => {
                path
            },
            _ => panic!("Only types are supported atm"),
        })
        .collect::<Vec<&syn::TypePath>>();

    let mutable_field_array_types = field_types
        .iter()
        .map(|field_type| quote_spanned!( field_type.span() => <#field_type as arrow2_derive::ArrowSerialize>::MutableArrayType))
        .collect::<Vec<TokenStream>>();

    let generated = quote! {
        /// An analog to `
        #[doc = #vec_name_str]
        /// ` with Struct of Arrow (SoA) layout
        #other_derive
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
                    #(#field_names: <#field_types as arrow2_derive::ArrowSerialize>::MutableArrayType::default(),)*
                    data_type: <#original_name as arrow2_derive::ArrowField>::data_type(),
                    validity: None,
                }
            }

            pub fn fields() -> Vec<arrow2::datatypes::Field> {
                vec![
                    #(
                        <#field_types as arrow2_derive::ArrowField>::field(#field_names_str),
                    )*
                ]
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

        impl arrow2_derive::ArrowMutableArray for #mutable_array_name {
            fn into_arc(self) -> std::sync::Arc<dyn arrow2::array::Array> {
                std::sync::Arc::new(arrow2::array::StructArray::from(self)) as std::sync::Arc<dyn arrow2::array::Array>
            }
        }
 
        impl<T> arrow2_derive::ArrowMutableArrayTryPushGeneric<T> for #mutable_array_name
        where T: arrow2_derive::ArrowSerialize, Self: arrow2::array::TryPush<Option<T::SerializeOutput>>
        {}
        
        impl arrow2::array::TryPush<Option<#original_name>> for #mutable_array_name {
            fn try_push(&mut self, item: Option<#original_name>) -> arrow2::error::Result<()> {
                use arrow2::array::MutableArray;

                match item {
                    Some(i) =>  {
                        let #original_name {
                            #(#field_names,)*
                        } = i;
                        #(
                            <#mutable_field_array_types as arrow2_derive::ArrowMutableArrayTryPushGeneric<#field_types>>::try_push_generic(&mut self.#field_names, #field_names)?;
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

        impl arrow2::array::TryExtend<Option<#original_name>> for #mutable_array_name {
            fn try_extend<I: IntoIterator<Item = Option<#original_name>>>(&mut self, iter: I) -> arrow2::error::Result<()> {
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
                    <#original_name as arrow2_derive::ArrowField>::data_type().clone(), 
                    values, 
                    std::mem::take(&mut self.validity).map(|x| x.into()),
                ))
            }
        
            fn as_arc(&mut self) -> std::sync::Arc<dyn arrow2::array::Array> {
                let values = vec![#(
                    <#mutable_field_array_types as arrow2::array::MutableArray>::as_arc(&mut self.#field_names), 
                )*];

                std::sync::Arc::new(arrow2::array::StructArray::from_data(
                    <#original_name as arrow2_derive::ArrowField>::data_type().clone(), 
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
                <Self as arrow2_derive::ArrowMutableArrayTryPushGeneric<Option<#original_name>>>::try_push_generic(self, None).unwrap();
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

        impl From<#mutable_array_name> for arrow2::array::StructArray  {
            fn from(other: #mutable_array_name) -> Self {
                let values = vec![#(
                    <#mutable_field_array_types as arrow2_derive::ArrowMutableArray>::into_arc(other.#field_names), 
                )*];

                let validity = if other.validity.as_ref().map(|x| x.null_count()).unwrap_or(0) > 0 {
                    other.validity.map(|x| x.into())
                } else {
                    None
                };        

                arrow2::array::StructArray::from_data(
                    <#original_name as arrow2_derive::ArrowField>::data_type(), 
                    values, 
                    validity
                )
            }
        }

        #visibility struct #array_name
        {
            array: Box<dyn arrow2::array::Array>
        }

        impl arrow2_derive::ArrowArray for #array_name 
        {
            type BaseArrayType = arrow2::array::StructArray;

            fn iter_from_array_ref<'a>(b: &'a dyn arrow2::array::Array)  -> arrow2::error::Result<<&'a Self as IntoIterator>::IntoIter>
            {
                use core::ops::Deref;
                let arr = b.as_any().downcast_ref::<arrow2::array::StructArray>().unwrap();
                let values = arr.values();
                let validity = arr.validity();
                // for now do a straight comp
                Ok(#iterator_name {
                    #(
                        #field_names: <<#field_types as arrow2_derive::ArrowDeserialize>::ArrayType as arrow2_derive::ArrowArray>::iter_from_array_ref(values[#field_indices].deref())?, 
                    )*
                    has_validity: validity.as_ref().is_some(),
                    validity_iter: validity.as_ref().map(|x| x.iter()).unwrap_or_else(|| arrow2::bitmap::utils::BitmapIter::new(&[], 0, 0))
                })
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
                #field_names: <&'a <#field_types as arrow2_derive::ArrowDeserialize>::ArrayType as IntoIterator>::IntoIter, 
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
                        #(#field_names: <#field_types as arrow2_derive::ArrowDeserialize>::arrow_deserialize_internal(#field_names),)*
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

        impl arrow2_derive::ArrowField for #original_name {
            fn data_type() -> arrow2::datatypes::DataType {
                arrow2::datatypes::DataType::Struct(
                    #mutable_array_name::fields()
                )
            }
        }

        impl arrow2_derive::ArrowSerialize for #original_name {
            type MutableArrayType = #mutable_array_name;
            type SerializeOutput = #original_name;

            fn arrow_serialize(v: Option<Self>) -> Option<#original_name> {
                v
            }
        }

        impl arrow2_derive::ArrowDeserialize for #original_name {
            type ArrayType = #array_name;

            fn arrow_deserialize<'a>(v: Option<Self>) -> Option<Self> {
                v
            }
        }

        arrow2_derive::arrow_enable_vec_for_type!(#original_name);
    };

    generated
}
