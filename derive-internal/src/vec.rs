use proc_macro2::TokenStream;
use quote::quote;
use syn::parse_quote;

use super::input::Input;
use super::parse::{parse, ParseTree};

macro_rules! to_datatype {
    ($type:tt) => {{
        parse_quote!(arrow2::datatypes::DataType::$type)
    }};
}

fn type_to_array(v: &str) -> syn::Type {
    if matches!(
        v,
        "u8" | "u16" | "u32" | "u64" | "i8" | "i16" | "i32" | "i64" | "f32" | "f64"
    ) {
        let a: proc_macro2::TokenStream = v.parse().unwrap();
        parse_quote!(arrow2::array::MutablePrimitiveArray<#a>)
    } else if v == "String" {
        parse_quote!(arrow2::array::MutableUtf8Array<i32>)
    } else if v == "bool" {
        parse_quote!(arrow2::array::MutableBooleanArray)
    } else {
        panic!("Type {} not supported", v)
    }
}

fn tree_to_array(tree: &ParseTree) -> (syn::Type, bool) {
    match tree {
        ParseTree::Type(arg, is_nullabe) => (type_to_array(arg), *is_nullabe),
        ParseTree::Vec(arg, is_nullable) => {
            if let ParseTree::Type(arg, _) = arg.as_ref() {
                let type_ = match arg.as_ref() {
                    "u8" => parse_quote!(arrow2::array::MutableBinaryArray<i32>),
                    other => {
                        let array = type_to_array(other);
                        parse_quote!(arrow2::array::MutableListArray<i32, #array>)
                    }
                };
                (type_, *is_nullable)
            } else {
                todo!("Vec<{:?}> is still not implemented", arg)
            }
        }
    }
}

fn type_to_datatype(v: &str) -> syn::Expr {
    match v {
        "bool" => to_datatype!(Boolean),
        "u8" => to_datatype!(UInt8),
        "u16" => to_datatype!(UInt16),
        "u32" => to_datatype!(UInt32),
        "u64" => to_datatype!(UInt64),
        "i8" => to_datatype!(Int8),
        "i16" => to_datatype!(Int16),
        "i32" => to_datatype!(Int32),
        "i64" => to_datatype!(Int64),
        "f32" => to_datatype!(Float32),
        "f64" => to_datatype!(Float64),
        "String" => to_datatype!(Utf8),
        other => panic!("Type {} not supported", other),
    }
}

fn tree_to_datatype(tree: &ParseTree) -> syn::Expr {
    match tree {
        ParseTree::Type(arg, _) => type_to_datatype(arg),
        ParseTree::Vec(arg, _) => {
            if let ParseTree::Type(arg, is_nullable) = arg.as_ref() {
                match arg.as_ref() {
                    "u8" => to_datatype!(Binary),
                    _ => {
                        let inner = type_to_datatype(arg);
                        let is_nullable = *is_nullable;
                        parse_quote!({
                            arrow2::datatypes::DataType::List(Box::new(
                            arrow2::datatypes::Field::new(
                                "item",
                                #inner,
                                #is_nullable,
                            )
                        ))
                        })
                    }
                }
            } else {
                todo!("Vec<{:?}> is still not implemented", arg)
            }
        }
    }
}

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

    let name = &input.vec_name();

    let fields_names = input
        .fields
        .iter()
        .map(|field| field.ident.as_ref().unwrap())
        .collect::<Vec<_>>();

    let fields_names_str = fields_names
        .iter()
        .map(|field| syn::LitStr::new(&format!("{}", field), proc_macro2::Span::call_site()))
        .collect::<Vec<_>>();

    let fields_doc = fields_names
        .iter()
        .map(|field| {
            format!(
                "A vector of `{0}` from a [`{1}`](struct.{1}.html)",
                field, name
            )
        })
        .collect::<Vec<_>>();

    let tree = input
        .fields
        .iter()
        .map(|field| match &field.ty {
            syn::Type::Path(path) => parse(path, false),
            _ => panic!("Only types are supported atm"),
        })
        .collect::<Vec<_>>();

    let (fields_types, fields_nullable): (Vec<_>, Vec<_>) =
        tree.iter().map(|tree| tree_to_array(tree)).unzip();

    let fields_datatypes = tree
        .iter()
        .map(|tree| tree_to_datatype(tree))
        .collect::<Vec<_>>();

    let mut required_fields = vec![];
    let mut nullable_fields = vec![];
    fields_names
        .iter()
        .zip(fields_nullable.iter())
        .for_each(|(field, is_nullable)| {
            if *is_nullable {
                nullable_fields.push(*field)
            } else {
                required_fields.push(*field)
            }
        });
    let n_fields = fields_types.len();
    let fields_enumerate = (0..n_fields).collect::<Vec<_>>();

    let generated = quote! {
        /// An analog to `
        #[doc = #vec_name_str]
        /// ` with Struct of Arrow (SoA) layout
        #[allow(dead_code)]
        #other_derive
        #visibility struct #name {
            #(
                #[doc = #fields_doc]
                #fields_names: #fields_types,
            )*
        }

        impl #name {
            pub fn new() -> Self {
                Self::default()
            }
        }

        impl #name {
            fn push(&mut self, item: #original_name) {
                let #original_name {
                    #(#fields_names,)*
                } = item;
                #(self.#required_fields.try_push(Some(#required_fields)).unwrap();)*;
                #(self.#nullable_fields.try_push(#nullable_fields).unwrap();)*
            }

            /*
            // todo: need a "Slice" struct to not clone strings
            fn value(&self, i: usize) -> #original_name {
                #original_name {
                    #(#fields_names: self.#fields_names,)*
                }
            }
            */
        }

        impl From<#name> for StructArray  {
            fn from(other: #name) -> Self {
                let fields = (0..#name::n_fields())
                    .map(#name::field)
                    .collect();
                // to macro
                let #name { #(#fields_names, )* } = other;
                let values = vec![#(#fields_names.into_arc(), )*];

                StructArray::from_data(fields, values, None)
            }
        }

        impl ArrowStruct for #name {
            fn n_fields() -> usize {
                #n_fields
            }

            fn field(i: usize) -> Field {
                match i {
                    #(#fields_enumerate => Field::new(#fields_names_str, #fields_datatypes, #fields_nullable),)*
                    _ => panic!(),
                }
            }
        }
    };

    generated
}
