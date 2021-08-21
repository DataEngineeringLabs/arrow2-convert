use proc_macro2::TokenStream;
use quote::quote;

use super::input::Input;

macro_rules! primitive {
    ($type:ty) => {{
        let path: syn::TypePath =
            syn::parse(quote! {arrow2::array::MutablePrimitiveArray<$type>}.into()).unwrap();
        syn::Type::Path(path)
    }};
}

macro_rules! to_type {
    ($type:ty) => {{
        let type_: syn::Type = syn::parse(quote! {$type}.into()).unwrap();
        type_
    }};
}

macro_rules! to_datatype {
    ($type:tt) => {{
        let type_: syn::Type =
            syn::parse(quote! {arrow2::datatypes::DataType::$type}.into()).unwrap();
        type_
    }};
}

fn type_to_array(v: &str) -> syn::Type {
    match v {
        "u8" => primitive!(u8),
        "u16" => primitive!(u16),
        "u32" => primitive!(u32),
        "u64" => primitive!(u64),
        "i8" => primitive!(i8),
        "i16" => primitive!(i16),
        "i32" => primitive!(i32),
        "i64" => primitive!(i64),
        "f32" => primitive!(f32),
        "f64" => primitive!(f64),
        "String" => {
            let path: syn::TypePath =
                syn::parse(quote! {arrow2::array::MutableUtf8Array<i32>}.into()).unwrap();
            syn::Type::Path(path)
        }
        other => panic!("Type {} not supported", other),
    }
}

fn type_to_datatype(v: &str) -> syn::Type {
    match v {
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

fn type_to_ref(v: &str, is_nullable: bool) -> syn::Type {
    if is_nullable {
        match v {
            "u8" => to_type!(Option<u8>),
            "u16" => to_type!(Option<u16>),
            "u32" => to_type!(Option<u32>),
            "u64" => to_type!(Option<u64>),
            "i8" => to_type!(Option<i8>),
            "i16" => to_type!(Option<i16>),
            "i32" => to_type!(Option<i32>),
            "i64" => to_type!(Option<i64>),
            "f32" => to_type!(Option<f32>),
            "f64" => to_type!(Option<f64>),
            "String" => {
                let type_: syn::Type = syn::parse(quote! {Option<&str>}.into()).unwrap();
                type_
            }
            other => panic!("Type {} not supported", other),
        }
    } else {
        match v {
            "u8" => to_type!(u8),
            "u16" => to_type!(u16),
            "u32" => to_type!(u32),
            "u64" => to_type!(u64),
            "i8" => to_type!(i8),
            "i16" => to_type!(i16),
            "i32" => to_type!(i32),
            "i64" => to_type!(i64),
            "f32" => to_type!(f32),
            "f64" => to_type!(f64),
            "String" => {
                let type_: syn::Type = syn::parse(quote! {&str}.into()).unwrap();
                type_
            }
            other => panic!("Type {} not supported", other),
        }
    }
}

fn parse_bracket_arg(args: &syn::PathArguments) -> Option<String> {
    if let syn::PathArguments::AngleBracketed(f) = args {
        if let syn::GenericArgument::Type(syn::Type::Path(a)) = &f.args[0] {
            Some(a.path.segments[0].ident.to_string())
        } else {
            None
        }
    } else {
        None
    }
}

fn parse(path: &syn::TypePath) -> (String, bool) {
    match path.path.segments[0].ident.to_string().as_ref() {
        "Option" => {
            let arg = parse_bracket_arg(&path.path.segments[0].arguments);
            let is_nullabe = arg.is_some();
            let arg = arg.unwrap_or_else(|| path.path.segments[0].ident.to_string());
            (arg, is_nullabe)
        }
        "Vec" => todo!("Vec is still not implemented"),
        _ => (path.path.segments[0].ident.to_string(), false),
    }
}

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

    let (fields_types, fields_nullable): (Vec<_>, Vec<_>) = input
        .fields
        .iter()
        .map(|field| match &field.ty {
            syn::Type::Path(path) => {
                let (arg, is_nullabe) = parse(path);
                (type_to_array(&arg), is_nullabe)
            }
            other => panic!("Type {:?} not supported", other),
        })
        .unzip();
    let fields_options = input
        .fields
        .iter()
        .map(|field| match &field.ty {
            syn::Type::Path(path) => {
                let (arg, is_nullable) = parse(path);
                type_to_ref(&arg, is_nullable)
            }
            other => other.clone(),
        })
        .collect::<Vec<_>>();

    let fields_datatypes = input
        .fields
        .iter()
        .map(|field| match &field.ty {
            syn::Type::Path(path) => {
                let (arg, _) = parse(path);
                type_to_datatype(&arg)
            }
            other => other.clone(),
        })
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
            fn push(&mut self, #(#fields_names: #fields_options,)*) {
                #(self.#required_fields.push(Some(#required_fields));)*;
                #(self.#nullable_fields.push(#nullable_fields);)*
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
