use proc_macro2::TokenStream;
use quote::quote;

use super::input::Input;

macro_rules! primitive {
    ($type:ty) => {{
        let path: syn::TypePath =
            syn::parse(quote! {arrow2::array::MutablePrimitiveArray<$type>}.into()).unwrap();
        Some(syn::Type::Path(path))
    }};
}

fn type_to_array(v: &str) -> Option<syn::Type> {
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
            Some(syn::Type::Path(path))
        }
        _ => None,
    }
}

pub fn derive(input: &Input) -> TokenStream {
    let name = &input.name;
    let vec_name_str = format!("Vec<{}>", name);
    let other_derive = &input.derive();
    let visibility = &input.visibility;

    let name = &input.vec_name();

    let fields_names = &input
        .fields
        .iter()
        .map(|field| field.ident.as_ref().unwrap())
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

    let fields_types = &input
        .fields
        .iter()
        .map(|field| match &field.ty {
            syn::Type::Path(a) => {
                let a = a.path.segments[0].ident.to_string();
                type_to_array(&a).unwrap_or_else(|| field.ty.clone())
            }
            other => other.clone(),
        })
        .collect::<Vec<_>>();

    let generated = quote! {
        /// An analog to `
        #[doc = #vec_name_str]
        /// ` with Struct of Arrow (SoA) layout
        #[allow(dead_code)]
        #other_derive
        #visibility struct #name {
            #(
                #[doc = #fields_doc]
                pub #fields_names: #fields_types,
            )*
        }

        impl #name {
            pub fn new() -> Self {
                Self::default()
            }
        }
    };

    generated
}
