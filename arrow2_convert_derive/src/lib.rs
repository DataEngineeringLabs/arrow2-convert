use proc_macro_error::{abort, proc_macro_error};

mod derive_enum;
mod derive_struct;
mod input;

use input::*;

/// Derive macro for arrow fields
#[proc_macro_error]
#[proc_macro_derive(ArrowField, attributes(arrow_field))]
pub fn arrow2_convert_derive_field(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let ast: syn::DeriveInput = syn::parse(input).unwrap();

    match &ast.data {
        syn::Data::Enum(e) => derive_enum::expand(DeriveEnum::from_ast(&ast, e)).into(),
        syn::Data::Struct(s) => derive_struct::expand(DeriveStruct::from_ast(&ast, s)).into(),
        _ => {
            abort!(ast.ident.span(), "Only structs and enums supported");
        }
    }
}
