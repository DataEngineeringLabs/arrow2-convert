use proc_macro2::TokenStream;
use proc_macro_error::proc_macro_error;
use quote::TokenStreamExt;

mod _struct;
mod input;

/// Derive macro for the Array trait.
#[proc_macro_error]
#[proc_macro_derive(ArrowField, attributes(arrow2_convert))]
pub fn arrow2_convert_derive_field(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let ast = syn::parse(input).unwrap();
    let input = input::Input::new(ast);

    // Build the output, possibly using quasi-quotation
    let mut generated = TokenStream::new();
    generated.append_all(_struct::expand_derive(&input));
    generated.into()
}
