use proc_macro2::TokenStream;
use quote::TokenStreamExt;

mod input;
mod parse;
mod vec;

/// Derive macro for the Array trait.
#[proc_macro_derive(StructOfArrow, attributes(arrow2_derive))]
pub fn arrow2_derive(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let ast = syn::parse(input).unwrap();
    let input = input::Input::new(ast);

    // Build the output, possibly using quasi-quotation
    let mut generated = TokenStream::new();
    generated.append_all(vec::derive(&input));

    generated.into()
}
