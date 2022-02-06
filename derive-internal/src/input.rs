use proc_macro2::{Span, TokenStream};
use quote::quote;
use syn::{Data, DeriveInput, Field, Ident, Lit, Meta, MetaNameValue, Visibility};

/// Representing the struct we are deriving
pub struct Input {
    /// The input struct name
    pub name: Ident,
    /// The list of traits to derive passed to `soa_derive` attribute
    pub derives: Vec<Ident>,
    /// The list of fields in the struct
    pub fields: Vec<Field>,
    /// The struct overall visibility
    pub visibility: Visibility,
}

impl Input {
    pub fn new(input: DeriveInput) -> Input {
        let fields = match input.data {
            Data::Struct(s) => s.fields.iter().cloned().collect::<Vec<_>>(),
            _ => panic!("#[derive(StructOfArray)] only supports structs."),
        };

        let mut derives: Vec<Ident> = vec![];
        for attr in input.attrs {
            if let Ok(meta) = attr.parse_meta() {
                if meta.path().is_ident("arrow2_derive") {
                    match meta {
                        Meta::NameValue(MetaNameValue {
                            lit: Lit::Str(string),
                            ..
                        }) => {
                            for value in string.value().split(',') {
                                derives.push(Ident::new(value.trim(), Span::call_site()));
                            }
                        }
                        _ => panic!(
                            "expected #[arrow2_derive = \"Traits, To, Derive\"], got #[{}]",
                            quote!(#meta)
                        ),
                    }
                }
            }
        }

        Input {
            name: input.ident,
            derives,
            fields,
            visibility: input.vis,
        }
    }

    pub fn derive(&self) -> TokenStream {
        if self.derives.is_empty() {
            TokenStream::new()
        } else {
            let derives = &self.derives;
            quote!(
                #[derive(
                    #(#derives,)*
                )]
            )
        }
    }

    pub fn mutable_array_name(&self) -> Ident {
        Ident::new(&format!("Mutable{}Array", self.name), Span::call_site())
    }

    pub fn array_name(&self) -> Ident {
        Ident::new(&format!("{}Array", self.name), Span::call_site())
    }

    pub fn iterator_name(&self) -> Ident {
        Ident::new(&format!("{}ArrayIterator", self.name), Span::call_site())
    }
}
