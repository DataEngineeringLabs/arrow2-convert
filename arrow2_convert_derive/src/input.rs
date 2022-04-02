use proc_macro2::Span;
use proc_macro_error::{abort, ResultExt};
use syn::{Data, DeriveInput, Ident, Lit, Meta, MetaNameValue, Visibility};

#[derive(PartialEq)]
pub enum TraitsToDerive {
    FieldOnly,
    SerializeOnly,
    DeserializeOnly,
    All,
}

/// Representing the struct we are deriving
pub struct Input {
    /// The input struct name
    pub name: Ident,
    /// The traits to derive
    pub traits_to_derive: TraitsToDerive,
    /// The list of fields in the struct
    pub fields: Vec<Field>,
    /// The struct overall visibility
    pub visibility: Visibility,
}

pub struct Field {
    pub syn: syn::Field,
    pub field_type: syn::Type,
}

fn arrow_field(field: &syn::Field) -> syn::Type {
    for attr in &field.attrs {
        if let Ok(meta) = attr.parse_meta() {
            if meta.path().is_ident("arrow_field") {
                if let Meta::List(list) = meta {
                    for nested in list.nested {
                        if let syn::NestedMeta::Meta(meta) = nested {
                            match meta {
                                Meta::NameValue(MetaNameValue {
                                    lit: Lit::Str(string),
                                    path,
                                    ..
                                }) => {
                                    if path.is_ident("override") {
                                        return syn::parse_str(&string.value()).unwrap_or_abort();
                                    }
                                }
                                _ => {
                                    use syn::spanned::Spanned;
                                    abort!(meta.span(), "Unexpected attribute");
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    field.ty.clone()
}

impl Input {
    pub fn new(input: DeriveInput) -> Input {
        let mut traits_to_derive = TraitsToDerive::All;

        let fields = match input.data {
            Data::Struct(s) => s
                .fields
                .iter()
                .map(|f| Field {
                    syn: f.clone(),
                    field_type: arrow_field(f),
                })
                .collect::<Vec<_>>(),
            _ => abort!(
                input.ident.span(),
                "#[derive(ArrowField)] only supports structs."
            ),
        };

        let mut derives: Vec<Ident> = vec![];
        for attr in input.attrs {
            if let Ok(meta) = attr.parse_meta() {
                if meta.path().is_ident("arrow2_convert") {
                    match meta {
                        Meta::NameValue(MetaNameValue {
                            lit: Lit::Str(string),
                            ..
                        }) => {
                            for value in string.value().split(',') {
                                match value {
                                    "field_only" | "serialize_only" | "deserialize_only" => {
                                        if traits_to_derive != TraitsToDerive::All {
                                            abort!(string.span(), "Only one of field_only, serialize-only or deserialize_only can be specified");
                                        }

                                        match value {
                                            "field_only" => {
                                                traits_to_derive = TraitsToDerive::FieldOnly;
                                            }
                                            "serialize_only" => {
                                                traits_to_derive = TraitsToDerive::SerializeOnly;
                                            }
                                            "deserialize_only" => {
                                                traits_to_derive = TraitsToDerive::DeserializeOnly;
                                            }
                                            _ => panic!("Unexpected {}", value), // intentionally leave as panic since we should never get here
                                        }
                                    }
                                    _ => abort!(string.span(), "Unexpected {}", value),
                                }
                                derives.push(Ident::new(value.trim(), Span::call_site()));
                            }
                        }
                        _ => {
                            use syn::spanned::Spanned;
                            abort!(meta.span(), "Unexpected attribute");
                        }
                    }
                }
            }
        }

        Input {
            name: input.ident,
            fields,
            visibility: input.vis,
            traits_to_derive,
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
