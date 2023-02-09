use proc_macro2::Span;
use proc_macro_error::{abort, ResultExt};
use syn::spanned::Spanned;
use syn::{DeriveInput, Ident, Lit, Meta, MetaNameValue, Visibility};

pub const ARROW_FIELD: &str = "arrow_field";
pub const FIELD_TYPE: &str = "type";
pub const FIELD_SKIP: &str = "skip";
pub const UNION_TYPE: &str = "type";
pub const UNION_TYPE_SPARSE: &str = "sparse";
pub const UNION_TYPE_DENSE: &str = "dense";
pub const TRANSPARENT: &str = "transparent";

pub struct DeriveCommon {
    /// The input name
    pub name: Ident,
    /// The overall visibility
    pub visibility: Visibility,
}

pub struct DeriveStruct {
    pub common: DeriveCommon,
    /// The list of fields in the struct
    pub fields: Vec<DeriveField>,
    pub is_transparent: bool,
}

pub struct DeriveEnum {
    pub common: DeriveCommon,
    /// The list of variants in the enum
    pub variants: Vec<DeriveVariant>,
    pub is_dense: bool,
}

/// All container attributes
pub struct ContainerAttrs {
    pub is_dense: Option<bool>,
    pub transparent: Option<Span>,
}

/// All field attributes
pub struct FieldAttrs {
    pub field_type: Option<syn::Type>,
    pub skip: bool,
}

pub struct DeriveField {
    pub syn: syn::Field,
    pub field_type: syn::Type,
    pub skip: bool,
}

pub struct DeriveVariant {
    pub syn: syn::Variant,
    pub field_type: syn::Type,
    pub is_unit: bool,
}

impl DeriveCommon {
    pub fn from_ast(input: &DeriveInput, _container_attrs: &ContainerAttrs) -> DeriveCommon {
        DeriveCommon {
            name: input.ident.clone(),
            visibility: input.vis.clone(),
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

impl ContainerAttrs {
    pub fn from_ast(attrs: &[syn::Attribute]) -> ContainerAttrs {
        let mut is_dense: Option<bool> = None;
        let mut is_transparent: Option<Span> = None;

        for attr in attrs {
            if let Ok(meta) = attr.parse_meta() {
                if meta.path().is_ident(ARROW_FIELD) {
                    if let Meta::List(list) = meta {
                        for nested in list.nested {
                            if let syn::NestedMeta::Meta(meta) = nested {
                                match meta {
                                    syn::Meta::NameValue(MetaNameValue {
                                        lit: Lit::Str(string),
                                        path,
                                        ..
                                    }) if path.is_ident(UNION_TYPE) => {
                                        match string.value().as_ref() {
                                            UNION_TYPE_DENSE => {
                                                is_dense = Some(true);
                                            }
                                            UNION_TYPE_SPARSE => {
                                                is_dense = Some(false);
                                            }
                                            _ => {
                                                abort!(path.span(), "Unexpected value for mode");
                                            }
                                        }
                                    }

                                    Meta::Path(path) if path.is_ident(TRANSPARENT) => {
                                        is_transparent = Some(path.span());
                                    }

                                    _ => {
                                        abort!(meta.span(), "Unexpected attribute");
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        ContainerAttrs {
            is_dense,
            transparent: is_transparent,
        }
    }
}

impl FieldAttrs {
    pub fn from_ast(input: &[syn::Attribute]) -> FieldAttrs {
        let mut field_type: Option<syn::Type> = None;
        let mut skip = false;

        for attr in input {
            if let Ok(meta) = attr.parse_meta() {
                if meta.path().is_ident(ARROW_FIELD) {
                    if let Meta::List(list) = meta {
                        for nested in list.nested {
                            if let syn::NestedMeta::Meta(meta) = nested {
                                match meta {
                                    Meta::NameValue(MetaNameValue {
                                        lit: Lit::Str(string),
                                        path,
                                        ..
                                    }) if path.is_ident(FIELD_TYPE) => {
                                        field_type =
                                            Some(syn::parse_str(&string.value()).unwrap_or_abort());
                                    }
                                    Meta::Path(path) if path.is_ident(FIELD_SKIP) => skip = true,
                                    _ => {
                                        abort!(meta.span(), "Unexpected attribute");
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        FieldAttrs { field_type, skip }
    }
}

impl DeriveStruct {
    pub fn from_ast(input: &DeriveInput, ast: &syn::DataStruct) -> DeriveStruct {
        let container_attrs = ContainerAttrs::from_ast(&input.attrs);
        let common = DeriveCommon::from_ast(input, &container_attrs);

        let is_transparent = if let Some(span) = container_attrs.transparent {
            if ast.fields.len() > 1 {
                abort!(span, "'transparent' is only supported on length-1 structs!");
            }
            true
        } else {
            false
        };

        DeriveStruct {
            common,
            fields: ast
                .fields
                .iter()
                .map(DeriveField::from_ast)
                .collect::<Vec<_>>(),
            is_transparent,
        }
    }
}

impl DeriveEnum {
    pub fn from_ast(input: &DeriveInput, ast: &syn::DataEnum) -> DeriveEnum {
        let container_attrs = ContainerAttrs::from_ast(&input.attrs);
        let common = DeriveCommon::from_ast(input, &container_attrs);

        DeriveEnum {
            common,
            variants: ast
                .variants
                .iter()
                .map(DeriveVariant::from_ast)
                .collect::<Vec<_>>(),
            is_dense: container_attrs
                .is_dense
                .unwrap_or_else(|| abort!(input.span(), "Missing mode attribute for enum")),
        }
    }
}

impl DeriveField {
    pub fn from_ast(input: &syn::Field) -> DeriveField {
        let attrs = FieldAttrs::from_ast(&input.attrs);

        DeriveField {
            syn: input.clone(),
            field_type: attrs.field_type.unwrap_or_else(|| input.ty.clone()),
            skip: attrs.skip,
        }
    }
}

impl DeriveVariant {
    pub fn from_ast(input: &syn::Variant) -> DeriveVariant {
        let attrs = FieldAttrs::from_ast(&input.attrs);

        let (is_unit, field_type) = match &input.fields {
            syn::Fields::Named(_f) => {
                unimplemented!()
            }
            syn::Fields::Unnamed(f) => {
                if f.unnamed.len() > 1 {
                    unimplemented!()
                } else {
                    (false, f.unnamed[0].ty.clone())
                }
            }
            syn::Fields::Unit => (true, syn::parse_str("bool").unwrap_or_abort()),
        };
        DeriveVariant {
            syn: input.clone(),
            field_type: attrs.field_type.unwrap_or_else(|| field_type.clone()),
            is_unit,
        }
    }
}
