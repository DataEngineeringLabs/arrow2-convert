#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParseTree {
    Type(String, bool),
    Vec(Box<ParseTree>, bool),
}

fn parse_bracket_arg(args: &syn::PathArguments, is_nullable: bool) -> ParseTree {
    if let syn::PathArguments::AngleBracketed(f) = args {
        if let syn::GenericArgument::Type(syn::Type::Path(path)) = &f.args[0] {
            parse(path, is_nullable)
        } else {
            unreachable!()
        }
    } else {
        unreachable!()
    }
}

pub fn parse(path: &syn::TypePath, is_nullable: bool) -> ParseTree {
    match path.path.segments[0].ident.to_string().as_ref() {
        "Option" => {
            assert!(!is_nullable);
            parse_bracket_arg(&path.path.segments[0].arguments, true)
        }
        "Vec" => {
            let arg = parse_bracket_arg(&path.path.segments[0].arguments, false);
            ParseTree::Vec(Box::new(arg), is_nullable)
        }
        _ => ParseTree::Type(path.path.segments[0].ident.to_string(), is_nullable),
    }
}
