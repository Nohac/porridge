use proc_macro::{Delimiter, TokenStream, TokenTree};

#[proc_macro_derive(Component, attributes(component))]
pub fn derive_component(input: TokenStream) -> TokenStream {
    let use_hash = has_hash_attribute(input.clone());
    let untracked = has_untracked_attribute(input.clone());
    let Some(name) = type_name(input) else {
        return compile_error("Component can only be derived for structs and enums");
    };

    match (use_hash, untracked) {
        (true, true) => format!(
            "impl ::bowl::Component for {name} {{
                fn tracked() -> bool {{
                    false
                }}

                fn fingerprint(&self) -> ::core::option::Option<u64> {{
                    ::core::option::Option::Some(::bowl::hash_component(self))
                }}
            }}"
        )
        .parse()
        .expect("generated component impl should parse"),
        (true, false) => format!(
            "impl ::bowl::Component for {name} {{
                fn fingerprint(&self) -> ::core::option::Option<u64> {{
                    ::core::option::Option::Some(::bowl::hash_component(self))
                }}
            }}"
        )
        .parse()
        .expect("generated component impl should parse"),
        (false, true) => format!(
            "impl ::bowl::Component for {name} {{
                fn tracked() -> bool {{
                    false
                }}
            }}"
        )
        .parse()
        .expect("generated component impl should parse"),
        (false, false) => format!("impl ::bowl::Component for {name} {{}}")
            .parse()
            .expect("generated component impl should parse"),
    }
}

fn has_hash_attribute(input: TokenStream) -> bool {
    component_attribute_contains(input, "hash")
}

fn has_untracked_attribute(input: TokenStream) -> bool {
    component_attribute_contains(input, "untracked")
}

fn component_attribute_contains(input: TokenStream, needle: &str) -> bool {
    let mut tokens = input.into_iter().peekable();

    while let Some(token) = tokens.next() {
        let TokenTree::Punct(punct) = token else {
            continue;
        };

        if punct.as_char() != '#' {
            continue;
        }

        let Some(TokenTree::Group(attribute)) = tokens.next() else {
            continue;
        };

        if attribute.delimiter() != Delimiter::Bracket {
            continue;
        }

        if attribute_contains_component_ident(attribute.stream(), needle) {
            return true;
        }
    }

    false
}

fn attribute_contains_component_ident(attribute: TokenStream, needle: &str) -> bool {
    let mut tokens = attribute.into_iter();
    let Some(TokenTree::Ident(ident)) = tokens.next() else {
        return false;
    };

    if ident.to_string() != "component" {
        return false;
    }

    tokens.any(|token| match token {
        TokenTree::Group(group) if group.delimiter() == Delimiter::Parenthesis => group
            .stream()
            .into_iter()
            .any(|inner| matches!(inner, TokenTree::Ident(ident) if ident.to_string() == needle)),
        _ => false,
    })
}

fn type_name(input: TokenStream) -> Option<String> {
    let mut saw_type_keyword = false;

    for token in input {
        match token {
            TokenTree::Ident(ident)
                if ident.to_string() == "struct" || ident.to_string() == "enum" =>
            {
                saw_type_keyword = true;
            }
            TokenTree::Ident(ident) if saw_type_keyword => return Some(ident.to_string()),
            TokenTree::Group(group) if group.delimiter() == Delimiter::Brace => {}
            _ => {}
        }
    }

    None
}

fn compile_error(message: &str) -> TokenStream {
    format!("compile_error!({message:?});")
        .parse()
        .expect("generated compile_error should parse")
}
