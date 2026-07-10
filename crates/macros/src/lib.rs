use proc_macro::{Delimiter, TokenStream, TokenTree};

#[proc_macro_derive(Component, attributes(component, relationship, relationship_target))]
pub fn derive_component(input: TokenStream) -> TokenStream {
    let use_hash = has_hash_attribute(input.clone());
    let untracked = has_untracked_attribute(input.clone());
    let use_revision = has_revision_attribute(input.clone());
    let edge_target = attribute_value(input.clone(), "relationship", "target");
    let target_edge = attribute_value(input.clone(), "relationship_target", "relationship");
    let Some(name) = type_name(input) else {
        return compile_error("Component can only be derived for structs and enums");
    };

    if use_revision && use_hash {
        return compile_error(
            "#[component(hash)] and #[component(revision)] are mutually exclusive",
        );
    }
    if target_edge.is_some() && (use_hash || use_revision) {
        return compile_error(
            "#[relationship_target] generates its fingerprint from the member list; \
             #[component(hash)]/#[component(revision)] are redundant and rejected",
        );
    }
    if edge_target.is_some() && target_edge.is_some() {
        return compile_error(
            "a component cannot be both a #[relationship] edge and a #[relationship_target]",
        );
    }

    let tracked_fn = if untracked {
        "fn tracked() -> bool { false }"
    } else {
        ""
    };

    // The fingerprint, in precedence order: the `revision: u64` field
    // verbatim (change detection without hashing the payload), a hash of
    // the whole value, or — for a maintained inverse — a hash of the
    // ordered member list (fingerprinted by construction).
    let fingerprint_fn = if use_revision {
        "fn fingerprint(&self) -> ::core::option::Option<u64> {
            ::core::option::Option::Some(self.revision)
        }"
        .to_string()
    } else if use_hash {
        "fn fingerprint(&self) -> ::core::option::Option<u64> {
            ::core::option::Option::Some(::bowl::hash_component(self))
        }"
        .to_string()
    } else if target_edge.is_some() {
        "fn fingerprint(&self) -> ::core::option::Option<u64> {
            ::core::option::Option::Some(::bowl::hash_component(&self.0))
        }"
        .to_string()
    } else {
        String::new()
    };

    // #[relationship(target = T)]: this component is an edge whose first
    // tuple field is the target entity; the engine maintains `T` on it.
    let edge_fn = edge_target
        .map(|target| {
            format!(
                "fn relationship_edge(&self) -> ::core::option::Option<::bowl::RelationshipEdge> {{
                    ::core::option::Option::Some(::bowl::RelationshipEdge::new::<{target}>(self.0))
                }}"
            )
        })
        .unwrap_or_default();

    // #[relationship_target(relationship = E)]: this component is the
    // maintained inverse — a tuple struct over `Vec<Entity>`.
    let target_fns = if target_edge.is_some() {
        "fn relationship_retractions(&self) -> ::std::vec::Vec<::bowl::RelationshipRetraction> {
            ::bowl::relationship_retractions_for(self)
        }

        fn relationship_members(&self) -> ::core::option::Option<::std::vec::Vec<::bowl::Entity>> {
            ::core::option::Option::Some(self.0.clone())
        }"
    } else {
        ""
    };

    let target_impl = target_edge
        .map(|edge| {
            format!(
                "impl ::bowl::RelationshipTarget for {name} {{
                    type Edge = {edge};

                    fn from_members(members: ::std::vec::Vec<::bowl::Entity>) -> Self {{
                        {name}(members)
                    }}

                    fn members(&self) -> &[::bowl::Entity] {{
                        &self.0
                    }}
                }}"
            )
        })
        .unwrap_or_default();

    format!(
        "impl ::bowl::Component for {name} {{
            {tracked_fn}
            {fingerprint_fn}
            {edge_fn}
            {target_fns}
        }}
        {target_impl}"
    )
    .parse()
    .expect("generated component impl should parse")
}

fn has_hash_attribute(input: TokenStream) -> bool {
    component_attribute_contains(input, "hash")
}

fn has_untracked_attribute(input: TokenStream) -> bool {
    component_attribute_contains(input, "untracked")
}

fn has_revision_attribute(input: TokenStream) -> bool {
    component_attribute_contains(input, "revision")
}

/// Extracts `value` from `#[attribute(key = value)]`, where the value may
/// be a path (tokens are concatenated until a comma or the end).
fn attribute_value(input: TokenStream, attribute: &str, key: &str) -> Option<String> {
    let mut tokens = input.into_iter().peekable();

    while let Some(token) = tokens.next() {
        let TokenTree::Punct(punct) = token else {
            continue;
        };
        if punct.as_char() != '#' {
            continue;
        }
        let Some(TokenTree::Group(group)) = tokens.next() else {
            continue;
        };
        if group.delimiter() != Delimiter::Bracket {
            continue;
        }

        let mut inner = group.stream().into_iter();
        let Some(TokenTree::Ident(name)) = inner.next() else {
            continue;
        };
        if name.to_string() != attribute {
            continue;
        }
        let Some(TokenTree::Group(args)) = inner.next() else {
            continue;
        };
        if args.delimiter() != Delimiter::Parenthesis {
            continue;
        }

        let mut args = args.stream().into_iter();
        while let Some(token) = args.next() {
            let TokenTree::Ident(ident) = token else {
                continue;
            };
            if ident.to_string() != key {
                continue;
            }
            match args.next() {
                Some(TokenTree::Punct(eq)) if eq.as_char() == '=' => {}
                _ => continue,
            }

            let mut value = String::new();
            for token in args.by_ref() {
                if matches!(&token, TokenTree::Punct(punct) if punct.as_char() == ',') {
                    break;
                }
                value.push_str(&token.to_string());
            }
            if !value.is_empty() {
                return Some(value);
            }
        }
    }

    None
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

/// Derives `SystemParam` for a struct of ambient system params — a *param
/// bundle*. Fields must be ambient (unit-state) params: `View`, `Commands`,
/// `WorldMetaView`, or other bundles (nesting works). Row-driving `Query`
/// params stay top-level in the system signature.
///
/// Supports at most one lifetime parameter and no type parameters.
#[proc_macro_derive(SystemParam)]
pub fn derive_system_param(input: TokenStream) -> TokenStream {
    let bundle = match parse_bundle(input) {
        Ok(bundle) => bundle,
        Err(message) => return compile_error(&message),
    };

    let name = &bundle.name;
    let (impl_generics, self_ty) = match &bundle.lifetime {
        Some(lifetime) => (format!("<'{lifetime}>"), format!("{name}<'{lifetime}>")),
        None => (String::new(), name.clone()),
    };

    let state_tuple = bundle
        .fields
        .iter()
        .map(|field| {
            format!(
                "<{} as ::bowl::__derive::SystemParam>::State,",
                field.ty_with_lifetime(&bundle.lifetime, "'static")
            )
        })
        .collect::<String>();

    let item_ty = match &bundle.lifetime {
        Some(_) => format!("{name}<'__item>"),
        None => name.clone(),
    };

    let mut states_body = String::new();
    let mut states_tuple = String::new();
    let mut keys_body = String::new();
    let mut deps_body = String::new();
    let mut access_body = String::new();
    let mut fetch_fields = String::new();
    let mut always_run_body = String::from("false");
    let mut validate_body = String::new();
    let mut view_types_body = String::new();
    let mut declared_outputs_body = String::new();

    for (index, field) in bundle.fields.iter().enumerate() {
        let static_ty = field.ty_with_lifetime(&bundle.lifetime, "'static");
        let item_lt_ty = field.ty_with_lifetime(&bundle.lifetime, "'__item");
        let field_name = &field.name;

        states_body.push_str(&format!(
            "let __f{index} = <{static_ty} as ::bowl::__derive::SystemParam>::states(snapshot);
             if __f{index}.len() != 1 {{
                 panic!(concat!(
                     \"SystemParam bundles only compose ambient params \",
                     \"(View, Commands, nested bundles); field `{field_name}` has a \",
                     \"row state set - drive rows with a top-level Query instead\"
                 ));
             }}
             let __f{index} = __f{index}.into_iter().next().expect(\"length checked above\");
            "
        ));
        states_tuple.push_str(&format!("__f{index},"));
        keys_body.push_str(&format!(
            "out.extend(<{static_ty} as ::bowl::__derive::SystemParam>::keys(&state.{index}));"
        ));
        deps_body.push_str(&format!(
            "out.extend(<{static_ty} as ::bowl::__derive::SystemParam>::deps(snapshot, &state.{index}));"
        ));
        access_body.push_str(&format!(
            "out.extend(<{static_ty} as ::bowl::__derive::SystemParam>::access(snapshot, &state.{index}));"
        ));
        fetch_fields.push_str(&format!(
            "{field_name}: <{item_lt_ty} as ::bowl::__derive::SystemParam>::fetch(bowl, snapshot, &state.{index}, commands, guards),"
        ));
        always_run_body.push_str(&format!(
            " || <{static_ty} as ::bowl::__derive::SystemParam>::always_run()"
        ));
        validate_body.push_str(&format!(
            "<{static_ty} as ::bowl::__derive::SystemParam>::validate_local()?;"
        ));
        view_types_body.push_str(&format!(
            "<{static_ty} as ::bowl::__derive::SystemParam>::view_sets(out);"
        ));
        declared_outputs_body.push_str(&format!(
            "match <{static_ty} as ::bowl::__derive::SystemParam>::declared_outputs() {{
                ::core::option::Option::Some(types) => out.extend(types),
                ::core::option::Option::None => return ::core::option::Option::None,
            }}"
        ));
    }

    format!(
        "impl{impl_generics} ::bowl::__derive::SystemParam for {self_ty} {{
            type State = ({state_tuple});
            type Item<'__item> = {item_ty};

            fn states(snapshot: &::bowl::__derive::Snapshot) -> ::std::vec::Vec<Self::State> {{
                {states_body}
                ::std::vec![({states_tuple})]
            }}

            fn keys(state: &Self::State) -> ::std::vec::Vec<::bowl::__derive::Entity> {{
                let mut out = ::std::vec::Vec::new();
                {keys_body}
                out
            }}

            fn deps(
                snapshot: &::bowl::__derive::Snapshot,
                state: &Self::State,
            ) -> ::std::vec::Vec<::bowl::__derive::Dep> {{
                let mut out = ::std::vec::Vec::new();
                {deps_body}
                out
            }}

            fn access(
                snapshot: &::bowl::__derive::Snapshot,
                state: &Self::State,
            ) -> ::std::vec::Vec<::bowl::__derive::Access> {{
                let mut out = ::std::vec::Vec::new();
                {access_body}
                out
            }}

            fn fetch<'__item>(
                bowl: &::bowl::__derive::Bowl,
                snapshot: &'__item ::bowl::__derive::Snapshot,
                state: &Self::State,
                commands: &::bowl::__derive::Commands<::bowl::__derive::Anything>,
                guards: &mut ::bowl::__derive::GuardStore,
            ) -> Self::Item<'__item> {{
                {name} {{
                    {fetch_fields}
                }}
            }}

            fn always_run() -> bool {{
                {always_run_body}
            }}

            fn validate_local() -> ::std::result::Result<(), ::std::string::String> {{
                {validate_body}
                ::std::result::Result::Ok(())
            }}

            fn view_sets(out: &mut ::std::vec::Vec<::std::vec::Vec<::std::any::TypeId>>) {{
                {view_types_body}
            }}

            fn declared_outputs() -> ::core::option::Option<::std::vec::Vec<::std::any::TypeId>> {{
                let mut out = ::std::vec::Vec::new();
                {declared_outputs_body}
                ::core::option::Option::Some(out)
            }}
        }}"
    )
    .parse()
    .expect("generated system param impl should parse")
}

struct BundleField {
    name: String,
    ty: Vec<TokenTree>,
}

impl BundleField {
    /// The field type with the bundle's lifetime substituted, rendered as a
    /// type string.
    fn ty_with_lifetime(&self, lifetime: &Option<String>, replacement: &str) -> String {
        let Some(lifetime) = lifetime else {
            return render_tokens(&self.ty);
        };
        let substituted = substitute_lifetime(self.ty.clone(), lifetime, replacement);
        render_tokens(&substituted)
    }
}

struct Bundle {
    name: String,
    lifetime: Option<String>,
    fields: Vec<BundleField>,
}

fn parse_bundle(input: TokenStream) -> Result<Bundle, String> {
    let mut tokens = input.into_iter().peekable();
    let mut name = None;
    let mut lifetime = None;
    let mut body = None;

    while let Some(token) = tokens.next() {
        match token {
            TokenTree::Punct(punct) if punct.as_char() == '#' => {
                // Skip the attribute group.
                let _ = tokens.next();
            }
            TokenTree::Ident(ident) if ident.to_string() == "struct" => {
                let Some(TokenTree::Ident(ident)) = tokens.next() else {
                    return Err("SystemParam can only be derived for structs".to_string());
                };
                name = Some(ident.to_string());

                // Optional generics: at most one lifetime, no type params.
                if matches!(tokens.peek(), Some(TokenTree::Punct(p)) if p.as_char() == '<') {
                    let _ = tokens.next();
                    loop {
                        match tokens.next() {
                            Some(TokenTree::Punct(p)) if p.as_char() == '\'' => {
                                let Some(TokenTree::Ident(lt)) = tokens.next() else {
                                    return Err("malformed lifetime parameter".to_string());
                                };
                                if lifetime.is_some() {
                                    return Err(
                                        "SystemParam bundles support at most one lifetime"
                                            .to_string(),
                                    );
                                }
                                lifetime = Some(lt.to_string());
                            }
                            Some(TokenTree::Punct(p)) if p.as_char() == '>' => break,
                            Some(TokenTree::Punct(p)) if p.as_char() == ',' => {}
                            Some(_) => {
                                return Err(
                                    "SystemParam bundles support lifetimes only, no type \
                                     parameters"
                                        .to_string(),
                                );
                            }
                            None => return Err("unterminated generics".to_string()),
                        }
                    }
                }
            }
            TokenTree::Group(group) if group.delimiter() == Delimiter::Brace => {
                body = Some(group.stream());
            }
            _ => {}
        }
    }

    let name = name.ok_or_else(|| "SystemParam can only be derived for structs".to_string())?;
    let body = body.ok_or_else(|| "SystemParam requires named fields".to_string())?;
    let fields = parse_fields(body)?;
    if fields.is_empty() {
        return Err("SystemParam bundles need at least one field".to_string());
    }

    Ok(Bundle {
        name,
        lifetime,
        fields,
    })
}

fn parse_fields(body: TokenStream) -> Result<Vec<BundleField>, String> {
    let mut fields = Vec::new();
    let mut tokens = body.into_iter().peekable();

    loop {
        // Skip attributes and visibility.
        loop {
            match tokens.peek() {
                Some(TokenTree::Punct(p)) if p.as_char() == '#' => {
                    let _ = tokens.next();
                    let _ = tokens.next();
                }
                Some(TokenTree::Ident(ident)) if ident.to_string() == "pub" => {
                    let _ = tokens.next();
                    if matches!(
                        tokens.peek(),
                        Some(TokenTree::Group(g)) if g.delimiter() == Delimiter::Parenthesis
                    ) {
                        let _ = tokens.next();
                    }
                }
                _ => break,
            }
        }

        let Some(TokenTree::Ident(field_name)) = tokens.next() else {
            break;
        };
        match tokens.next() {
            Some(TokenTree::Punct(p)) if p.as_char() == ':' => {}
            _ => return Err(format!("expected `:` after field `{field_name}`")),
        }

        // Collect the type up to a comma at angle-bracket depth zero.
        let mut ty = Vec::new();
        let mut depth = 0i32;
        loop {
            match tokens.peek() {
                Some(TokenTree::Punct(p)) if p.as_char() == '<' => depth += 1,
                Some(TokenTree::Punct(p)) if p.as_char() == '>' => depth -= 1,
                Some(TokenTree::Punct(p)) if p.as_char() == ',' && depth == 0 => {
                    let _ = tokens.next();
                    break;
                }
                None => break,
                _ => {}
            }
            match tokens.next() {
                Some(token) => ty.push(token),
                None => break,
            }
        }

        if ty.is_empty() {
            return Err(format!("field `{field_name}` has an empty type"));
        }
        fields.push(BundleField {
            name: field_name.to_string(),
            ty,
        });
    }

    Ok(fields)
}

/// Replaces every occurrence of `'lifetime` in the token sequence with
/// `replacement` (a full lifetime like `'static`), recursing into groups.
fn substitute_lifetime(
    tokens: Vec<TokenTree>,
    lifetime: &str,
    replacement: &str,
) -> Vec<TokenTree> {
    let mut out = Vec::new();
    let mut iter = tokens.into_iter().peekable();

    while let Some(token) = iter.next() {
        match token {
            TokenTree::Punct(punct) if punct.as_char() == '\'' => {
                if matches!(
                    iter.peek(),
                    Some(TokenTree::Ident(ident)) if ident.to_string() == lifetime
                ) {
                    let _ = iter.next();
                    let replaced: TokenStream = replacement
                        .parse()
                        .expect("replacement lifetime should parse");
                    out.extend(replaced);
                } else {
                    out.push(TokenTree::Punct(punct));
                }
            }
            TokenTree::Group(group) => {
                let inner = substitute_lifetime(
                    group.stream().into_iter().collect(),
                    lifetime,
                    replacement,
                );
                let mut stream = TokenStream::new();
                stream.extend(inner);
                out.push(TokenTree::Group(proc_macro::Group::new(
                    group.delimiter(),
                    stream,
                )));
            }
            other => out.push(other),
        }
    }

    out
}

fn render_tokens(tokens: &[TokenTree]) -> String {
    let mut stream = TokenStream::new();
    stream.extend(tokens.iter().cloned());
    stream.to_string()
}
