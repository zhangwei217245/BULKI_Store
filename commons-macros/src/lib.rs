use proc_macro::TokenStream;
use quote::quote;
use syn::{parse::Parse, parse_macro_input, Expr, ImplItem, ItemImpl, Lit, Meta, Token, Type};

#[derive(Default)]
struct HandlerConfig {
    pattern: String,
    namespace: Option<String>,
}

impl Parse for HandlerConfig {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        let mut config = HandlerConfig::default();

        while !input.is_empty() {
            let meta: Meta = input.parse()?;
            match meta {
                Meta::NameValue(nv) if nv.path.is_ident("pattern") => {
                    if let Expr::Lit(expr_lit) = nv.value {
                        if let Lit::Str(lit) = expr_lit.lit {
                            config.pattern = lit.value();
                        }
                    }
                }
                Meta::NameValue(nv) if nv.path.is_ident("namespace") => {
                    if let Expr::Lit(expr_lit) = nv.value {
                        if let Lit::Str(lit) = expr_lit.lit {
                            config.namespace = Some(lit.value());
                        }
                    }
                }
                _ => return Err(syn::Error::new_spanned(meta, "unsupported attribute")),
            }

            if !input.is_empty() {
                input.parse::<Token![,]>()?;
            }
        }

        Ok(config)
    }
}

fn matches_handler_signature(method: &syn::ImplItemFn) -> bool {
    if method.sig.inputs.len() != 2 {
        return false;
    }

    // Check if first parameter is &self
    if let syn::FnArg::Receiver(receiver) = &method.sig.inputs[0] {
        if !receiver.reference.is_some() {
            return false;
        }
    } else {
        return false;
    }

    // Check if second parameter is &Vec<u8>
    let pat_type = match &method.sig.inputs[1] {
        syn::FnArg::Typed(pat_type) => pat_type,
        _ => return false,
    };

    let is_vec_param = matches!(
        &*pat_type.ty,
        syn::Type::Reference(type_ref) if matches!(
            &*type_ref.elem,
            syn::Type::Path(type_path) if type_path.path.segments.len() == 1
                && type_path.path.segments[0].ident == "Vec"
                && matches!(type_path.path.segments[0].arguments, syn::PathArguments::AngleBracketed(_))
        )
    );

    if !is_vec_param {
        return false;
    }

    // Check return type anyhow::Result<Vec<u8>, HandlerResult>
    let return_type = match &method.sig.output {
        syn::ReturnType::Type(_, return_type) => &**return_type,
        _ => return false,
    };

    let type_path = match return_type {
        syn::Type::Path(type_path) => type_path,
        _ => return false,
    };

    if type_path.path.segments.len() != 2
        || type_path.path.segments[0].ident != "anyhow"
        || type_path.path.segments[1].ident != "Result"
    {
        return false;
    }

    let args = match &type_path.path.segments[1].arguments {
        syn::PathArguments::AngleBracketed(args) if args.args.len() == 2 => &args.args,
        _ => return false,
    };

    let mut args_iter = args.iter();

    let vec_type = match args_iter.next() {
        Some(syn::GenericArgument::Type(syn::Type::Path(vec_type))) => vec_type,
        _ => return false,
    };

    if vec_type.path.segments.len() != 1
        || vec_type.path.segments[0].ident != "Vec"
        || !matches!(
            vec_type.path.segments[0].arguments,
            syn::PathArguments::AngleBracketed(_)
        )
    {
        return false;
    }

    let handler_type = match args_iter.next() {
        Some(syn::GenericArgument::Type(syn::Type::Path(handler_type))) => handler_type,
        _ => return false,
    };

    handler_type.path.segments.len() == 1 && handler_type.path.segments[0].ident == "HandlerResult"
}

#[proc_macro_attribute]
pub fn req_handler(attr: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as ItemImpl);
    let config = parse_macro_input!(attr as HandlerConfig);

    // Get the struct name
    let struct_name = if let Type::Path(type_path) = &input.self_ty.as_ref() {
        type_path
            .path
            .segments
            .last()
            .map(|seg| &seg.ident)
            .expect("No struct name found")
    } else {
        panic!("Expected struct impl")
    };

    // Find all handler methods
    let handler_methods: Vec<_> = input
        .items
        .iter()
        .filter_map(|item| {
            if let ImplItem::Fn(method) = item {
                if matches_handler_signature(method) {
                    Some(&method.sig.ident)
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect();

    // Generate registration code
    let pattern = &config.pattern;
    let registration = quote! {
        impl ::commons::handler::HandlerRegistration for #struct_name {
            fn register_handlers(&self, handler: &mut ::commons::handler::HandlerDispatcher<::commons::handler::RequestHandlerKind>) {
                const MODULE_PATH: &str = module_path!();
                const STRUCT_NAME: &str = stringify!(#struct_name);

                #(
                    let handler_name = format!(
                        #pattern,
                        module = MODULE_PATH,
                        struct_name = STRUCT_NAME,
                        method = stringify!(#handler_methods)
                    );

                    handler.register(
                        &handler_name,
                        |data| ::core::result::Result::Ok(self.#handler_methods(data))
                    );
                )*
            }
        }

        inventory::submit! {
            Box::new(#struct_name) as Box<dyn ::commons::handler::RequestHandlerRegistration>
        }
    };

    // Add registration to impl
    let mut output = input.clone();
    output.items.push(syn::parse2(registration).unwrap());

    quote!(#output).into()
}

#[proc_macro_attribute]
pub fn resp_handler(attr: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as ItemImpl);
    let config = parse_macro_input!(attr as HandlerConfig);

    // Get the struct name
    let struct_name = if let Type::Path(type_path) = &input.self_ty.as_ref() {
        type_path
            .path
            .segments
            .last()
            .map(|seg| &seg.ident)
            .expect("No struct name found")
    } else {
        panic!("Expected struct impl")
    };

    // Find all handler methods
    let handler_methods: Vec<_> = input
        .items
        .iter()
        .filter_map(|item| {
            if let ImplItem::Fn(method) = item {
                if matches_handler_signature(method) {
                    Some(&method.sig.ident)
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect();

    // Generate registration code
    let pattern = &config.pattern;
    let registration = quote! {
        impl ::commons::handler::ResponseHandlerRegistration for #struct_name {
            fn register_handlers(&self, handler: &mut ::commons::handler::HandlerDispatcher<::commons::handler::ResponseHandlerKind>) {
                const MODULE_PATH: &str = module_path!();
                const STRUCT_NAME: &str = stringify!(#struct_name);

                #(
                    let handler_name = format!(
                        #pattern,
                        module = MODULE_PATH,
                        struct_name = STRUCT_NAME,
                        method = stringify!(#handler_methods)
                    );

                    handler.register(
                        &handler_name,
                        |data| ::core::result::Result::Ok(self.#handler_methods(data))
                    );
                )*
            }
        }

        inventory::submit! {
            Box::new(#struct_name) as Box<dyn ::commons::handler::ResponseHandlerRegistration>
        }
    };

    // Add registration to impl
    let mut output = input.clone();
    output.items.push(syn::parse2(registration).unwrap());

    quote!(#output).into()
}
