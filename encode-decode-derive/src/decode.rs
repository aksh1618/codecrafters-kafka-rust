use proc_macro2::TokenStream as TokenStream2;
use quote::{format_ident, quote};
use syn::{Attribute, Data, DeriveInput, Fields, Generics, Ident, Type};

pub fn impl_decode(ast: &DeriveInput) -> TokenStream2 {
    let type_name = &ast.ident;
    match ast.data {
        Data::Struct(ref data) => impl_decode_for_struct(type_name, &data.fields, &ast.generics),
        Data::Enum(ref _data) => impl_decode_for_enum(type_name, &ast.attrs),
        _ => unimplemented!(),
    }
}

fn impl_decode_for_struct(
    struct_name: &Ident,
    struct_fields: &Fields,
    struct_generics: &Generics,
) -> TokenStream2 {
    let mut fields_definition = vec![];
    let mut fields_in_declaration = vec![];
    for (i, field) in struct_fields.iter().enumerate() {
        let field_name = if let Some(ref field_name) = field.ident {
            field_name
        } else {
            &format_ident!("field{}", i)
        };
        fields_definition.push(quote! {
            let #field_name = crate::buf::Decode::decode(buf);
        });
        fields_in_declaration.push(quote! {
            #field_name
        })
    }
    let (impl_generics, ty_generics, where_clause) = struct_generics.split_for_impl();

    quote! {
        impl #impl_generics crate::buf::Decode for #struct_name #ty_generics #where_clause {
            fn decode<B: bytes::Buf + ?Sized>(mut buf: &mut B) -> Self {
                #(#fields_definition)*
                Self { #(#fields_in_declaration),* }
            }
        }
    }
}

fn impl_decode_for_enum(enum_name: &Ident, enum_attrs: &[Attribute]) -> TokenStream2 {
    let enum_repr = find_repr_type(enum_attrs);

    quote! {
        impl crate::buf::Decode for #enum_name {
            fn decode<B: bytes::Buf + ?Sized>(mut buf: &mut B) -> Self {
                let repr_value: #enum_repr = crate::buf::Decode::decode(buf);
                Self::from_repr(repr_value).unwrap_or_else(|| panic!("Unsupported #enum_name {repr_value}"))
            }
        }
    }
}

fn find_repr_type(attrs: &[Attribute]) -> Type {
    attrs
        .iter()
        .find(|attr| attr.path().is_ident("repr"))
        .map(|attr| {
            attr.parse_args::<Type>()
                .unwrap_or_else(|_| panic!("Failed to parse type from enum's repr attribute"))
        })
        .expect("Enum must have repr attribute")
}
