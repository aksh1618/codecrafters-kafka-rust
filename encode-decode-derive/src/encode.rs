use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::{Attribute, Data, DeriveInput, Fields, Generics, Ident, Index, Type};

pub fn impl_encode(ast: &DeriveInput) -> TokenStream2 {
    let type_name = &ast.ident;
    match ast.data {
        Data::Struct(ref data) => impl_encode_for_struct(type_name, &data.fields, &ast.generics),
        Data::Enum(ref _data) => impl_encode_for_enum(type_name, &ast.attrs),
        _ => unimplemented!(),
    }
}

fn impl_encode_for_struct(
    struct_name: &Ident,
    struct_fields: &Fields,
    struct_generics: &Generics,
) -> TokenStream2 {
    let encode_fields = struct_fields.iter().enumerate().map(|(i, field)| {
        if let Some(ref field_name) = field.ident {
            quote! {
                crate::buf::Encode::encode(&self.#field_name, buf);
            }
        } else {
            let index = Index::from(i);
            quote! {
                crate::buf::Encode::encode(&self.#index, buf);
            }
        }
    });
    let (impl_generics, ty_generics, where_clause) = struct_generics.split_for_impl();

    quote! {
        impl #impl_generics crate::buf::Encode for #struct_name #ty_generics #where_clause {
            fn encode<T: bytes::BufMut + ?Sized>(&self, mut buf: &mut T) {
                #(#encode_fields)*
            }
        }
    }
}

fn impl_encode_for_enum(enum_name: &Ident, enum_attrs: &[Attribute]) -> TokenStream2 {
    let enum_repr = find_repr_type(enum_attrs);

    quote! {
        impl crate::buf::Encode for #enum_name {
            fn encode<T: bytes::BufMut + ?Sized>(&self, mut buf: &mut T) {
                crate::buf::Encode::encode(&(*self as #enum_repr), buf);
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
