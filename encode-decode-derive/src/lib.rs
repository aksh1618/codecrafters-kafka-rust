use proc_macro::TokenStream;

mod decode;
mod encode;

#[proc_macro_derive(Encode)]
pub fn derive_encode(input: TokenStream) -> TokenStream {
    let ast = syn::parse(input).unwrap();
    encode::impl_encode(&ast).into()
}

#[proc_macro_derive(Decode)]
pub fn derive_decode(input: TokenStream) -> TokenStream {
    let ast = syn::parse(input).unwrap();
    decode::impl_decode(&ast).into()
}
