use bytes::BufMut;

pub(crate) trait BufMutExt<F>
where
    Self: BufMut,
{
    fn put_api_response_payload(&mut self, api_spec: &F);
}

// pub(crate) trait BufExt<T> {
//     fn get_api_request_payload(&mut self) -> T;
// }
