use prometheus_client::encoding::text::Encode;

#[derive(Clone, Hash, PartialEq, Eq, Encode)]
pub struct HttpLabels {
    pub method: HttpMethod,
    pub status: HttpStatus,
    pub success: Success,
    pub type_id: u32,
    pub writer_id: u32,
}

#[derive(Clone, Hash, PartialEq, Eq, Encode)]
pub enum HttpMethod {
    GET,
    POST,
}

#[derive(Clone, Hash, PartialEq, Eq, Encode)]
pub enum HttpStatus {
    Status2xx,
    Status3xx,
    Status4xx,
    Status5xx,
}
#[derive(Clone, Hash, PartialEq, Eq, Encode)]
pub enum Success {
    Yes,
    No,
}
