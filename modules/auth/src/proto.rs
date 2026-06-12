//! Generated protobuf and gRPC types.
//!
//! The submodules here are produced at build time from the `.proto` contracts
//! under `modules/auth/proto`. Use the aliased imports described in the project
//! guidelines when a generated `XxxRequest` collides with a service DTO.

pub mod preauth {
    #![allow(clippy::all)]
    #![allow(clippy::pedantic)]
    tonic::include_proto!("isla.auth.preauth");
}
