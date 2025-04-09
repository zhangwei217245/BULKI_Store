use crate::handler::{RequestHandlerKind, ResponseHandlerKind};
use crate::rpc::{
    grpc::{GrpcRxEndpoint, GrpcTxEndpoint},
    tcp::{TcpRxEndpoint, TcpTxEndpoint},
    RPCImpl, RxContext, RxEndpoint, TxContext, TxEndpoint,
};
use anyhow::Result;

pub fn create_rx_endpoint(
    impl_type: RPCImpl,
    context: RxContext<String>,
) -> Result<Box<dyn RxEndpoint<Address = String>>> {
    match impl_type {
        RPCImpl::Grpc => Ok(Box::new(GrpcRxEndpoint::new(context))),
        RPCImpl::Tcp => {
            let tcp_context = RxContext::<std::net::SocketAddr> {
                rpc_id: context.rpc_id,
                rank: context.rank,
                size: context.size,
                world: context.world,
                server_addresses: None,
                address: None,
                handler: context.handler,
            };
            Ok(Box::new(TcpRxEndpoint::new(tcp_context)))
        }
    }
}

pub fn create_tx_endpoint(
    impl_type: RPCImpl,
    context: TxContext<String>,
) -> Result<Box<dyn TxEndpoint<Address = String>>> {
    match impl_type {
        RPCImpl::Grpc => Ok(Box::new(GrpcTxEndpoint::new(context))),
        RPCImpl::Tcp => {
            let tcp_context = TxContext::<std::net::SocketAddr> {
                rpc_id: context.rpc_id,
                rank: context.rank,
                size: context.size,
                world: context.world,
                server_addresses: None,
                handler: context.handler,
            };
            Ok(Box::new(TcpTxEndpoint::new(tcp_context)))
        }
    }
}
