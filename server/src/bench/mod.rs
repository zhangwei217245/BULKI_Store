use commons::err::StatusCode;
use commons::handler::HandlerResult;
use commons::rpc::RPCData;
use rayon::prelude::*;

pub fn times_two(data: &mut RPCData) -> HandlerResult {
    println!("times_two");
    // Modify data in place
    data.data = data
        .data
        .as_ref()
        .map(|x| x.par_iter().map(|x| x * 2).collect());
    // Return success status
    HandlerResult {
        status_code: StatusCode::Ok as u8,
        message: None,
    }
}

pub fn times_three(data: &mut RPCData) -> HandlerResult {
    println!("times_three");
    // Modify data in place
    data.data = data
        .data
        .as_ref()
        .map(|x| x.par_iter().map(|x| x * 3).collect());
    // Return success status
    HandlerResult {
        status_code: StatusCode::Ok as u8,
        message: None,
    }
}
