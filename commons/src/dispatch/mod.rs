pub mod dispatch {

    pub trait Dispatchable {
        fn dispatch(&self, func_name: &str, data: &Vec<u8>) -> Option<Vec<u8>>;
    }
}
