pub mod bulki_store {
    use crate::common::RPCData;

    #[derive(Default)]
    pub struct BulkiStore {
        // Add any store-specific fields here
    }

    pub trait Dispatchable {
        fn dispatch(&self, func_name: &str, data: &Vec<u8>) -> Option<Vec<u8>>;
    }

    impl BulkiStore {
        pub fn new() -> Self {
            Self::default()
        }

        pub fn times_two(&self, data: &Vec<u8>) -> Vec<u8> {
            data.iter().map(|x| x * 2).collect()
        }

        pub fn times_three(&self, data: &Vec<u8>) -> Vec<u8> {
            data.iter().map(|x| x * 3).collect()
        }
    }

    impl Dispatchable for BulkiStore {
        fn dispatch(&self, func_name: &str, data: &Vec<u8>) -> Option<Vec<u8>> {
            match func_name {
                "times_two" => Some(self.times_two(data)),
                "times_three" => Some(self.times_three(data)),
                _ => None,
            }
        }
    }
}
