use std::collections::HashMap;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::RwLock;

#[derive(Error, Debug)]
pub enum DispatchError {
    #[error("Module not found: {0}")]
    ModuleNotFound(String),
    #[error("Function not found: {0}")]
    FunctionNotFound(String),
    #[error("Invalid function path: {0}")]
    InvalidFunctionPath(String),
    #[error("Execution error: {0}")]
    ExecutionError(String),
}

pub type DispatchResult<T> = Result<T, DispatchError>;
pub type HandlerFn = Arc<dyn Fn(Vec<u8>) -> DispatchResult<Vec<u8>> + Send + Sync>;

// Registry to store module and function handlers
#[derive(Default)]
pub struct Registry {
    modules: HashMap<String, HashMap<String, HandlerFn>>,
}

pub struct Dispatcher {
    registry: Arc<RwLock<Registry>>,
}

impl Dispatcher {
    pub fn new() -> Self {
        Self {
            registry: Arc::new(RwLock::new(Registry::default())),
        }
    }

    pub async fn register_handler(&self, module: &str, func: &str, handler: HandlerFn) {
        let mut registry = self.registry.write().await;
        let module_handlers = registry.modules.entry(module.to_string()).or_default();
        module_handlers.insert(func.to_string(), handler);
    }

    pub async fn dispatch(&self, func_path: &str, payload: Vec<u8>) -> DispatchResult<Vec<u8>> {
        // Split the function path into module and function names
        let parts: Vec<&str> = func_path.split('.').collect();
        if parts.len() != 2 {
            return Err(DispatchError::InvalidFunctionPath(func_path.to_string()));
        }

        let (module_name, func_name) = (parts[0], parts[1]);
        let registry = self.registry.read().await;

        // Get the module
        let module = registry
            .modules
            .get(module_name)
            .ok_or_else(|| DispatchError::ModuleNotFound(module_name.to_string()))?;

        // Get the function handler
        let handler = module
            .get(func_name)
            .ok_or_else(|| DispatchError::FunctionNotFound(func_name.to_string()))?;

        // Execute the handler
        handler(payload)
    }

    pub async fn unregister_handler(&self, module: &str, func: &str) {
        let mut registry = self.registry.write().await;
        if let Some(module_handlers) = registry.modules.get_mut(module) {
            module_handlers.remove(func);
            // Remove the module if it's empty
            if module_handlers.is_empty() {
                registry.modules.remove(module);
            }
        }
    }

    pub async fn list_handlers(&self) -> Vec<String> {
        let registry = self.registry.read().await;
        let mut handlers = Vec::new();
        for (module, funcs) in &registry.modules {
            for func in funcs.keys() {
                handlers.push(format!("{}.{}", module, func));
            }
        }
        handlers.sort();
        handlers
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};
    use std::sync::atomic::{AtomicU32, Ordering};

    // Test structures for serialization/deserialization
    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    struct MathRequest {
        a: i32,
        b: i32,
    }

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    struct MathResponse {
        result: i32,
    }

    #[tokio::test]
    async fn test_basic_dispatch() {
        let dispatcher = Dispatcher::new();

        // Register a simple echo handler
        dispatcher
            .register_handler("test", "echo", Arc::new(|payload| Ok(payload)))
            .await;

        let test_data = vec![1, 2, 3, 4, 5];
        let result = dispatcher
            .dispatch("test.echo", test_data.clone())
            .await
            .unwrap();
        assert_eq!(result, test_data);
    }

    #[tokio::test]
    async fn test_math_operations() {
        let dispatcher = Dispatcher::new();

        // Register add handler
        dispatcher
            .register_handler(
                "math",
                "add",
                Arc::new(|payload| {
                    let req: MathRequest = rmp_serde::from_slice(&payload)
                        .map_err(|e| DispatchError::ExecutionError(e.to_string()))?;
                    let resp = MathResponse {
                        result: req.a + req.b,
                    };
                    rmp_serde::to_vec(&resp)
                        .map_err(|e| DispatchError::ExecutionError(e.to_string()))
                }),
            )
            .await;

        // Test addition
        let req = MathRequest { a: 5, b: 3 };
        let payload = rmp_serde::to_vec(&req).unwrap();
        let result = dispatcher.dispatch("math.add", payload).await.unwrap();
        let response: MathResponse = rmp_serde::from_slice(&result).unwrap();
        assert_eq!(response.result, 8);
    }

    #[tokio::test]
    async fn test_concurrent_dispatch() {
        let dispatcher = Arc::new(Dispatcher::new());
        let counter = Arc::new(AtomicU32::new(0));

        // Register a handler that increments a counter
        let counter_clone = counter.clone();
        dispatcher
            .register_handler(
                "counter",
                "increment",
                Arc::new(move |_| {
                    let value = counter_clone.fetch_add(1, Ordering::SeqCst);
                    Ok(value.to_be_bytes().to_vec())
                }),
            )
            .await;

        // Spawn multiple tasks to call the handler concurrently
        let mut handles = vec![];
        for _ in 0..10 {
            let dispatcher = dispatcher.clone();
            let handle = tokio::spawn(async move {
                dispatcher
                    .dispatch("counter.increment", vec![])
                    .await
                    .unwrap()
            });
            handles.push(handle);
        }

        // Wait for all tasks to complete
        for handle in handles {
            handle.await.unwrap();
        }

        assert_eq!(counter.load(Ordering::SeqCst), 10);
    }

    #[tokio::test]
    async fn test_handler_management() {
        let dispatcher = Dispatcher::new();

        // Test registering handlers
        dispatcher
            .register_handler("module1", "func1", Arc::new(|p| Ok(p)))
            .await;
        dispatcher
            .register_handler("module1", "func2", Arc::new(|p| Ok(p)))
            .await;
        dispatcher
            .register_handler("module2", "func1", Arc::new(|p| Ok(p)))
            .await;

        // Test listing handlers
        let handlers = dispatcher.list_handlers().await;
        assert_eq!(handlers.len(), 3);
        assert!(handlers.contains(&"module1.func1".to_string()));
        assert!(handlers.contains(&"module1.func2".to_string()));
        assert!(handlers.contains(&"module2.func1".to_string()));

        // Test unregistering handlers
        dispatcher.unregister_handler("module1", "func1").await;
        let handlers = dispatcher.list_handlers().await;
        assert_eq!(handlers.len(), 2);
        assert!(!handlers.contains(&"module1.func1".to_string()));
    }

    #[tokio::test]
    async fn test_error_cases() {
        let dispatcher = Dispatcher::new();

        // Test invalid function path
        let result = dispatcher.dispatch("invalid_path", vec![]).await;
        assert!(matches!(result, Err(DispatchError::InvalidFunctionPath(_))));

        // Test non-existent module
        let result = dispatcher.dispatch("nonexistent.func", vec![]).await;
        assert!(matches!(result, Err(DispatchError::ModuleNotFound(_))));

        // Register a handler that always fails
        dispatcher
            .register_handler(
                "test",
                "fail",
                Arc::new(|_| Err(DispatchError::ExecutionError("Intentional failure".into()))),
            )
            .await;

        // Test handler execution failure
        let result = dispatcher.dispatch("test.fail", vec![]).await;
        assert!(matches!(result, Err(DispatchError::ExecutionError(_))));
    }
}

#[tokio::main]
async fn main() {
    let dispatcher = Dispatcher::new();

    // Register a handler
    dispatcher
        .register_handler(
            "math",
            "add",
            Arc::new(|payload| {
                // In real code, you'd deserialize the payload
                Ok(payload)
            }),
        )
        .await;

    // Dispatch a call
    match dispatcher.dispatch("math.add", vec![1, 2, 3]).await {
        Ok(result) => println!("Result: {:?}", result),
        Err(e) => eprintln!("Error: {}", e),
    }
}
