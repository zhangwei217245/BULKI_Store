pub mod aop {
    use std::time::Instant;

    macro_rules! timed_block {
        ($name:expr, $block:block) => {{
            let start = Instant::now();
            let result = { $block };
            let duration = start.elapsed();
            println!(
                "[{}] executed in {:.3}ms ({} ns)",
                $name,
                duration.as_secs_f64() * 1000.0,
                duration.as_nanos()
            );
            result
        }};
        ($block:block) => {{
            timed_block!("Anonymous Block", $block)
        }};
    }

    pub fn with_timing<F, R>(name: &str, func: F) -> R
    where
        F: FnOnce() -> R, // Accepts any callable that can be invoked once
    {
        timed_block!(name, { func() })
    }

    // Overload for anonymous blocks
    pub fn with_timing_anon<F, R>(func: F) -> R
    where
        F: FnOnce() -> R,
    {
        with_timing("Anonymous Function", func)
    }
}
