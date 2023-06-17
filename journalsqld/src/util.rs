pub fn measure<T, F: FnOnce() -> T>(f: F) -> (std::time::Duration, T) {
    let start = std::time::Instant::now();
    let res = f();
    (start.elapsed(), res)
}
