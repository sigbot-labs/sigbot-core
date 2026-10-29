//! Request path helpers; authentication and identity are request scoped.
pub struct RequestPaths;

impl RequestPaths {
    pub fn clean_context_path<'a>(context_path: &Option<String>, path: &'a str) -> &'a str {
        match context_path.as_deref() {
            Some(prefix) if prefix != "/" => path.strip_prefix(prefix).unwrap_or(path),
            _ => path,
        }
    }
}
