#[macro_export]
macro_rules! trace {
    ($($arg:tt)+) => {
        ()
    };
}
#[macro_export]
macro_rules! debug {
    ($($arg:tt)+) => {
        ()
    };
}
#[macro_export]
macro_rules! info {
    ($($arg:tt)+) => {
        ()
    };
}

#[macro_export]
macro_rules! _warn {
    ($($arg:tt)+) => {
        ()
    };
}
#[macro_export]
macro_rules! error {
    ($($arg:tt)+) => {
        ()
    };
}
#[macro_export]
macro_rules! fatal {
    ($($arg:tt)+) => {
        ()
    };
}
#[macro_export]
macro_rules! log_enabled {
    ($lvl:expr) => {
        false
    };
}
pub use {_warn as warn, debug, error, fatal, info, trace};

use std::io::Write;
pub fn init(path: &str, _l: &str) -> std::io::Result<()> {
    std::fs::create_dir_all(path)?;
    let mut log = std::fs::File::create(format!("{}/breeze.log", path))?;
    log.write(b"===> log disabled <===")?;
    Ok(())
}
