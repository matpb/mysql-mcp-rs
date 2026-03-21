//! MySQL MCP server library — read-only MySQL access for the Model Context Protocol.
//!
//! The binary entrypoint is thin; use [`run`] to start the HTTP server programmatically.

pub mod config;
pub mod db;
pub mod mcp;
pub mod sanitizer;

mod server;

pub use server::run;
