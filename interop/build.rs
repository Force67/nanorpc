//! Generates Rust bindings for the interop schema at build time, exactly
//! like nanobuf's own conformance crate.

use std::path::Path;

use nanobuf_diagnostics::{Diagnostics, SourceMap};
use nanobuf_schema::Unit;

fn main() {
    println!("cargo::rerun-if-changed=schema");

    let text = std::fs::read_to_string("schema/echo.nb").expect("schema/echo.nb");
    let mut sources = SourceMap::new();
    let mut diags = Diagnostics::new();
    let file = sources.add("schema/echo.nb", text.clone());
    let ast = nanobuf_syntax::parse(file, &text, &mut diags)
        .unwrap_or_else(|| panic!("{}", diags.render(&sources, false)));
    let schema = nanobuf_schema::analyze(&[Unit { file, ast }], &mut diags)
        .unwrap_or_else(|| panic!("{}", diags.render(&sources, false)));

    let files = nanobuf_codegen::generate(&schema, nanobuf_codegen::Lang::Rust, "echo")
        .expect("rust generation succeeds");
    let out_dir = std::env::var("OUT_DIR").unwrap();
    for file in files {
        std::fs::write(Path::new(&out_dir).join(file.name), file.content).unwrap();
    }
}
