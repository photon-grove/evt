//! Keeps the Rust walkthrough in `docs/guide/getting-started.md` honest. Every `rust` code block in
//! the guide must appear verbatim in `examples/getting_started.rs`, which CI compiles and tests, so
//! a snippet can't drift from code that builds.

use std::fs;
use std::path::Path;

#[test]
fn guide_rust_blocks_come_from_the_example() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let guide = fs::read_to_string(root.join("../../docs/guide/getting-started.md")).unwrap();
    let example = fs::read_to_string(root.join("examples/getting_started.rs")).unwrap();

    // A Rust block fenced any other way (`rust,ignore`, `rs`) would skip the check below.
    for line in guide.lines() {
        assert!(
            line == "```rust" || !line.starts_with("```rs"),
            "fence Rust blocks in the guide with exactly ```rust, found: {line}"
        );
    }

    let blocks = rust_blocks(&guide);
    assert_eq!(
        blocks.len(),
        12,
        "the guide's Rust block count changed; update this count after checking the new blocks"
    );

    for block in blocks {
        assert!(
            example.contains(&block),
            "this guide block is not in examples/getting_started.rs verbatim:\n{block}"
        );
    }
}

/// Returns the body of every fenced block opened with exactly ```` ```rust ````.
fn rust_blocks(markdown: &str) -> Vec<String> {
    let mut blocks = Vec::new();
    let mut current: Option<Vec<&str>> = None;

    for line in markdown.lines() {
        match current.as_mut() {
            None if line == "```rust" => current = Some(Vec::new()),
            None => {}
            Some(lines) if line == "```" => {
                blocks.push(lines.join("\n"));
                current = None;
            }
            Some(lines) => lines.push(line),
        }
    }

    blocks
}
