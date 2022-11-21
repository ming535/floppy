#!/usr/bin/env bash
if [ "$(uname)" == "Darwin" ]; then
    RUSTFLAGS="-Z sanitizer=thread" RUSTDOCFLAGS="-Z sanitizer=thread" RUST_TEST_THREADS=1 cargo +nightly test -Z build-std --target x86_64-apple-darwin
else
    RUSTFLAGS="-Z sanitizer=thread" RUSTDOCFLAGS="-Z sanitizer=thread" RUST_TEST_THREADS=1 cargo +nightly test -Z build-std --target x86_64-unknown-linux-gnu
fi
