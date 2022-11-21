#!/usr/bin/env bash
if [ "$(uname)" == "Darwin" ]; then
    ASAN_OPTIONS=detect_leaks=1 RUSTFLAGS=-Zsanitizer=address RUSTDOCFLAGS=-Zsanitizer=address cargo +nightly test -Zbuild-std --target x86_64-apple-darwin
else
    RUSTFLAGS=-Zsanitizer=address RUSTDOCFLAGS=-Zsanitizer=address cargo +nightly test -Zbuild-std --target x86_64-unknown-linux-gnu
fi
