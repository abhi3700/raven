#!/usr/bin/env bash

RUSTFLAGS="-Awarnings" cargo nextest run --workspace --all-targets