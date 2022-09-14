#!/usr/bin/sh

cargo \
	tarpaulin \
	-o Html \
	--output-dir target/tarpaulin \
	--all-features \
	--rustflags="-C opt-level=0" \
	$@
