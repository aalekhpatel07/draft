#!/usr/bin/sh

cargo \
	tarpaulin \
	-o Html \
	--output-dir /var/www/html/draft-tarpaulin \
	--all-features \
	--rustflags="-C opt-level=0"
