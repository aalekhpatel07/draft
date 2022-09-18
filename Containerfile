FROM rust:1.63

WORKDIR /app

RUN apt-get update -y && \
    apt-get upgrade -y

RUN rustup update && \
    rustup component add clippy && \
    rustup component add rustfmt

RUN cargo install cargo-nextest cargo-tarpaulin

COPY Cargo.toml .
COPY Cargo.lock .

COPY draft-core draft-core
COPY draft-server draft-server

RUN cargo nextest run --release --all-features
RUN cargo build --release

CMD ["/bin/sleep", "infinity"]