FROM rust

RUN mkdir /datafusion-benchmarks
RUN mkdir /datafusion-benchmarks/src
WORKDIR /datafusion-benchmarks

COPY Cargo.toml .
COPY rust-toolchain .
COPY src ./src

RUN cargo build --release