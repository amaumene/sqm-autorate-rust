FROM rust:alpine AS builder

ARG TARGETARCH

#RUN apk add --no-cache \
#    clang \
#    llvm \
#    elfutils-dev \
#    libbpf-dev \
#    linux-headers \
#    musl-dev

WORKDIR /build

COPY . .

RUN cargo update

RUN cargo build --release --no-default-features 

RUN ls -lahR .

FROM scratch
COPY --from=builder /build/target/release/sqm-autorate-rust /sqm-autorate-rust
