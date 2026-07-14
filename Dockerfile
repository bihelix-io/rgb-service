FROM 905418016605.dkr.ecr.ap-southeast-1.amazonaws.com/rust:bookworm AS builder
WORKDIR /build
COPY . .
RUN set -eux; \
    cargo --config net.git-fetch-with-cli=true build --release

# runner
FROM 905418016605.dkr.ecr.ap-southeast-1.amazonaws.com/debian:bookworm-slim AS runtime
ENV TINI_VERSION="v0.19.0"
ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini /sbin/tini

RUN set -eux; \
    chmod +x /sbin/tini; \
    apt-get update; \
    apt-get install -y --no-install-recommends curl net-tools procps ca-certificates; \
    update-ca-certificates; \
    apt-get clean && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=builder /build/target/release/rgb-service /app/

EXPOSE 80
CMD ["/sbin/tini", "--"]
