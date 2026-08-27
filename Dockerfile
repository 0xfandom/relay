# One builder, three runtimes.
#
# The three binaries share a workspace and therefore share almost all of their
# compilation. A Dockerfile per binary would compile `relay-store` three times and
# produce three copies of the same dependency tree; here the builder stage is built
# once and all three runtime stages copy out of that single cached layer.
#
# syntax=docker/dockerfile:1

# ------------------------------------------------------------------- build
FROM rust:1.95-slim-trixie AS builder

# Needed to link against OpenSSL-free TLS and to compile the `ring` backend rustls
# uses. `pkg-config` and a C toolchain are build-time only and do not follow the
# binary into the runtime image.
RUN apt-get update \
    && apt-get install -y --no-install-recommends pkg-config libssl-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build
COPY . .

# Cache mounts rather than the usual "copy the manifests, build a dummy main, copy
# the real source" dance. That trick exists to keep dependency compilation in its own
# layer, and it is fragile with six crates: every new crate has to be added to the
# manifest-copy list or it silently stops being cached. A cache mount keeps the same
# compiled dependencies across builds without the bookkeeping.
#
# The binaries are copied out of the cache mount at the end of the same RUN, because
# a cache mount is not part of the image: anything left in `target/` disappears when
# the step finishes.
RUN --mount=type=cache,target=/build/target,sharing=locked \
    --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    cargo build --release --workspace \
    && mkdir -p /out \
    && cp target/release/relay-api target/release/relay-dispatcher target/release/relay-testkit /out/

# ------------------------------------------------------------------- runtime
# A shared base so the three images differ only in which binary they carry.
FROM debian:trixie-slim AS runtime

# `ca-certificates` because Relay's whole job is making outbound HTTPS requests and
# rustls is linked against the platform root store — without it the image starts
# perfectly and then fails every delivery with a certificate error.
#
# `curl` because a container healthcheck has to run *inside* the container, and a
# slim image has no HTTP client at all. It is a real trade: curl is exactly the tool
# an attacker who got execution here would want. Kept because the alternative is
# either no healthcheck — losing the ordering that makes `docker compose up` work in
# one command — or teaching each binary to probe itself, which puts an HTTP client in
# the API purely so that it can call itself.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*

# Not root. Relay resolves and connects to addresses that customers control, which
# makes it the most attractive process in the deployment to escape from.
RUN useradd --system --uid 10001 --no-create-home relay
USER 10001:10001

# ------------------------------------------------------------------- api
FROM runtime AS api
COPY --from=builder /out/relay-api /usr/local/bin/relay-api
EXPOSE 8080
# Exec form, so the binary is PID 1 and receives SIGTERM directly. The shell form
# would put `/bin/sh` at PID 1, which does not forward signals — every shutdown
# would be a SIGKILL after the grace period, cutting off in-flight work.
ENTRYPOINT ["/usr/local/bin/relay-api"]

# ------------------------------------------------------------------- dispatcher
FROM runtime AS dispatcher
COPY --from=builder /out/relay-dispatcher /usr/local/bin/relay-dispatcher
# Metrics only. The dispatcher makes outbound requests and serves nothing else.
EXPOSE 9091
ENTRYPOINT ["/usr/local/bin/relay-dispatcher"]

# ------------------------------------------------------------------- testkit
FROM runtime AS testkit
COPY --from=builder /out/relay-testkit /usr/local/bin/relay-testkit
EXPOSE 9099
ENTRYPOINT ["/usr/local/bin/relay-testkit"]
