# Self-contained, multi-arch build. The build stage runs natively on the
# builder (BUILDPLATFORM) and cross-compiles a fully static musl binary for the
# target arch — fast and reproducible. The runtime image is `scratch`: just the
# ~260 KB binary, nothing else. No shell, no libc, minimal attack surface, and
# the smallest possible image / RSS contribution.

# --- build -------------------------------------------------------------------
FROM --platform=$BUILDPLATFORM alpine:3.20 AS build

ARG ZIG_VERSION=0.16.0
ARG BUILDARCH
ARG TARGETARCH

RUN apk add --no-cache curl xz

# Fetch the Zig toolchain for the *builder* architecture.
RUN set -eux; \
    case "$BUILDARCH" in \
      amd64) ZA=x86_64 ;; \
      arm64) ZA=aarch64 ;; \
      *) echo "unsupported BUILDARCH=$BUILDARCH" >&2; exit 1 ;; \
    esac; \
    curl -fsSL "https://ziglang.org/download/${ZIG_VERSION}/zig-${ZA}-linux-${ZIG_VERSION}.tar.xz" -o /tmp/zig.tar.xz; \
    mkdir -p /opt/zig; \
    tar -xJf /tmp/zig.tar.xz -C /opt/zig --strip-components=1; \
    rm /tmp/zig.tar.xz
ENV PATH="/opt/zig:${PATH}"

WORKDIR /src
COPY build.zig build.zig.zon ./
COPY src ./src

# Cross-compile a static musl binary for the target architecture.
RUN set -eux; \
    case "$TARGETARCH" in \
      amd64) ZT=x86_64-linux-musl ;; \
      arm64) ZT=aarch64-linux-musl ;; \
      *) echo "unsupported TARGETARCH=$TARGETARCH" >&2; exit 1 ;; \
    esac; \
    zig build -Dtarget="$ZT" --release=fast; \
    /opt/zig/zig version

# --- runtime -----------------------------------------------------------------
FROM scratch
COPY --from=build /src/zig-out/bin/telemetry /telemetry
EXPOSE 8000
# Runs as the user set in docker-compose (non-root). No shell available.
ENTRYPOINT ["/telemetry"]
