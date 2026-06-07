#!/usr/bin/env bash
# Build and push the multi-arch image (linux/arm64 for the Pi + linux/amd64).
# Usage: IMAGE=ghcr.io/alexrios/500mb-zig:latest ./scripts/build-image.sh
set -euo pipefail

IMAGE="${IMAGE:-ghcr.io/alexrios/500mb-zig:latest}"
PLATFORMS="${PLATFORMS:-linux/arm64,linux/amd64}"
PUSH="${PUSH:-1}"

cd "$(dirname "$0")/.."

builder="zig500"
docker buildx inspect "$builder" >/dev/null 2>&1 || docker buildx create --name "$builder" --use
docker buildx use "$builder"

args=(buildx build --platform "$PLATFORMS" -t "$IMAGE")
[ "$PUSH" = "1" ] && args+=(--push) || args+=(--load)

echo "Building $IMAGE for $PLATFORMS (push=$PUSH)"
docker "${args[@]}" .
echo "done: $IMAGE"
