#!/bin/bash
#     Copyright 2025. ThingsBoard
#
#     Licensed under the Apache License, Version 2.0 (the "License");
#     you may not use this file except in compliance with the License.
#     You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#     Unless required by applicable law or agreed to in writing, software
#     distributed under the License is distributed on an "AS IS" BASIS,
#     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#     See the License for the specific language governing permissions and
#     limitations under the License.

set -e

###############################################################################
# docker_build_multiarch.sh
#
# Multi-Architecture Docker Build Script for ThingsBoard Gateway
#
# Supported operations:
#   1. Default: Build for current platform and load into local Docker
#   2. --push:  Build for one or more platforms and push to remote registry
#   3. --platform <list>: Manually specify platform(s) (e.g. linux/amd64,linux/arm64)
#                         If omitted, supported platforms are auto-detected.
#   4. --help:  Show this help
#
# Usage Examples:
#   ./docker_build_multiarch.sh
#       → Build and load locally for the current host platform
#
#   ./docker_build_multiarch.sh --push -r myregistry/tb-gateway
#       → Detect platforms automatically and push multi-arch image to registry
#
#   ./docker_build_multiarch.sh --push -r myregistry/tb-gateway --platform linux/amd64,linux/arm64
#       → Push multi-arch image to registry with manually specified platforms
#
# Notes:
#   -r or --repository is required when using --push
#   --platform is optional and overrides auto-detection when provided
###############################################################################

# ─── Configurable Defaults ────────────────────────────────────────────────
DOCKERFILE_PATH="docker/Dockerfile"
CONTEXT="."
TAG="latest"
DEFAULT_IMAGE="tb-gateway"
BUILDER_NAME="multiarch-builder"

# ─── Argument Parsing ─────────────────────────────────────────────────────
PUSH=false
REPOSITORY=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --platform)
            if [[ -z "$2" || "$2" == "--"* ]]; then
                echo "[X] Error: --platform requires a value (e.g. linux/amd64,linux/arm64)"
                exit 1
            fi
            PLATFORMS="$2"
            shift 2
            ;;
        --push)
            PUSH=true
            shift
            ;;
        -r|--repository)
            REPOSITORY="$2"
            shift 2
            ;;
        -h|--help)
            echo "Usage: ./docker_build_multiarch.sh [OPTIONS]"
            echo ""
            echo "Build ThingsBoard Gateway Docker image for the current platform (default) or for multiple platforms with push."
            echo ""
            echo "Options:"
            echo "  --push                Push the image to a registry (requires -r)"
            echo "  -r, --repository REPO Target repository (e.g. myrepo/tb-gateway)"
            echo "  --platform PLATFORMS  Manually set platforms (comma-separated)"
            echo "  -h, --help            Show this help message"
            echo ""
            echo "Examples:"
            echo "  ./docker_build_multiarch.sh"
            echo "  ./docker_build_multiarch.sh --push -r myrepo/tb-gateway"
            echo "  ./docker_build_multiarch.sh --push -r myrepo/tb-gateway --platform linux/amd64,linux/arm64"
            exit 0
            ;;
        *)
            echo "[X] Unknown option: $1"
            exit 1
            ;;
    esac
done

# ─── Validation ───────────────────────────────────────────────────────────
if [[ "$PUSH" == true && -z "$REPOSITORY" ]]; then
    echo "[X] Error: --push requires -r <repository> to be set."
    exit 1
fi

IMAGE_TAG="${REPOSITORY:-$DEFAULT_IMAGE}:${TAG}"

# ─── Setup Buildx ─────────────────────────────────────────────────────────
if ! docker buildx inspect "$BUILDER_NAME" &>/dev/null; then
    echo "[*] Creating buildx builder: $BUILDER_NAME..."
    docker buildx create --name "$BUILDER_NAME" --use
else
    echo "[*] Using buildx builder: $BUILDER_NAME"
    docker buildx use "$BUILDER_NAME"
fi

# ─── Register QEMU for Cross-Platform Support ─────────────────────────────
echo "[*] Registering QEMU emulation..."
docker run --rm --privileged tonistiigi/binfmt --install all

# ─── Detect Supported Platforms If Not Set ─────────────────────────────────
if [[ -z "$PLATFORMS" ]]; then
    echo "[*] Parsing base image from Dockerfile..."
    BASE_IMAGE=$(awk '/^FROM/ { for (i=1; i<=NF; i++) if ($i !~ /FROM|--platform=|\$TARGETPLATFORM|AS/) print $i }' "$DOCKERFILE_PATH" | head -n1)

    if [[ -z "$BASE_IMAGE" ]]; then
        echo "[!] Failed to detect base image from Dockerfile: $DOCKERFILE_PATH"
        exit 1
    fi

    echo "[*] Detected base image: $BASE_IMAGE"
    echo "[*] Checking supported platforms for base image..."

    PLATFORMS=$(docker buildx imagetools inspect "$BASE_IMAGE" 2>/dev/null \
        | awk '/Platform:/ { print $2 }' \
        | grep -v '^unknown/unknown$' \
        | sort -u \
        | paste -sd, -)

    if [[ -z "$PLATFORMS" ]]; then
        echo "[!] Failed to detect platforms from base image. Falling back to default safe platforms."
        PLATFORMS="linux/amd64,linux/arm64,linux/arm/v7,linux/386"
    fi

    echo "[*] Supported platforms: $PLATFORMS"
else
    echo "[*] Using manually defined platforms: $PLATFORMS"
fi

# ─── Build Execution ──────────────────────────────────────────────────────
if [[ -z "$PLATFORMS" ]]; then
    echo "[X] Internal error: PLATFORMS is empty before build"
    exit 1
fi

if [[ "$PUSH" == true ]]; then
    echo "[*] Building and pushing multi-arch image to: ${IMAGE_TAG}"
    docker buildx build \
        --platform "$PLATFORMS" \
        --file "$DOCKERFILE_PATH" \
        --tag "$IMAGE_TAG" \
        --build-arg BUILDKIT_INLINE_CACHE=1 \
        --push "$CONTEXT"
else
    CURRENT_PLATFORM="$(docker version -f '{{.Server.Os}}')/$(docker version -f '{{.Server.Arch}}')"
    echo "[*] Building and loading image for current platform: $CURRENT_PLATFORM"
    PLATFORMS="$CURRENT_PLATFORM"
    docker buildx build \
        --platform "$PLATFORMS" \
        --file "$DOCKERFILE_PATH" \
        --tag "$IMAGE_TAG" \
        --build-arg BUILDKIT_INLINE_CACHE=1 \
        --load "$CONTEXT"
fi

# ─── Final Output ─────────────────────────────────────────────────────────
echo "[✓] Build completed!"
echo "    Image Tag : $IMAGE_TAG"
echo "    Platforms : $PLATFORMS"
if [[ "$PUSH" == true ]]; then
    echo "    Action    : pushed to registry"
else
    echo "    Action    : built and loaded into local Docker for current platform"
fi
