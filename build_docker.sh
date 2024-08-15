#!/bin/sh
#     Copyright 2024. ThingsBoard
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

set -e # exit on any error

# Fetch the current branch name and latest commit ID
BRANCH_NAME=$(git rev-parse --abbrev-ref HEAD | sed 's/[\/]/-/g')
COMMIT_ID=$(git rev-parse --short HEAD)

# Combine them to create a version tag
VERSION_TAG="${BRANCH_NAME}-${COMMIT_ID}"

echo "$(date) Building project with version tag $VERSION_TAG ..."
set -x

#docker build . -t sevlamat/tb-gateway:$VERSION_TAG -f docker/Dockerfile -o type=registry

# multi arch
 DOCKER_CLI_EXPERIMENTAL=enabled \
 docker buildx build . -t sevlamat/tb-gateway:$VERSION_TAG -f docker/Dockerfile --platform=linux/amd64,linux/arm64 -o type=registry

set +x
echo "$(date) Done."
