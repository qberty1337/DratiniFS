#!/bin/sh
# Run the host tests in a 32-bit Linux container (the driver is built ILP32 so its structs keep
# Sony's exact layout), from Git Bash / any shell with docker:   sh tests/run_tests.sh [--regolden]
# The image tag is pinned: the golden images depend on mkfs.fat's exact output. --privileged is for
# the exFAT oracle (fuse-exfat on a loop device), the only independent exFAT implementation here.
cd "$(dirname "$0")/.." || exit 1
here=$(pwd -W 2>/dev/null || pwd)
MSYS_NO_PATHCONV=1 docker run --rm --privileged --platform linux/386 -e FST_FSCK_EACH -v "$here:/src" -w /src/tests alpine:3.20 \
    sh -c "apk add --no-cache gcc musl-dev dosfstools mtools exfatprogs fuse-exfat >/dev/null && sh run_host.sh $*"
