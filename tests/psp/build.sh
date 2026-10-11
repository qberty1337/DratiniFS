#!/bin/sh
# Build tests/psp/EBOOT.PBP in the pspdev/pspdev container (from Git Bash or any shell with docker).
cd "$(dirname "$0")" || exit 1
here=$(cd .. && (pwd -W 2>/dev/null || pwd))
MSYS_NO_PATHCONV=1 docker run --rm -v "$here:/t" -w /t/psp pspdev/pspdev sh -c "make clean >/dev/null && make"
