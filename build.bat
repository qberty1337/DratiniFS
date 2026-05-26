@echo off
echo Building fatms...
docker run --rm -v "%cd%:/src" -w /src pspdev/pspdev sh -c "apk add --no-cache python3 >/dev/null 2>&1; make"

echo.
echo Building extremespeed app...
docker run --rm -v "%cd%:/src" -w /src pspdev/pspdev make -C extremespeed
