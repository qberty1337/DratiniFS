@echo off
echo Building fatms...
docker run --rm -v "%cd%:/src" -w /src pspdev/pspdev make

echo.
echo Building extremespeed app...
docker run --rm -v "%cd%:/src" -w /src pspdev/pspdev make -C extremespeed
