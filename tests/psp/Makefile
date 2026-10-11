# fatms on-device conformance test: EBOOT.PBP for ms0:/PSP/GAME/FATMSTEST/
# Build (Git Bash):  sh tests/psp/build.sh   (pspdev/pspdev docker)
TARGET = fatmstest
OBJS = main.o io_psp.o scenarios.o sdkver.o

INCDIR = ../common
CFLAGS = -O2 -G0 -Wall -Wextra -Wno-unused-parameter -std=gnu11
ASFLAGS = $(CFLAGS)
LIBS = -lpspctrl

BUILD_PRX = 1
PSP_FW_VERSION = 661
EXTRA_TARGETS = EBOOT.PBP
PSP_EBOOT_TITLE = fatms conformance test

PSPSDK = $(shell psp-config --pspsdk-path)
include $(PSPSDK)/lib/build.mak

scenarios.o: ../common/scenarios.c ../common/fstest.h
	$(CC) $(CFLAGS) $(CPPFLAGS) -c -o $@ $<
