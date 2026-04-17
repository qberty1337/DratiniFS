TARGET = extremespeed
OBJS = src/main.o

PSP_FW_VERSION = 661

INCDIR = ../shared
CFLAGS = -O2 -G0 -Wall -Wextra -fno-strict-aliasing
CXXFLAGS = $(CFLAGS) -fno-exceptions -fno-rtti
ASFLAGS = $(CFLAGS)

LIBDIR =
LDFLAGS =
LIBS = -lpsppower -lpspkubridge

BUILD_PRX = 1
EXTRA_TARGETS = EBOOT.PBP
PSP_EBOOT_TITLE = DratiniFS ExtremeSpeed
PSP_EBOOT_ICON = ICON0.PNG

PSPSDK = $(shell psp-config --pspsdk-path)
include $(PSPSDK)/lib/build.mak
