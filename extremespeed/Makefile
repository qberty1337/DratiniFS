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

# build the kernel helper PRX alongside the EBOOT.PBP
all: kernel_flash.prx

kernel_flash.prx: kernel_flash.c
	psp-gcc -I. -I$(PSPSDK)/include -O2 -G0 -Wall -fno-strict-aliasing \
	  -D_PSP_FW_VERSION=661 \
	  -c -o kernel_flash.o kernel_flash.c
	psp-gcc -L$(PSPSDK)/lib -specs=$(PSPSDK)/lib/prxspecs \
	  -Wl,-q,-T$(PSPSDK)/lib/linkfile.prx,-zmax-page-size=128 \
	  kernel_flash.o $(PSPSDK)/lib/prxexports.o \
	  -nostdlib -lpspmodinfo -lpspkernel \
	  -o kernel_flash.elf
	psp-fixup-imports kernel_flash.elf
	psp-prxgen kernel_flash.elf kernel_flash.prx
	rm -f kernel_flash.elf kernel_flash.o
