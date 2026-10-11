# One source, two modules (as Sony builds them):
#   fatms.prx    sceMSFAT_Driver  every model but the Go          -> flash0:/kd/fatms.prx
#   fatmsef.prx  sceFATFS_Driver  PSP Go (+ internal storage)      -> flash0:/kd/fatmsef.prx
# `make` builds both; the Go one is this Makefile again with GO=1 (objects in obj_go/).
GO ?= 0
# src/tmhooks.o must stay first: it pins Sony's ExtendKernelStack call sites at fixed text offsets
SRC_OBJS = src/tmhooks.o src/main.o src/fatfs_core.o src/fatfs_path.o src/fatfs_dirent.o src/fatfs_ops.o \
           src/fatfs_read.o src/fatfs_write.o src/fatfs_fatcache.o src/fatfs_devctl.o src/msstor_cap.o src/exfat.o src/es.o src/format.o \
           src/iofilemgr_kernel.o src/kernel_stubs.o
HEADERS = src/fatms.h src/fatcache.h src/fatpath.h src/exfat_upcase.h shared/extremespeed_format.h

ifeq ($(GO),1)
TARGET = dratinifs_go
OBJS = $(addprefix obj_go/,$(SRC_OBJS) src/fatfs_ef.o) obj_go/exports_go.o
PRX_EXPORTS = exports_go.exp
OUT = fatmsef
MODNAME = sceFATFS_Driver
HOOKS = 3144 3bec 4514
EXTRA_CFLAGS = -DPSPGO
else
TARGET = dratinifs
OBJS = $(SRC_OBJS) exports.o
PRX_EXPORTS = exports.exp
OUT = fatms
MODNAME = sceMSFAT_Driver
HOOKS = 30fc 3ba4 44cc
EXTRA_CFLAGS =
endif

BUILD_PRX = 1
# kernel-mode module that replaces fatms as the ms0: filesystem driver.
# We provide ALL kernel stubs ourselves (iofilemgr_kernel.S + kernel_stubs.S) and do
# NOT link -lpspkernel, to avoid the "stubs out of order" import-table corruption.
USE_KERNEL_LIBC = 1

PSP_FW_VERSION = 661

CFLAGS = -O2 -G0 -Wall -Wextra -fno-strict-aliasing -fno-builtin $(EXTRA_CFLAGS)
CXXFLAGS = $(CFLAGS) -fno-exceptions -fno-rtti
ASFLAGS = $(CFLAGS)

LIBS = -nostdlib -lpspdebug -lpspctrl_driver -lpspmodinfo -lpspsdk -lgcc

PSPSDK = $(shell psp-config --pspsdk-path)
include $(PSPSDK)/lib/build_prx.mak

$(OBJS): $(HEADERS)
obj_go/%.o: %.c
	@mkdir -p $(dir $@)
	$(CC) $(CFLAGS) -c -o $@ $<
obj_go/%.o: %.S
	@mkdir -p $(dir $@)
	$(AS) $(ASFLAGS) -c -o $@ $<

ifeq ($(GO),1)
all: $(TARGET).prx $(OUT).prx
else
all: $(TARGET).prx $(OUT).prx go
go:
	$(MAKE) GO=1
clean: clean_go
clean_go:
	$(MAKE) GO=1 clean
endif

# $(OUT).prx = compressed, deploy to flash0:/kd/  (build.bat installs python3 in the container)
$(OUT).prx: $(TARGET).prx pspgz.py SystemControl.hdr
	@# ARK TM/DC hooks Sony's ExtendKernelStack call sites by offset: they must stay put (src/tmhooks.S)
	@for a in $(HOOKS); do psp-objdump -d $(TARGET).elf | grep -qE "^ +$$a:.*jal.*sceKernelExtendKernelStack" \
	  || { echo "tmhooks.S: no ExtendKernelStack call at text+0x$$a"; exit 1; }; done
	cp $(TARGET).prx $(OUT)_uncompressed.prx
	python3 pspgz.py $(OUT).prx SystemControl.hdr $(OUT)_uncompressed.prx $(MODNAME) 0x1007
	@echo "  $(OUT).prx  uncompressed: $$(stat -c%s $(OUT)_uncompressed.prx) bytes"
	@echo "  $(OUT).prx  compressed:   $$(stat -c%s $(OUT).prx) bytes"
