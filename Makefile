TARGET = psp_exfat_flash
OBJS = src/main.o src/es.o src/iofilemgr_kernel.o src/kernel_stubs.o exports.o

BUILD_PRX = 1
PRX_EXPORTS = exports.exp

# kernel mode module replaces fatms as the ms0: filesystem driver
USE_KERNEL_LIBC = 1
# NOT using USE_KERNEL_LIBS — we provide ALL kernel stubs ourselves in
# iofilemgr_kernel.S + kernel_stubs.S to avoid the "stubs out of order"
# import table corruption that -lpspkernel causes when mixed with our stubs.

PSP_FW_VERSION = 661

INCDIR = shared
CFLAGS = -O2 -G0 -Wall -Wextra -fno-strict-aliasing -fno-builtin
CXXFLAGS = $(CFLAGS) -fno-exceptions -fno-rtti
ASFLAGS = $(CFLAGS)

LIBDIR =
LDFLAGS =
LIBS = -nostdlib -lpspdebug -lpspctrl_driver -lpspmodinfo -lpspsdk -lgcc

PSPSDK = $(shell psp-config --pspsdk-path)
include $(PSPSDK)/lib/build_prx.mak

# fatms.prx = compressed (deploy to flash0:/kd/)
# fatms_uncompressed.prx = raw build
all: $(TARGET).prx fatms.prx

fatms.prx: $(TARGET).prx pspgz.py SystemControl.hdr
	cp $(TARGET).prx fatms_uncompressed.prx
	python3 pspgz.py fatms.prx SystemControl.hdr fatms_uncompressed.prx sceMSFAT_Driver 0x1007
	@echo "  Uncompressed: $$(stat -c%s fatms_uncompressed.prx) bytes"
	@echo "  Compressed:   $$(stat -c%s fatms.prx) bytes"
