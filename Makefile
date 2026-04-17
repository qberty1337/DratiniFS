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

# deploy as plain PRX to flash0:/kd/fatms.prx.
all: $(TARGET).prx
	cp $(TARGET).prx fatms.prx
