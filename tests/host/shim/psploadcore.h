/* Host shim: the one SceModule field the driver reads (text_addr), plus the lookup. */
#ifndef HOST_PSPLOADCORE_H
#define HOST_PSPLOADCORE_H
#include "pspkernel.h"

typedef struct SceModule {
    char modname[28];
    u32  text_addr;
} SceModule;

SceModule *sceKernelFindModuleByName(const char *modname);

#endif
