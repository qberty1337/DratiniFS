	.set noreorder

#include "pspimport.s"

// consolidate stubs here for the local builder

IMPORT_START "ThreadManForKernel", 0x00090000
IMPORT_FUNC "ThreadManForKernel", 0xB7D098C6, sceKernelCreateMutex
IMPORT_FUNC "ThreadManForKernel", 0xB011B11F, sceKernelLockMutex
IMPORT_FUNC "ThreadManForKernel", 0x6B30100F, sceKernelUnlockMutex
IMPORT_FUNC "ThreadManForKernel", 0xF8170FBE, sceKernelDeleteMutex
IMPORT_FUNC "ThreadManForKernel", 0xCEADEB47, sceKernelDelayThread
IMPORT_FUNC "ThreadManForKernel", 0x446D8DE6, sceKernelCreateThread
IMPORT_FUNC "ThreadManForKernel", 0xF475845D, sceKernelStartThread
IMPORT_FUNC "ThreadManForKernel", 0x809CE29B, sceKernelExitDeleteThread
IMPORT_FUNC "ThreadManForKernel", 0x4FE44D5E, sceKernelCheckThreadKernelStack
IMPORT_FUNC "ThreadManForKernel", 0xBC31C1B9, sceKernelExtendKernelStack
IMPORT_FUNC "ThreadManForKernel", 0x3AD58B8C, sceKernelSuspendDispatchThread
IMPORT_FUNC "ThreadManForKernel", 0x27E22EC2, sceKernelResumeDispatchThread
IMPORT_FUNC "ThreadManForKernel", 0x55C20A00, sceKernelCreateEventFlag
IMPORT_FUNC "ThreadManForKernel", 0xEF9E4C70, sceKernelDeleteEventFlag
IMPORT_FUNC "ThreadManForKernel", 0x1FB15A32, sceKernelSetEventFlag
IMPORT_FUNC "ThreadManForKernel", 0x402FCF22, sceKernelWaitEventFlag
IMPORT_FUNC "ThreadManForKernel", 0xC11BA8C4, sceKernelNotifyCallback
IMPORT_FUNC "ThreadManForKernel", 0xC07BB470, sceKernelCreateFpl
IMPORT_FUNC "ThreadManForKernel", 0x623AE665, sceKernelTryAllocateFpl

/// cpu interrupts
IMPORT_START "InterruptManagerForKernel", 0x00090000
IMPORT_FUNC "InterruptManagerForKernel", 0x092968F4, sceKernelCpuSuspendIntr
IMPORT_FUNC "InterruptManagerForKernel", 0x5F10D406, sceKernelCpuResumeIntr

// maintly need dcache and time (reconsider?..)
IMPORT_START "UtilsForKernel", 0x00090000
IMPORT_FUNC "UtilsForKernel", 0x34B9FA9E, sceKernelDcacheWritebackInvalidateRange
IMPORT_FUNC "UtilsForKernel", 0x27CC57F0, sceKernelLibcTime

IMPORT_START "InitForKernel", 0x00090000
IMPORT_FUNC "InitForKernel", 0x7A2333AD, sceKernelInitApitype
IMPORT_START "sceSysEventForKernel", 0x00090000
IMPORT_FUNC "sceSysEventForKernel", 0xCD9E4BB5, sceKernelRegisterSysEventHandler
IMPORT_FUNC "sceSysEventForKernel", 0xD7D3FDCD, sceKernelUnregisterSysEventHandler

// kernel-mode libc (memset/memcpy/memcmp/strlen) to handle dma/cache coherency properly
IMPORT_START "SysclibForKernel", 0x00090000
IMPORT_FUNC "SysclibForKernel", 0x10F3BB61, memset
IMPORT_FUNC "SysclibForKernel", 0xAB7592FF, memcpy
IMPORT_FUNC "SysclibForKernel", 0x81D0D1F7, memcmp
IMPORT_FUNC "SysclibForKernel", 0x52DF196C, strlen
