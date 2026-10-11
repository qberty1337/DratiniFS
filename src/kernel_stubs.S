	.set noreorder

#include "pspimport.s"

// Kernel imports. Every NID here is in Sony 6.61 fatms's own import table (nid_map.md), except the
// two marked DratiniFS (msstor_cap.c, the large-card patch). Add nothing else without a reason.
// The PSP Go build (PSPGO) adds the ones marked fatmsef, all in Sony's fatmsef import table.

IMPORT_START "ThreadManForKernel", 0x00090000
IMPORT_FUNC "ThreadManForKernel", 0xB7D098C6, sceKernelCreateMutex
IMPORT_FUNC "ThreadManForKernel", 0xB011B11F, sceKernelLockMutex
IMPORT_FUNC "ThreadManForKernel", 0x6B30100F, sceKernelUnlockMutex
IMPORT_FUNC "ThreadManForKernel", 0xF8170FBE, sceKernelDeleteMutex
IMPORT_FUNC "ThreadManForKernel", 0x446D8DE6, sceKernelCreateThread
IMPORT_FUNC "ThreadManForKernel", 0xF475845D, sceKernelStartThread
IMPORT_FUNC "ThreadManForKernel", 0x9FA03CD3, sceKernelDeleteThread
IMPORT_FUNC "ThreadManForKernel", 0x4FE44D5E, sceKernelCheckThreadKernelStack
IMPORT_FUNC "ThreadManForKernel", 0xBC31C1B9, sceKernelExtendKernelStack
IMPORT_FUNC "ThreadManForKernel", 0x3AD58B8C, sceKernelSuspendDispatchThread
IMPORT_FUNC "ThreadManForKernel", 0x27E22EC2, sceKernelResumeDispatchThread
IMPORT_FUNC "ThreadManForKernel", 0x55C20A00, sceKernelCreateEventFlag
IMPORT_FUNC "ThreadManForKernel", 0x1FB15A32, sceKernelSetEventFlag
IMPORT_FUNC "ThreadManForKernel", 0x402FCF22, sceKernelWaitEventFlag
IMPORT_FUNC "ThreadManForKernel", 0xC11BA8C4, sceKernelNotifyCallback
IMPORT_FUNC "ThreadManForKernel", 0xC07BB470, sceKernelCreateFpl
IMPORT_FUNC "ThreadManForKernel", 0x623AE665, sceKernelTryAllocateFpl
IMPORT_FUNC "ThreadManForKernel", 0xED1410E0, sceKernelDeleteFpl

IMPORT_START "InterruptManagerForKernel", 0x00090000
IMPORT_FUNC "InterruptManagerForKernel", 0x092968F4, sceKernelCpuSuspendIntr
IMPORT_FUNC "InterruptManagerForKernel", 0x5F10D406, sceKernelCpuResumeIntr

IMPORT_START "UtilsForKernel", 0x00090000
IMPORT_FUNC "UtilsForKernel", 0x34B9FA9E, sceKernelDcacheWritebackInvalidateRange
IMPORT_FUNC "UtilsForKernel", 0xC2DF770E, sceKernelIcacheInvalidateRange       // DratiniFS
#ifdef PSPGO
IMPORT_FUNC "UtilsForKernel", 0xC8186A58, sceKernelUtilsMd5Digest              // fatmsef
#endif
IMPORT_START "LoadCoreForKernel", 0x00090000
IMPORT_FUNC "LoadCoreForKernel", 0xF6B1BF0F, sceKernelFindModuleByName        // DratiniFS

IMPORT_START "sceSysEventForKernel", 0x00090000
IMPORT_FUNC "sceSysEventForKernel", 0xCD9E4BB5, sceKernelRegisterSysEventHandler

IMPORT_START "SysclibForKernel", 0x00090000
IMPORT_FUNC "SysclibForKernel", 0x10F3BB61, memset
IMPORT_FUNC "SysclibForKernel", 0xAB7592FF, memcpy
IMPORT_FUNC "SysclibForKernel", 0x52DF196C, strlen
IMPORT_FUNC "SysclibForKernel", 0xC2145E80, snprintf
IMPORT_FUNC "SysclibForKernel", 0xC0AB8932, strcmp
IMPORT_FUNC "SysclibForKernel", 0x7AB35214, strncmp
IMPORT_FUNC "SysclibForKernel", 0xB49A7697, strncpy

IMPORT_START "SysMemForKernel", 0x00090000
IMPORT_FUNC "SysMemForKernel", 0x58148F07, sceKernelCreateHeap
IMPORT_FUNC "SysMemForKernel", 0x23D81675, sceKernelAllocHeapMemory
IMPORT_FUNC "SysMemForKernel", 0x87C2AB85, sceKernelFreeHeapMemory
IMPORT_FUNC "SysMemForKernel", 0xB4F00CB5, sceKernelGetCompiledSdkVersion
IMPORT_FUNC "SysMemForKernel", 0xC90B0992, sceKernelGetUIDcontrolBlock
#ifdef PSPGO
IMPORT_FUNC "SysMemForKernel", 0x07C586A1, sceKernelGetModel                    // fatmsef
#endif

// DOS time <-> ScePspDateTime + current clock (entry timestamps, getstat/dread/chstat)
IMPORT_START "sceRtc_driver", 0x00090000
IMPORT_FUNC "sceRtc_driver", 0x74772CCC, sceRtc_driver_74772CCC
IMPORT_FUNC "sceRtc_driver", 0x9012B140, sceRtc_driver_9012B140
IMPORT_FUNC "sceRtc_driver", 0xA4A5BF1B, sceRtc_driver_A4A5BF1B


#ifdef PSPGO
// PSP Go (fatmsef): internal storage, the emulated ms0: test, the format's factory layout
IMPORT_START "InitForKernel", 0x00090000
IMPORT_FUNC "InitForKernel", 0x9D33A110, sceKernelBootFrom
IMPORT_FUNC "InitForKernel", 0x7233B5BC, sceKernelApplicationType
IMPORT_FUNC "InitForKernel", 0x7A2333AD, sceKernelInitApitype
IMPORT_START "sceEFlash_driver", 0x00090000
IMPORT_FUNC "sceEFlash_driver", 0x8FB8591D, sceEFlash_driver_8FB8591D
IMPORT_START "sceIdStorage_driver", 0x00090000
IMPORT_FUNC "sceIdStorage_driver", 0x6FE062D1, sceIdStorageLookup
#endif
