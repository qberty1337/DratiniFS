	.set noreorder

#include "pspimport.s"

// let's add some kernel mode funcs that are not shipped as a prebuilt library in modern pspsdk

IMPORT_START "IoFileMgrForKernel", 0x00090000
IMPORT_FUNC "IoFileMgrForKernel", 0x8E982A74, k_sceIoAddDrv
IMPORT_FUNC "IoFileMgrForKernel", 0xC7F35804, k_sceIoDelDrv
IMPORT_FUNC "IoFileMgrForKernel", 0xB2A628C1, k_sceIoAssign
IMPORT_FUNC "IoFileMgrForKernel", 0x6D08A871, k_sceIoUnassign
IMPORT_FUNC "IoFileMgrForKernel", 0x109F50BC, k_sceIoOpen
IMPORT_FUNC "IoFileMgrForKernel", 0x810C4BC3, k_sceIoClose
IMPORT_FUNC "IoFileMgrForKernel", 0x42EC03AC, k_sceIoWrite
IMPORT_FUNC "IoFileMgrForKernel", 0x68963324, k_sceIoLseek32
IMPORT_FUNC "IoFileMgrForKernel", 0x6A638D83, k_sceIoRead
IMPORT_FUNC "IoFileMgrForKernel", 0x27EB27B8, k_sceIoLseek64k
IMPORT_FUNC "IoFileMgrForKernel", 0x63632449, k_sceIoIoctl
IMPORT_FUNC "IoFileMgrForKernel", 0x54F5FB11, k_sceIoDevctl

IMPORT_START "LoadCoreForKernel", 0x00090000
IMPORT_FUNC "LoadCoreForKernel", 0xF6B1BF0F, sceKernelFindModuleByName

// prep for ark-4 msstor cache reinitialization
IMPORT_START "SystemCtrlPrivate", 0x00090000
IMPORT_FUNC "SystemCtrlPrivate", 0x5D7817CF, msstorCacheInit

// all 13 nids from sony's 6.61 fatms that I could find that maintained some relevance... for the most part
IMPORT_START "sceCodepage_driver", 0x00090000
IMPORT_FUNC "sceCodepage_driver", 0xEE932176, sceCodepage_driver_EE932176
IMPORT_FUNC "sceCodepage_driver", 0x1D0DE569, sceCodepage_driver_1D0DE569
IMPORT_FUNC "sceCodepage_driver", 0x039BF9E9, sceCodepage_driver_039BF9E9
IMPORT_FUNC "sceCodepage_driver", 0x907CBFD2, sceCodepage_driver_907CBFD2
IMPORT_FUNC "sceCodepage_driver", 0x47BDF633, sceCodepage_driver_47BDF633
IMPORT_FUNC "sceCodepage_driver", 0x855C5C2E, sceCodepage_driver_855C5C2E
IMPORT_FUNC "sceCodepage_driver", 0xDCD95405, sceCodepage_driver_DCD95405
IMPORT_FUNC "sceCodepage_driver", 0x014E0C72, sceCodepage_driver_014E0C72
IMPORT_FUNC "sceCodepage_driver", 0x0AA54D6D, sceCodepage_driver_0AA54D6D
IMPORT_FUNC "sceCodepage_driver", 0x11123ED1, sceCodepage_driver_11123ED1
IMPORT_FUNC "sceCodepage_driver", 0x266ABDD8, sceCodepage_driver_266ABDD8
IMPORT_FUNC "sceCodepage_driver", 0xB0AE63AA, sceCodepage_driver_B0AE63AA
IMPORT_FUNC "sceCodepage_driver", 0xC899572E, sceCodepage_driver_C899572E
