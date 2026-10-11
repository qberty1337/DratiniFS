/* Host shim for the PSP SDK: just enough types and prototypes for the unmodified baseline
 * sources (and tests/common) to build as a 32-bit host program. Layouts match pspsdk's on the
 * PSP (ILP32, little endian). Implementations live in tests/host/hostkern.c. */
#ifndef HOST_PSPKERNEL_H
#define HOST_PSPKERNEL_H

#include <stdint.h>

#if UINTPTR_MAX != 0xffffffffu
#error "build the host tests 32-bit (tests/run_tests.sh uses an i386 container)"
#endif

typedef uint8_t  u8;
typedef uint16_t u16;
typedef uint32_t u32;
typedef uint64_t u64;
typedef int8_t   s8;
typedef int16_t  s16;
typedef int32_t  s32;
typedef int64_t  s64;
typedef int      SceUID;
typedef unsigned int SceSize;
typedef unsigned int SceUInt;
typedef int      SceMode;
typedef int64_t  SceOff;

typedef struct ScePspDateTime {
    unsigned short year, month, day, hour, minute, second;
    unsigned int   microsecond;
} ScePspDateTime;

typedef struct SceIoStat {
    SceMode        st_mode;
    unsigned int   st_attr;
    SceOff         st_size;
    ScePspDateTime sce_st_ctime;
    ScePspDateTime sce_st_atime;
    ScePspDateTime sce_st_mtime;
    unsigned int   st_private[6];
} SceIoStat;

typedef struct SceIoDirent {
    SceIoStat d_stat;
    char      d_name[256];
    void     *d_private;
    int       dummy;
} SceIoDirent;

#define PSP_O_RDONLY  0x0001
#define PSP_O_WRONLY  0x0002
#define PSP_O_RDWR    0x0003
#define PSP_O_APPEND  0x0100
#define PSP_O_CREAT   0x0200
#define PSP_O_TRUNC   0x0400
#define PSP_O_EXCL    0x0800
#define PSP_SEEK_SET  0
#define PSP_SEEK_CUR  1
#define PSP_SEEK_END  2

struct PspIoDrv;
typedef struct PspIoDrvArg { struct PspIoDrv *drv; void *arg; } PspIoDrvArg;
typedef struct PspIoDrvFileArg { u32 unk1; u32 fs_num; PspIoDrvArg *drv; u32 unk2; void *arg; } PspIoDrvFileArg;
typedef struct PspIoDrvFuncs {
    int (*IoInit)(PspIoDrvArg *arg);
    int (*IoExit)(PspIoDrvArg *arg);
    int (*IoOpen)(PspIoDrvFileArg *arg, char *file, int flags, SceMode mode);
    int (*IoClose)(PspIoDrvFileArg *arg);
    int (*IoRead)(PspIoDrvFileArg *arg, char *data, int len);
    int (*IoWrite)(PspIoDrvFileArg *arg, const char *data, int len);
    SceOff (*IoLseek)(PspIoDrvFileArg *arg, SceOff ofs, int whence);
    int (*IoIoctl)(PspIoDrvFileArg *arg, unsigned int cmd, void *indata, int inlen, void *outdata, int outlen);
    int (*IoRemove)(PspIoDrvFileArg *arg, const char *name);
    int (*IoMkdir)(PspIoDrvFileArg *arg, const char *name, SceMode mode);
    int (*IoRmdir)(PspIoDrvFileArg *arg, const char *name);
    int (*IoDopen)(PspIoDrvFileArg *arg, const char *dirname);
    int (*IoDclose)(PspIoDrvFileArg *arg);
    int (*IoDread)(PspIoDrvFileArg *arg, SceIoDirent *dir);
    int (*IoGetstat)(PspIoDrvFileArg *arg, const char *file, SceIoStat *stat);
    int (*IoChstat)(PspIoDrvFileArg *arg, const char *file, SceIoStat *stat, int bits);
    int (*IoRename)(PspIoDrvFileArg *arg, const char *oldname, const char *newname);
    int (*IoChdir)(PspIoDrvFileArg *arg, const char *dir);
    int (*IoMount)(PspIoDrvFileArg *arg);
    int (*IoUmount)(PspIoDrvFileArg *arg);
    int (*IoDevctl)(PspIoDrvFileArg *arg, const char *devname, unsigned int cmd, void *indata, int inlen, void *outdata, int outlen);
    int (*IoUnk21)(PspIoDrvFileArg *arg);
} PspIoDrvFuncs;
typedef struct PspIoDrv { const char *name; u32 dev_type; u32 unk2; const char *name2; PspIoDrvFuncs *funcs; } PspIoDrv;

typedef struct PspSysEventHandler {
    int size;
    char *name;
    int type_mask;
    int (*handler)(int ev_id, char *ev_name, void *param, int *result);
    int r28, busy;
    struct PspSysEventHandler *next;
    int reserved[9];
} PspSysEventHandler;

typedef struct _uidControlBlock { u8 raw[0x30]; } SceUidControlBlock;   /* only +0x16 (attr) is read */

#define PSP_MODULE_KERNEL       0x1000
#define PSP_MODULE_SINGLE_LOAD  0x0001
#define PSP_MODULE_SINGLE_START 0x0002
#define PSP_MODULE_NO_STOP      0x0004
#define PSP_MODULE_INFO(name, attr, major, minor) \
    static const char host_module_name[] __attribute__((unused)) = name

/* kernel API used by the baseline (stubbed in tests/host/hostkern.c) */
int    sceKernelCpuSuspendIntr(void);
void   sceKernelCpuResumeIntr(int state);
int    sceKernelSuspendDispatchThread(void);
int    sceKernelResumeDispatchThread(int state);
SceUID sceKernelCreateThread(const char *name, int (*entry)(SceSize, void *), int prio, int stack, SceUInt attr, void *opt);
int    sceKernelStartThread(SceUID thid, SceSize arglen, void *argp);
int    sceKernelDeleteThread(SceUID thid);
SceUID sceKernelCreateEventFlag(const char *name, int attr, int bits, void *opt);
int    sceKernelSetEventFlag(SceUID evid, u32 bits);
int    sceKernelWaitEventFlag(SceUID evid, u32 bits, u32 wait, u32 *outBits, SceUInt *timeout);
int    sceKernelNotifyCallback(SceUID cb, int arg2);
SceUID sceKernelCreateFpl(const char *name, int part, int attr, unsigned int size, unsigned int blocks, void *opt);
int    sceKernelTryAllocateFpl(SceUID uid, void **data);
int    sceKernelDeleteFpl(SceUID uid);
SceUID sceKernelCreateHeap(SceUID partitionid, SceSize size, int unk, const char *name);
void  *sceKernelAllocHeapMemory(SceUID heapid, SceSize size);
int    sceKernelFreeHeapMemory(SceUID heapid, void *block);
void   sceKernelDcacheWritebackInvalidateRange(const void *p, unsigned int size);
int    sceKernelExtendKernelStack(int type, void (*cb)(void *), void *arg);
int    sceKernelRegisterSysEventHandler(PspSysEventHandler *handler);
int    sceKernelGetUIDcontrolBlock(SceUID uid, SceUidControlBlock **block);

#endif
