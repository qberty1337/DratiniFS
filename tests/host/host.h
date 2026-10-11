/* host.h - the host stand-in for the PSP kernel below fatms (tests/host/hostkern.c) */
#ifndef HOST_H
#define HOST_H
#include <pspkernel.h>

/* the card: a whole-disk image served as msstor0: and msstor0p1: (see hostkern.c) */
extern u8  *host_disk;               /* its sector 0 (MBR / boot sector) */
extern u64  host_disk_bytes;
int  host_disk_load(const char *path);
int  host_disk_save(const char *path);
extern u32 host_part_lba;           /* where msstor0p1: starts (MBR entry 0, else 0) */
extern u32 host_attr_cap;           /* knob: the adapter's attribute capacity in sectors (default: the image) */
extern int host_ms0_opens;          /* msstor0: handles open right now */
extern u32 *host_ms_text;           /* the fake msstor module's text (sceMSstor_Driver) */
u32  host_ms_capacity(void);        /* what msstor's capacity getter (text+0x3334) returns */
void host_ms_setup(void);           /* msstor card setup: capacity back to the attribute value */

/* knobs */
extern volatile int host_wp;        /* card reports write-protected (ioctl 0x2125008) */
extern volatile u32 host_sdk;       /* sceKernelGetCompiledSdkVersion() */
extern volatile int host_ms_status; /* mscmhc0 0x2025801: 4 inserted, 3 inserting */
extern u32 host_k1;                 /* $k1 seen by the driver (0 = kernel caller) */
extern int host_kcall;              /* io_host: caller is kernel mode (no fd quota, kernel-only devctls) */

/* observations */
extern volatile u32 host_rd_calls, host_rd_sectors, host_wr_calls, host_wr_sectors;
extern volatile int host_illegal_writes;          /* writes reaching a write-protected card */
extern volatile SceUID host_last_cb;
extern volatile int host_last_cb_arg;
struct host_cp { void *block; unsigned block_size; void *t[4]; int s[4]; int sizes[4]; int calls; };
extern struct host_cp host_cp;                    /* sceCodepage_driver_039BF9E9 call */
extern PspSysEventHandler *host_sysev;
extern PspIoDrv *host_drv;                        /* the fatms driver */
extern void (*host_on_terminate)(const char *drvname);   /* sceIoTerminateFd(driver) */
PspIoDrvArg *host_devarg(const char *path);       /* the driver a path's device reaches (assigns, aliases) */
const char *host_assigned_drv(const char *dev);   /* the driver `dev` ("ms0:") is assigned to, or 0 */

/* PSP Go (fatmsef): internal flash images and the kernel calls the Go build adds */
int  host_ef_load(int i, const char *path);       /* 0 = eflash0a0f0p0: (ef0:), 1 = eflash0a0f1p0: (eh0:) */
int  host_ef_opens(int i);                        /* its open handles */
extern int host_bootfrom, host_apptype, host_apitype, host_model;   /* InitForKernel, sceKernelGetModel */
extern u8  host_idstorage[0x200];                 /* IdStorage key 0x48 ... */
extern int host_idstorage_ok;                     /* ... is there */
extern int host_msemu;                            /* msemu.prx's devices exist (for the aliases) */

/* the media thread: card insert / eject, waiting until fatms has (un)assigned ms0: */
int  host_boot(void);                             /* module_start + insert; 0 on success */
int  host_insert(void);
int  host_eject(void);
extern volatile u32 host_mounts;                  /* successful IoMounts so far */
int  host_remounted(u32 before);                  /* wait until ms0: is mounted again after `before` */
int  host_mutex_count(SceUID m);                  /* recursion depth of a kernel mutex */

#endif
