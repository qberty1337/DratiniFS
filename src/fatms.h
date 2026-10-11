/* fatms.h - shared decls for the faithful Sony 6.61 sceMSFAT_Driver port.
 * Context field names map to Sony's ctx@0xd088 offsets (see fatfs_core_notes.md).
 * Each volume has Sony's FATFS table (vol_ops, ctx+0x1f8): fatms's Memory Stick table, and on the
 * PSP Go (built with PSPGO into fatmsef.prx) the internal-storage table of fatfs_ef.c.
 * Sony's exact error constants are preserved. */
#ifndef FATMS_H
#define FATMS_H

#include <pspkernel.h>
#include <pspiofilemgr_kernel.h>
#include <pspiofilemgr.h>
#include <pspthreadman.h>
#include <pspthreadman_kernel.h>
#include <pspsysmem_kernel.h>
#include <pspsysevent.h>
#include <string.h>
#include <stdio.h>
#include <stddef.h>
#include "fatcache.h"       /* Sony FAT-cache engine (host-tested); embedded in the ctx below */
#include "fatpath.h"        /* Sony path/name helpers (host-tested) */

/* ---- Sony fatms error codes (returned verbatim) ---- */
#define FAT_ENOENT   (int)0x80010002
#define FAT_EBADF    (int)0x80010009
#define FAT_ENOMEM   (-0x7ffefff4)   /* 0x8001000c resource */
#define FAT_EACCES   (int)0x8001000d /* write-protect / read-only / not ready */
#define FAT_ENXIO    (-0x7ffefff0)   /* 0x80010010 no media / dev not ready */
#define FAT_EEXIST   (int)0x80010011
#define FAT_ENODEV   (-0x7ffeffed)   /* 0x80010013 */
#define FAT_ENOTDIR  (int)0x80010014
#define FAT_EINVAL   (-0x7ffeffea)   /* 0x80010016 */
#define FAT_EMFILE   (int)0x80010018
#define FAT_ENOSPC   (int)0x8001001c
#define FAT_EBADBPB  (-0x7ffe4ffa)   /* 0x8001b006 bad boot sector */
#define FAT_EMOUNTED (-0x7ffdfcdb)   /* already-mounted / mismatch */
#define FAT_EUNSUP   (int)0x80020325 /* unsupported ioctl/devctl */

/* state flags at ctx.flags (Sony ctx+0x12c) */
#define ST_PRESENT    0x01           /* media present (media thread) */
#define ST_OPEN       0x02           /* block device fd open */
#define ST_ASSIGNED   0x04           /* ms0: assigned */
#define ST_MOUNTED    0x08           /* fatfs_mount done */
#define ST_WHOLEDISK  0x10           /* opened msstor0: (no partition) */
#define ST_RDONLY_SW  0x20           /* block device opened read-only */
#define ST_NOMEDIA    0x40
#define ST_SUSPEND    0x80           /* sysevent 0x102 .. 0x10000 */
#define ST_ASSIGN_ERR 0x200          /* PSP Go internal storage: the last assign failed */

/* media/FS flags at ctx.media_flags (Sony ctx+0xdc) */
#define MF_OK         0x0001         /* identified + valid BPB (FUN_00005978) */
#define MF_BAD        0x0020         /* identify failed */
#define MF_WP         0x0800         /* write protected (ioctl 0x2125008) */
#define MF_INSERTED   0x2000
#define MF_REMOVED    0x4000

#define FAT_NUM_FD     10     /* FdBuf = 10 slots x 0x684 */
#define FAT_FD_SIZE    0x684
#define FAT_NUM_DIRENT 4      /* dirent cache, 0x800 each */

struct fatms_ctx;

/* dir iterator (Sony 6 words): start cluster (0 = root), current cluster, sector LBA,
   cluster index, sector within cluster (or within the FAT12/16 root), entry within sector */
typedef struct { u32 start, cur, lba, cidx, sidx, eidx; } diter;

/* open-file slot, exactly Sony's 0x684-byte layout (FdBuf) */
typedef struct OpenFile {
    u32    is_dir;            /* +0x000 handle kind: 0 file, 1 dir */
    struct fatms_ctx *ctx;    /* +0x004 */
    fpath  path;              /* +0x008 (+0x214 = name, +0x414 = 8.3 string) */
    u32    ckpt[128];         /* +0x424 cluster checkpoints (FUN_00004d90) */
    u32    ckpt_count;        /* +0x624 */
    u32    cur_cluster_index; /* +0x628 */
    u32    cur_cluster;       /* +0x62c */
    u32    rsv[3];            /* +0x630 (+0x638 = reserve run: never set in 6.61, so FUN_00002a28 is dead) */
    u8     entry[32];         /* +0x63c the 32-byte directory entry; size at +0x658 */
    u32    oflags;            /* +0x65c open flags */
    u32    position;          /* +0x660 */
    u32    first_cluster;     /* +0x664 */
    diter  dirpos;            /* +0x668 where `entry` lives */
    u32    inuse;             /* +0x680 */
} OpenFile;
#define F_SIZE(f)  (*(u32 *)((f)->entry + 0x1c))
#define F_ATTR(f)  ((f)->entry[0x0b])

/* directory handle: a heap scratch block (FUN_0000736c), not an fd slot */
typedef struct {
    u32    is_dir;            /* +0x000 = 1 */
    struct fatms_ctx *ctx;    /* +0x004 */
    fpath  path;              /* +0x008 (dir path at +0xc, wildcard pattern at +0x214) */
    diter  it;                /* +0x424 next entry; it.lba == 0 => end */
} DirHandle;

_Static_assert(sizeof(OpenFile) == FAT_FD_SIZE && offsetof(OpenFile, inuse) == 0x680 &&
               offsetof(OpenFile, entry) == 0x63c && offsetof(OpenFile, dirpos) == 0x668, "Sony fd slot layout");
_Static_assert(offsetof(DirHandle, it) == 0x424 && sizeof(DirHandle) <= 0x800, "Sony dir handle layout");

/* one DirentCache slot (Sony ctx+0x30, stride 0x24): a 4-sector window of one directory */
typedef struct { u8 *buf; u32 dirty, stamp, tag, cur, lba, cidx, sbase, nsec; } dcache_slot;

/* Block-device sub-object (Sony vtable +0x1c/+0x20) */
typedef struct {
    int (*read_sectors)(struct fatms_ctx *c, void *buf, u32 lba, u32 n);   /* vt+0x1c */
    int (*write_sectors)(struct fatms_ctx *c, const void *buf, u32 lba, u32 n);
} fatms_blockdev;

/* Sony's per-volume FATFS table (ctx+0x1f8). Slot +0x0c is never called and +0x1c/+0x20 (sector
   read/write) are the same functions for every volume (ctx.blk), so they are not here. */
typedef struct vol_ops {
    int (*init)(struct fatms_ctx *c);                                         /* +0x00 IoInit */
    int (*exit)(struct fatms_ctx *c);                                         /* +0x04 IoExit */
    int (*start)(struct fatms_ctx *c);                                        /* +0x08 after AddDrv */
    int (*mount)(struct fatms_ctx *c, const char *fs, const char *blockdev, unsigned flags);   /* +0x10 */
    int (*umount)(struct fatms_ctx *c);                                       /* +0x14 */
    int (*rescan)(struct fatms_ctx *c);                                       /* +0x18 */
    int (*format)(struct fatms_ctx *c, int arg);                              /* +0x24 devctl 0x241d813 */
    int (*identify)(struct fatms_ctx *c);                                     /* +0x28 */
    int (*devctl)(struct fatms_ctx *c, unsigned int cmd, void *in, int inlen, void *out, int outlen); /* +0x2c */
    int (*sysevent)(struct fatms_ctx *c, int ev, void *param);                /* +0x30 */
} vol_ops;

typedef struct fatms_ctx {
    /* --- FAT geometry (filled by fatfs_identify / BPB parse) --- */
    u16 bytes_per_sector;      /* +0x00  must be 0x200 */
    u16 sectors_per_cluster;   /* +0x02 */
    u16 reserved_sectors;      /* +0x04 */
    u16 root_entries;          /* +0x06  0 => FAT32 */
    u8  num_fats;              /* +0x08 */
    u8  media_type;            /* +0x09 */
    u16 ms_sectors_per_cluster;/* +0x0a MS erase block in clusters (ioctl 0x2125803) */
    u32 total_sectors;         /* +0x0c */
    u32 fat_size;              /* +0x10 */
    u32 root_cluster;          /* +0x14 FAT32 */
    u16 fsinfo_sector;         /* +0x18 */
    u16 backup_boot;           /* +0x1a */
    u16 ms_unit;               /* +0x1c erase-unit clusters or 0xffff */
    u32 ms_max_cluster;        /* +0x20 max cluster rounded to erase blocks or 0xffff */
    u32 root_dir_lba;          /* +0xc8  (FAT16 root-dir start sector) */
    u32 data_start_lba;        /* +0xcc  (first data sector) */
    u32 fat_max_cluster;       /* +0xd0  (highest valid cluster + 1) */
    u32 cluster_shift;         /* +0xd8  log2(sectors per cluster) */
    u32 fat_window_sectors;    /* +0xd4  FAT-sector cache window size */
    int fat_type;              /* 12 / 16 / 32  (Sony: flag bits 0x1000/0x8000 at +0xdc) */

    u32   fat_cache_tag;        /* +0x24 = 0xffffffff */
    void *fat_cache;           /* +0x28 */
    u32   fat_cache_count;      /* +0x2c = 6 */
    dcache_slot dc[FAT_NUM_DIRENT]; /* +0x30 DirentCache */
    u32  dc_clock;             /* +0xc0 LRU clock */
    u32  mode_c4;              /* +0xc4 1 = dirent/FAT write-through (devctl 0x241d814) */

    u32  media_flags;          /* +0xdc  MF_* */
    const vol_ops *ops;        /* +0x1f8 */
    fatms_blockdev *blk;       /* vtable +0x1c/+0x20 */
    void *data_buf;            /* +0x13c one-sector bounce buffer */
    u32  flags;                /* +0x12c state flags (ST_*) */
    int  ctrl_fd;              /* +0x1d0 block device fd */
    u32  mount_count;          /* +0x130 */
    u32  assign_count;         /* +0x134 */
    int  assign_result;        /* +0x138 */

    SceUID fdbuf_fpl, fatcache_fpl, dirent_fpl, databuf_fpl; /* +0x140/44/48/4c */
    void  *fd[FAT_NUM_FD];     /* +0x1fc.. 10 FD slots into FdBuf */
    u32    tbl150[32];         /* +0x150 insert/eject callbacks */

    SceUID media_thread;       /* +0x1d4 */
    SceUID detect_evf;         /* +0x1d8 "SceFatfsDetectMedium" */
    u32    detect_arg;         /* +0x1dc */
    u32    in_insert;          /* +0x1e0 */
    u32    unk1e4;             /* +0x1e4 busy countdown (sysevent 0x100002) */

    const char *name_ms0;      /* +0x1e8 "ms0:" */
    const char *name_fatms0;   /* +0x1ec "fatms0:" */
    const char *name_msstor0p1;/* +0x1f0 "msstor0p1:" */
    const char *name_msstor0;  /* +0x1f4 "msstor0:" */

    const char *name_fatms;    /* +0x224 "fatms" */
    const char *obj_name;      /* +0x228 "SceFatfsMs" (mutex/heap/buf names) */
    u32    vol_type;           /* +0x22c */
    SceUID mutex;              /* +0x230 */
    fat_engine fe;             /* Sony FAT-cache engine state (FAT I/O + alloc + free counts) */
    u32    exfat;              /* DratiniFS: the volume is exFAT (exfat.c serves every FS worker) */
} fatms_ctx;

/* Standard kernel/libc prototypes come from the pspsdk headers above; our .S stubs
 * supply the symbols (we deliberately do NOT link -lpspkernel). Only the custom
 * imports (k_sceIo* wrappers, the codepage/rtc drivers) are declared here. */
extern int  k_sceIoAddDrv(PspIoDrv *drv);
extern int  k_sceIoDevctl(const char *dev, unsigned int cmd, void *indata, int inlen, void *outdata, int outlen);
extern int  k_sceIoIoctl(SceUID fd, unsigned int cmd, void *indata, int inlen, void *outdata, int outlen);
extern SceUID k_sceIoOpen(const char *name, int flags, SceMode mode);
extern int    k_sceIoClose(SceUID fd);
extern int    k_sceIoRead(SceUID fd, void *buf, int size);
extern int    k_sceIoWrite(SceUID fd, const void *buf, int size);
extern SceOff k_sceIoLseek64k(SceUID fd, SceOff ofs, int whence);
extern int    k_sceIoAssign(const char *dev, const char *dev2, const char *dev3, int mode, void *unk1, int unk2);
extern int    k_sceIoUnassign(const char *dev);
extern int    k_sceIoTerminateFd(const char *name);
/* regular kernel mutex API is not in the pspsdk headers (only LwMutex) */
extern SceUID sceKernelCreateMutex(const char *name, u32 attr, int initCount, void *opt);
extern int    sceKernelLockMutex(SceUID m, int count, unsigned int *timeout);
extern int    sceKernelUnlockMutex(SceUID m, int count);
extern int    sceKernelDeleteMutex(SceUID m);
extern int  sceKernelGetCompiledSdkVersion(void);       /* caller SDK: Sony switches name encodings at 3.80 */
extern int  sceCodepage_driver_EE932176(void);
extern int  sceCodepage_driver_1D0DE569(int *a, int *b, int *c, int *d);
extern int  sceCodepage_driver_039BF9E9(void *t0, int s0, void *t1, int s1, void *t2, int s2, void *t3, int s3);  /* load tables */
extern int  sceCodepage_driver_B0AE63AA(int cp);          /* set codepage (0x0d = SJIS) */
extern int  sceCodepage_driver_0AA54D6D(int *arg);        /* get codepage */
extern char sceCodepage_driver_DCD95405(int ch);         /* uppercase an OEM char */
/* RTC driver (Sony imports): DOS time <-> ScePspDateTime, current local time */
extern int  sceRtc_driver_74772CCC(ScePspDateTime *dt, u32 dos);        /* DOS -> datetime */
extern int  sceRtc_driver_9012B140(ScePspDateTime *dt);                 /* current local time */
extern int  sceRtc_driver_A4A5BF1B(const ScePspDateTime *dt, u32 *dos); /* datetime -> DOS */

/* $k1 of the caller: bit 31 is set while serving a user-mode syscall. (Host tests: tests/host.) */
#ifdef FATMS_HOST
extern u32 host_k1;
static inline u32 fat_k1(void) { return host_k1; }
#else
static inline u32 fat_k1(void) { u32 k1; __asm__ volatile("move %0, $k1" : "=r"(k1)); return k1; }
#endif

/* ---- FATFS core (fatfs_core.c) ---- */
extern fatms_ctx g_ctx;                                 /* the Memory Stick volume: fatms, ms0: */
extern const vol_ops g_ms_ops;                          /* its table (Sony 0xd040) */
extern fatms_blockdev fatfs_blk;                        /* sector read/write, shared by every volume */
int  fatfs_register(PspIoDrv *drv, fatms_ctx *c);       /* fatms_init_and_addDrv (0x2d08) */
int  fatfs_ioinit(PspIoDrvArg *arg);                    /* fatms_IoInit (0x2ea0) */
fatms_ctx *fatfs_find(const char *drvname);             /* FUN_00002c94 */
fatms_ctx *fatfs_redirect(void);                        /* DAT_0000d2c4: the emulated ms0: volume */
int  fatfs_mount(fatms_ctx *c, const char *fs_name, const char *blockdev, unsigned flags);
int  fatfs_umount(fatms_ctx *c);    /* vtable+0x14 */
void fatfs_clear_tbl150(fatms_ctx *c);              /* FUN_0000b338 */
int  fatfs_bpb_parse(fatms_ctx *c, const u8 *buf);  /* identify's boot-sector part (ba94 / fatmsef d60c) */
#ifdef PSPGO
int  vol_emulated(void);            /* fatmsef 0xe74c: ms0: is internal storage (a game runs from it) */
#else
static inline int vol_emulated(void) { return 0; }      /* Sony's FUN_0000c8d4 on every model but the Go */
#endif
void fatfs_set_flags(fatms_ctx *c, u32 mask, int set);
void *fatfs_alloc_block(SceUID *fpl, int size, const char *name); /* FUN_00004f04 1-block Fpl */
u8  *fatfs_get_scratch(fatms_ctx *c);              /* FUN_00004fcc 0x800 pool */
void fatfs_put_scratch(fatms_ctx *c, void *b);     /* FUN_00005044 */
int  fatfs_prep(fatms_ctx *c);                     /* FUN_0000573c per-op lazy (re)mount */
int  fatfs_ready(fatms_ctx *c);                    /* FUN_00005978 media-ready gate */
int  fatfs_fd_valid(fatms_ctx *c, void *f);        /* FUN_0000592c fd-handle validation */
int  fatfs_flush(fatms_ctx *c);                    /* FUN_000056fc: dirent cache + FAT window */
int  fatfs_rescan(fatms_ctx *c);                   /* vtable+0x18: force an eject+insert cycle */
int  fatfs_blkdevctl(fatms_ctx *c, int enable);    /* vtable+0x24 */
void fatfs_notify_all(fatms_ctx *c, int arg);      /* FUN_0000b398 */
int  fatfs_cb_register(fatms_ctx *c, SceUID cb);   /* FUN_0000c770 */
int  fatfs_cb_unregister(fatms_ctx *c, SceUID cb); /* FUN_0000b430 */
int  fatfs_cb_arg(void *in, int inlen, SceUID *cb);  /* FUN_0000c528/c5b4: a callback uid the caller may use */
int  ms_devctl(fatms_ctx *c, unsigned int cmd, void *in, int inlen, void *out, int outlen);  /* vtable+0x2c (beb4) */

#ifdef PSPGO
/* ---- PSP Go internal storage (fatfs_ef.c) ---- */
extern fatms_ctx g_ctx_ef, g_ctx_eh;                /* fatef ef0: (type 2), fateh eh0: (type 3) */
extern const vol_ops g_ef_ops;                      /* fatmsef 0xf2d0 */
int  ef_alias_ms0(void);                            /* fatmsef 0xe7d8: ms0:'s devices -> msemu + fatef0: */
extern int sceKernelBootFrom(void);                 /* InitForKernel 0x9D33A110 */
extern int sceKernelApplicationType(void);          /* InitForKernel 0x7233B5BC */
extern int sceKernelInitApitype(void);              /* InitForKernel 0x7A2333AD */
extern int sceKernelGetModel(void);                 /* SysMemForKernel 0x07C586A1 */
extern int sceKernelUtilsMd5Digest(u8 *data, u32 size, u8 *digest);         /* UtilsForKernel 0xC8186A58 */
extern int sceIdStorageLookup(u16 key, u32 offset, void *buf, u32 len);    /* sceIdStorage_driver 0x6FE062D1 */
extern int sceEFlash_driver_8FB8591D(int timeout);  /* wait until the internal flash is up */
extern int k_sceIoAddAlias(const char *alias, const char *dev);             /* IoFileMgrForKernel 0xE5323C5B */
#endif

/* ---- DratiniFS: cards up to 2 TB (msstor_cap.c) ---- */
void msstor_cap_insert(fatms_ctx *c);              /* media insert, after msstor0p1: is open */
int  mbr_entry(int i, u8 *type, u32 *lba, u32 *len); /* the inserted card's MBR entry i (0 = ok) */

/* ---- DratiniFS: exFAT (exfat.c). Each FS worker hands over right after Sony's entry checks. ---- */
int  exfat_identify(fatms_ctx *c, const u8 *boot);   /* 1 = not an exFAT boot sector */
int  exfat_mount(fatms_ctx *c);                      /* fatfs_prep, after the FAT window is set up */
int  exfat_open(fatms_ctx *c, fpath *p, unsigned flags, OpenFile **out);
int  exfat_close(fatms_ctx *c, OpenFile *f);
int  exfat_read(fatms_ctx *c, OpenFile *f, void *buf, int len);
int  exfat_write(fatms_ctx *c, OpenFile *f, const void *buf, int len);
int  exfat_dopen(fatms_ctx *c, fpath *p, DirHandle **out);
int  exfat_dread(fatms_ctx *c, DirHandle *d, SceIoDirent *de, void *priv);
int  exfat_getstat(fatms_ctx *c, fpath *p, SceIoStat *st);
int  exfat_chstat(fatms_ctx *c, fpath *p, const SceIoStat *st, int bits);
int  exfat_mkdir(fatms_ctx *c, fpath *p);
int  exfat_remove(fatms_ctx *c, fpath *p);
int  exfat_rmdir(fatms_ctx *c, fpath *p);
int  exfat_move(fatms_ctx *c, fpath *src, fpath *dst);
int  exfat_sync_free(fatms_ctx *c);                  /* devinfo: the bitmap's free count */
int  exfat_root_free(fatms_ctx *c, u32 *out);
int  dread_emit(fatms_ctx *c, SceIoDirent *de, void *priv, const u8 *ent, const u16 *leaf, const char *sfn);
int  flagcheck(unsigned flags, int exists);          /* fatfs_ops.c FUN_00008514 (shared with exFAT) */
u32  slot_scan(fatms_ctx *c, const fpath *p, int *idx); /* FUN_0000a200 */

/* ---- DratiniFS: ExtremeSpeed partitions (es.c), hooked into main.c's workers ---- */
void es_load(fatms_ctx *c);                          /* fatfs_prep, after identify */
int  es_open(fatms_ctx *c, fpath *p, unsigned flags, OpenFile **out);   /* FAT_ENOENT: not ES */
int  es_is_file(OpenFile *f);
int  es_read(fatms_ctx *c, OpenFile *f, void *buf, int len);
int  es_getstat(fatms_ctx *c, fpath *p, SceIoStat *st);
int  es_vdir(fatms_ctx *c, fpath *p, SceIoStat *st);
int  es_remove(fatms_ctx *c, fpath *p);
int  es_dopen(fatms_ctx *c, fpath *p, DirHandle **out);
void es_dir_init(fatms_ctx *c, DirHandle *d, const fpath *p, int virt);
int  es_dir_virtual(DirHandle *d);
int  es_dread(fatms_ctx *c, DirHandle *d, SceIoDirent *de, void *priv);
int  es_devctl(fatms_ctx *c, unsigned int cmd, void *in, int inlen, void *out, int outlen);

/* ---- DratiniFS: formatting (format.c): devctl 0x0241D813 (XMB) and 0x04100005 (ES app) ---- */
int  dfs_format(fatms_ctx *c, int exfat);            /* 1 exFAT, 0 FAT32, -1 what the card is now */

/* ---- devctl tree (fatfs_devctl.c): FUN_00006c18 (fs) + vtable+0x2c (device) ---- */
int  fatfs_devctl(fatms_ctx *c, unsigned int cmd, void *indata, int inlen, void *outdata, int outlen);

/* ---- directory layer (fatfs_dirent.c) ---- */
int  dir_iter_init(fatms_ctx *c, diter *it, u32 cluster);                 /* FUN_00000a0c */
int  dir_iter_move(fatms_ctx *c, diter *it, int n);                       /* FUN_00000aa8 */
int  dirent_get(fatms_ctx *c, u8 **ent, diter *it);                       /* FUN_00000000 */
int  dirent_put(fatms_ctx *c, const u8 *ent, diter *it);                  /* FUN_00000094 */
int  dirent_flush(fatms_ctx *c);                                          /* FUN_00001220 */
void dirent_reset(fatms_ctx *c);                                          /* 573c dirent part */
void dirent_forget(fatms_ctx *c, u32 cluster);                            /* FUN_00000f18+12f4 */
void dirent_drop_all(fatms_ctx *c);                                       /* fatmsef 0x12f4 */
int  dir_scan(fatms_ctx *c, OpenFile *out, diter *it, const u16 *name, int wild);  /* FUN_00001af8 */
int  dir_lookup(fatms_ctx *c, OpenFile *out, u32 cluster, const u16 *name);      /* FUN_00001a8c */
int  dir_walk(fatms_ctx *c, const u16 *dirpath, u32 *cluster);                   /* FUN_000020f4 */
int  dir_find_free(fatms_ctx *c, OpenFile *out, u32 cluster, const u16 *name, int need_lfn); /* FUN_000017a4 */
int  dir_make_alias(fatms_ctx *c, fpath *p, u32 cluster);                 /* FUN_0000a2cc */
int  dir_write_entries(fatms_ctx *c, OpenFile *f);                        /* FUN_00000124 */
int  dir_delete_entries(fatms_ctx *c, OpenFile *f);                       /* FUN_00000288 */
int  dir_is_empty(fatms_ctx *c, u32 cluster);                             /* FUN_00008d28 */
int  dir_zero_cluster(fatms_ctx *c, u32 cluster);                         /* FUN_00000428 */
void dir_new_entry(fatms_ctx *c, OpenFile *f);                            /* FUN_00008664 */
void dirent_stat(const u8 *de, SceIoStat *st);                            /* FUN_00000538 */
void dirent_stamp(u8 *de, int created);                                   /* FUN_0000aac0 */
int  datetime_valid(const ScePspDateTime *dt);                            /* FUN_0000a9b0 */
int  vol_label_find(fatms_ctx *c, u8 *ent, diter *pos, int want_free);    /* FUN_00001f00 */
int  root_free_entries(fatms_ctx *c, u32 *out);                           /* FUN_00006210 */

/* ---- file + directory workers (fatfs_ops.c) ---- */
int  fatfs_path_get(fatms_ctx *c, const char *in, fpath **out);         /* FUN_00009794 */
void fatfs_path_put(fatms_ctx *c, fpath *p);                            /* FUN_00009908 */
int  fatfs_open(fatms_ctx *c, fpath *p, unsigned flags, OpenFile **out);  /* FUN_00008158 */
int  fatfs_close(fatms_ctx *c, OpenFile *f);                              /* FUN_00005e74 */
int  fatfs_lseek(fatms_ctx *c, OpenFile *f, SceOff ofs, int whence, SceOff *out);   /* FUN_00007c0c */
int  fatfs_at_eof(fatms_ctx *c, OpenFile *f);                             /* FUN_000079c4 */
int  fatfs_set_pattern(fatms_ctx *c, DirHandle *d, const char *pat);      /* FUN_00007a3c */
int  fatfs_dopen(fatms_ctx *c, fpath *p, DirHandle **out);                /* FUN_0000736c */
int  fatfs_dread(fatms_ctx *c, DirHandle *d, SceIoDirent *de, void *priv);/* FUN_00007610 */
int  fatfs_dclose(fatms_ctx *c, DirHandle *d);                            /* FUN_00005f28 */
int  fatfs_getstat(fatms_ctx *c, fpath *p, SceIoStat *st);                /* FUN_000078f0 */
int  fatfs_chstat(fatms_ctx *c, fpath *p, const SceIoStat *st, int bits); /* 0x5c18 */
int  fatfs_mkdir(fatms_ctx *c, fpath *p);                                 /* 0x7dc0 */
int  fatfs_remove(fatms_ctx *c, fpath *p);                                /* FUN_00008a20 */
int  fatfs_rmdir(fatms_ctx *c, fpath *p);                                 /* FUN_00008bc0 */
int  fatfs_move(fatms_ctx *c, fpath *src, fpath *dst);                    /* FUN_00006978 */

/* ---- data path (fatfs_read.c / fatfs_write.c) ---- */
int  fatfs_read(fatms_ctx *c, OpenFile *f, void *buf, int len);           /* FUN_000086c4 */
int  fatfs_write(fatms_ctx *c, OpenFile *f, const void *buf, int len);    /* FUN_00008de8 */
int  fatfs_seek_cluster(fatms_ctx *c, OpenFile *f, u32 cidx, u32 *cl);    /* FUN_00004d90 via checkpoints */
int  fatfs_walk(fatms_ctx *c, u32 cl, u32 n, u32 *out);                   /* FUN_00004d90 */
int  fatfs_data_io(fatms_ctx *c, OpenFile *f, int write, u8 *buf, u32 pos, u32 len, u32 cl, u32 *done);
void fatfs_ckpt_record(OpenFile *f, u32 idx, u32 cl);
int  fatfs_free_chain(fatms_ctx *c, u32 cl);                              /* FUN_000047ec */
u32  fatfs_clus2lba(fatms_ctx *c, u32 cl);                                /* FUN_000056e4 */
int  fatfs_sector_io(fatms_ctx *c, int write, u32 off, u32 len, u8 *buf, u32 lba, u32 *done); /* FUN_000053f0 */

#endif
