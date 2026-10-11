/*
 * sceMSFAT_Driver - faithful Sony 6.61 fatms.prx reimplementation; built with PSPGO it is the PSP Go's
 * sceFATFS_Driver (fatmsef.prx), the same handlers serving ms0:, ef0: and eh0:.
 * main.c = the 22 PspIoDrvFuncs handlers (Sony 0x2ea0..0x4344). Each one does Sony's gates
 * (fs_num, no-media, assigned), converts the path before taking the volume mutex (FUN_00009794),
 * then runs the worker on an extended kernel stack when the caller's is shallow:
 * 0x600 for read/write/lseek, 0x1000 for everything else.
 */
#include "fatms.h"

#ifdef PSPGO
#define MODNAME "sceFATFS_Driver"
#else
#define MODNAME "sceMSFAT_Driver"
#endif
PSP_MODULE_INFO(MODNAME,
                PSP_MODULE_KERNEL | PSP_MODULE_SINGLE_START | PSP_MODULE_SINGLE_LOAD | PSP_MODULE_NO_STOP,
                2, 9);                              /* Sony's version bytes: 09 02 (both modules) */
unsigned int module_sdk_version = 0x06060110;       /* Sony's exported value */

typedef int (*worker_t)(void *);
extern int sceKernelCheckThreadKernelStack(void);
/* tmhooks.S: `jal sceKernelExtendKernelStack` at Sony's text offsets (ARK Time Machine hooks them) */
extern int tm_ext_open(int size, worker_t w, void *a);
extern int tm_ext_dopen(int size, worker_t w, void *a);
extern int tm_ext_devctl(int size, worker_t w, void *a);

static int run_ext(int size, worker_t w, void *a)   /* returns the worker's result, as Sony's */
{
    if (sceKernelCheckThreadKernelStack() < size)   /* returns the worker's int (sdk prototype says void) */
        return sceKernelExtendKernelStack(size, (void (*)(void *))(void (*)(void))w, a);
    return w(a);
}
static int run_tm(int (*ext)(int, worker_t, void *), worker_t w, void *a)
{
    return sceKernelCheckThreadKernelStack() < 0x1000 ? ext(0x1000, w, a) : w(a);
}
static fatms_ctx *CTX(PspIoDrvFileArg *a) { return (fatms_ctx *)a->drv->arg; }
#define LOCK(c)   sceKernelLockMutex((c)->mutex, 1, 0)
#define UNLOCK(c) sceKernelUnlockMutex((c)->mutex, 1)

/* path ops. Layout matters: tmctrl reads the fpath pointer at arg+4 (Sony's {ctx, fpath, ...}). */
struct a_path { fatms_ctx *c; fpath *p, *p2; void *x; int i; };
static int path_op(PspIoDrvFileArg *a, const char *path, worker_t w, void *x, int i)
{
    if (a->fs_num != 0) return FAT_ENODEV;
    fatms_ctx *c = CTX(a);
    if (c->flags & ST_NOMEDIA) return FAT_ENXIO;
    if (!(c->flags & ST_ASSIGNED)) return FAT_ENODEV;
    struct a_path ap = { c, 0, 0, x, i };
    int r = fatfs_path_get(c, path, &ap.p);
    if (r < 0) return r;
    if ((r = LOCK(c)) >= 0) {
        r = run_ext(0x1000, w, &ap);
        if (r > 0) r = 0;
        UNLOCK(c);
    }
    fatfs_path_put(c, ap.p);
    return r;
}

static int fatms_IoInit(PspIoDrvArg *a) { return fatfs_ioinit(a); }                 /* 0x2ea0 */
static int fatms_IoExit(PspIoDrvArg *a)                                             /* 0x2f90 */
{
    fatms_ctx *c = (fatms_ctx *)a->arg;
    c->ops->exit(c);
    sceKernelDeleteMutex(c->mutex);
    return 0;
}

static int t_open(void *v) { struct a_path *x = v; OpenFile *f = 0;
    int r = es_open(x->c, x->p, (unsigned)x->i, &f);                     /* DratiniFS: ES first for reads */
    if (r == FAT_ENOENT) r = fatfs_open(x->c, x->p, (unsigned)x->i, &f);
    if (r >= 0) x->x = f;
    return r; }
static int fatms_IoOpen(PspIoDrvFileArg *a, char *file, int flags, SceMode mode)    /* 0x2fcc -> FUN_00008158 */
{
    (void)mode;
    if (a->fs_num != 0) return FAT_ENODEV;
    fatms_ctx *c = CTX(a);
    if (c->flags & ST_NOMEDIA) return FAT_ENXIO;
    if (!(c->flags & ST_ASSIGNED)) return FAT_ENODEV;
    struct a_path ap = { c, 0, 0, 0, flags };
    int r = fatfs_path_get(c, file, &ap.p);
    if (r < 0) return r;
    if ((r = LOCK(c)) >= 0) {
        r = run_tm(tm_ext_open, t_open, &ap);
        if (r >= 0) { a->arg = ap.x; r = 0; }
        UNLOCK(c);
    }
    fatfs_path_put(c, ap.p);
    return r;
}

struct a_fd { fatms_ctx *c; OpenFile *f; void *buf; int len; SceOff ofs; int whence; SceOff pos; };
static int t_close(void *v) { struct a_fd *x = v; return fatfs_close(x->c, x->f); }
static int fatms_IoClose(PspIoDrvFileArg *a)                                         /* 0x310c -> FUN_00005e74 */
{
    if (!a) return FAT_EINVAL;
    if (a->fs_num != 0) return FAT_ENODEV;
    fatms_ctx *c = CTX(a);
    OpenFile *f = (OpenFile *)a->arg;
    if (c->flags & ST_NOMEDIA) return FAT_ENXIO;
    int r = LOCK(c);
    if (r < 0) return r;
    r = FAT_EINVAL;
    if (f && f->is_dir == 0) {                       /* Sony returns 0 whatever the close/flush did */
        struct a_fd x = { c, f, 0, 0, 0, 0, 0 };
        run_ext(0x1000, t_close, &x);
        r = 0;
    }
    UNLOCK(c);
    return r;
}

static int t_read(void *v)  { struct a_fd *x = v; return fatfs_read(x->c, x->f, x->buf, x->len); }
static int t_write(void *v) { struct a_fd *x = v; return fatfs_write(x->c, x->f, x->buf, x->len); }
static int fatms_IoRead(PspIoDrvFileArg *a, char *d, int l)                          /* 0x31f0 -> FUN_000086c4 */
{
    if (!d) return FAT_EINVAL;
    if (a->fs_num != 0) return FAT_ENODEV;
    fatms_ctx *c = CTX(a);
    OpenFile *f = (OpenFile *)a->arg;
    if (c->flags & ST_NOMEDIA) return FAT_ENXIO;
    int r = LOCK(c);
    if (r < 0) return r;
    r = 0;
    if (l != 0) {
        r = FAT_EINVAL;
        if (f && f->is_dir == 0) {
            struct a_fd x = { c, f, d, l, 0, 0, 0 };
            if ((r = run_ext(0x600, t_read, &x)) >= 0 && r != l) {   /* short read: fine at EOF, else EBADF */
                int e = fatfs_at_eof(c, f);                          /* FUN_000079c4, on the caller's stack */
                if (e != 1) r = (e < 0) ? e : (int)0x80010009;
            }
        }
    }
    UNLOCK(c);
    return r;
}
static int fatms_IoWrite(PspIoDrvFileArg *a, const char *d, int l)                   /* 0x3338 -> FUN_00008de8 */
{
    if (!d) return FAT_EINVAL;
    if (a->fs_num != 0) return FAT_ENODEV;
    fatms_ctx *c = CTX(a);
    OpenFile *f = (OpenFile *)a->arg;
    if (c->flags & ST_NOMEDIA) return FAT_ENXIO;
    int r = LOCK(c);
    if (r < 0) return r;
    r = 0;
    if (l != 0) {
        r = FAT_EINVAL;
        if (f && f->is_dir == 0) {
            struct a_fd x = { c, f, (void *)d, l, 0, 0, 0 };
            r = run_ext(0x600, t_write, &x);
        }
    }
    UNLOCK(c);
    return r;
}
static int t_seek(void *v) { struct a_fd *x = v; return fatfs_lseek(x->c, x->f, x->ofs, x->whence, &x->pos); }
static SceOff fatms_IoLseek(PspIoDrvFileArg *a, SceOff ofs, int whence)              /* 0x3458 -> FUN_00007c0c */
{
    if (a->fs_num != 0) return FAT_ENODEV;
    fatms_ctx *c = CTX(a);
    OpenFile *f = (OpenFile *)a->arg;
    if (c->flags & ST_NOMEDIA) return FAT_ENXIO;
    int r = LOCK(c);
    if (r < 0) return r;
    SceOff ret = FAT_EINVAL;
    if (f && f->is_dir == 0) {
        struct a_fd x = { c, f, 0, 0, ofs, whence, 0 };
        if ((r = run_ext(0x600, t_seek, &x)) >= 0) { ret = x.pos; *(SceOff *)((u8 *)a + 0x18) = x.pos; }   /* iob i_fpos */
        else ret = r;
    }
    UNLOCK(c);
    return ret;
}

/* FUN_00007b80: 0x242d016 = EOF probe on a file; 0x2415050 = dread wildcard on a dir */
struct a_ioctl { fatms_ctx *c; void *h; unsigned int cmd; void *in; };
static int t_ioctl(void *v) { struct a_ioctl *x = v; u32 dir = *(u32 *)x->h;
    if (x->cmd == 0x02415050) return (!dir || !x->in) ? FAT_EINVAL : fatfs_set_pattern(x->c, (DirHandle *)x->h, *(const char **)x->in);
    if (x->cmd == 0x0242D016) return dir ? FAT_EINVAL : fatfs_at_eof(x->c, (OpenFile *)x->h);
    return (!dir && es_is_file((OpenFile *)x->h)) ? 0 : FAT_EUNSUP; }   /* DratiniFS: ES files accept any */
static int fatms_IoIoctl(PspIoDrvFileArg *a, unsigned int cmd, void *i, int il, void *o, int ol)   /* 0x35b8 */
{
    (void)il; (void)o; (void)ol;
    if (!a) return FAT_EINVAL;
    if (a->fs_num != 0) return FAT_ENODEV;
    /* loader probes: modulemgr/loadexec refuse to load from a device that fails these */
    if (cmd == 0x00208002 || cmd == 0x00208013 || cmd == 0x00208014 || cmd == 0x00208082) return 0;
    fatms_ctx *c = CTX(a);
    if (c->flags & ST_NOMEDIA) return FAT_ENXIO;
    if (!a->arg) return FAT_EINVAL;
    int r = LOCK(c);
    if (r < 0) return r;
    struct a_ioctl x = { c, a->arg, cmd, i };
    r = run_ext(0x1000, t_ioctl, &x);
    UNLOCK(c);
    return r > 0 ? 0 : r;
}

static int t_remove(void *v) { struct a_path *x = v; int r = fatfs_remove(x->c, x->p);
    return r == FAT_ENOENT ? es_remove(x->c, x->p) : r; }              /* DratiniFS: then ES */
static int t_mkdir(void *v)  { struct a_path *x = v; return fatfs_mkdir(x->c, x->p); }
static int t_rmdir(void *v)  { struct a_path *x = v; return fatfs_rmdir(x->c, x->p); }
static int t_getstat(void *v){ struct a_path *x = v; int r = es_getstat(x->c, x->p, (SceIoStat *)x->x);
    if (r == FAT_ENOENT) r = fatfs_getstat(x->c, x->p, (SceIoStat *)x->x);   /* DratiniFS: ES first */
    return r == FAT_ENOENT ? es_vdir(x->c, x->p, (SceIoStat *)x->x) : r; }
static int t_chstat(void *v) { struct a_path *x = v; return fatfs_chstat(x->c, x->p, (const SceIoStat *)x->x, x->i); }
static int fatms_IoRemove(PspIoDrvFileArg *a, const char *n)            { return path_op(a, n, t_remove, 0, 0); }   /* -> FUN_00008a20 */
static int fatms_IoMkdir(PspIoDrvFileArg *a, const char *n, SceMode m)  { (void)m; return path_op(a, n, t_mkdir, 0, 0); }  /* -> 0x7dc0 */
static int fatms_IoRmdir(PspIoDrvFileArg *a, const char *n)             { return path_op(a, n, t_rmdir, 0, 0); }    /* -> FUN_00008bc0 */
static int fatms_IoGetstat(PspIoDrvFileArg *a, const char *f, SceIoStat *s)   /* -> FUN_000078f0 */
{
    if (!a || !s) return FAT_EINVAL;
    return path_op(a, f, t_getstat, s, 0);
}
static int fatms_IoChstat(PspIoDrvFileArg *a, const char *f, SceIoStat *s, int b)   /* -> 0x5c18 (no null checks, as Sony) */
{
    return path_op(a, f, t_chstat, s, b);
}

/* FUN_00008b68: sceIoRename only renames inside the source's directory (Sony copies the
   source's parent path over the destination's); moving needs devctl 0x2415830 */
static int t_rename(void *v) { struct a_path *x = v;
    fp_wcsncpy(x->p2->dir, x->p->dir, 0xff);
    return fatfs_move(x->c, x->p, x->p2); }
static int fatms_IoRename(PspIoDrvFileArg *a, const char *o, const char *n)
{
    if (a->fs_num != 0) return FAT_ENODEV;
    fatms_ctx *c = CTX(a);
    if (c->flags & ST_NOMEDIA) return FAT_ENXIO;
    if (!(c->flags & ST_ASSIGNED)) return FAT_ENODEV;
    struct a_path ap = { c, 0, 0, 0, 0 };
    int r = fatfs_path_get(c, o, &ap.p);
    if (r < 0) return r;
    if ((r = fatfs_path_get(c, n, &ap.p2)) >= 0) {
        if ((r = LOCK(c)) >= 0) {
            r = run_ext(0x1000, t_rename, &ap);
            if (r > 0) r = 0;
            UNLOCK(c);
        }
        fatfs_path_put(c, ap.p2);
    }
    fatfs_path_put(c, ap.p);
    return r;
}

static int t_dopen(void *v) { struct a_path *x = v; DirHandle *d = 0;
    int r = fatfs_dopen(x->c, x->p, &d), virt = (r == FAT_ENOENT);
    if (virt) r = es_dopen(x->c, x->p, &d);                              /* DratiniFS: an ES-only directory */
    if (r >= 0) { if (!virt) es_dir_init(x->c, d, x->p, 0); x->x = d; }
    return r; }
static int fatms_IoDopen(PspIoDrvFileArg *a, const char *n)                          /* 0x3a14 -> FUN_0000736c */
{
    if (a->fs_num != 0) return FAT_ENODEV;
    fatms_ctx *c = CTX(a);
    if (c->flags & ST_NOMEDIA) return FAT_ENXIO;
    if (!(c->flags & ST_ASSIGNED)) return FAT_ENODEV;
    char *tmp = (char *)fatfs_get_scratch(c);
    if (!tmp) return FAT_ENOMEM;
    int len = 0;
    while (len < 0x800 && n[len]) len++;                                 /* strnlen(n, 0x800) */
    char last = len ? n[len - 1] : 0;
    snprintf(tmp, 0x800, "%s%s", n, "\\dummy" + (last == '\\' || last == '/'));   /* resolve the dir as a parent */
    struct a_path ap = { c, 0, 0, 0, 0 };
    int r = fatfs_path_get(c, tmp, &ap.p);
    if (r >= 0) {
        if ((r = LOCK(c)) >= 0) {
            r = run_tm(tm_ext_dopen, t_dopen, &ap);
            if (r >= 0) { r = 0; a->arg = ap.x; }
            UNLOCK(c);
        }
        fatfs_path_put(c, ap.p);
    }
    fatfs_put_scratch(c, tmp);
    return r;
}
struct a_dir { fatms_ctx *c; DirHandle *d; SceIoDirent *de; void *priv; };
static int t_dclose(void *v) { struct a_dir *x = v; return fatfs_dclose(x->c, x->d); }
static int t_dread(void *v)  { struct a_dir *x = v;
    int r = es_dir_virtual(x->d) ? FAT_ENOENT : fatfs_dread(x->c, x->d, x->de, x->priv);
    return r == FAT_ENOENT ? es_dread(x->c, x->d, x->de, x->priv) : r; }   /* DratiniFS: ES overlay */
static int fatms_IoDclose(PspIoDrvFileArg *a)                                        /* 0x3bb4 -> FUN_00005f28 */
{
    if (!a) return FAT_EINVAL;
    if (a->fs_num != 0) return FAT_ENODEV;
    fatms_ctx *c = CTX(a);
    DirHandle *d = (DirHandle *)a->arg;
    if (c->flags & ST_NOMEDIA) return FAT_ENXIO;
    if (!d || d->is_dir == 0) return FAT_EINVAL;
    int r = LOCK(c);
    if (r < 0) return r;
    struct a_dir x = { c, d, 0, 0 };
    r = run_ext(0x1000, t_dclose, &x);
    UNLOCK(c);
    return r > 0 ? 0 : r;
}
static int fatms_IoDread(PspIoDrvFileArg *a, SceIoDirent *de)                        /* 0x3cb0 -> FUN_00007610 */
{
    if (!a || !de) return FAT_EINVAL;
    if (a->fs_num != 0) return FAT_ENODEV;
    void *priv = de->d_private;                      /* the caller's buffer: Sony fills it, never clears it */
    if ((int)(fat_k1() & (u32)priv) < 0) return FAT_EINVAL;
    fatms_ctx *c = CTX(a);
    DirHandle *d = (DirHandle *)a->arg;
    if (c->flags & ST_NOMEDIA) return FAT_ENXIO;
    if (!d || d->is_dir == 0) return FAT_EINVAL;
    int r = LOCK(c);
    if (r < 0) return r;
    struct a_dir x = { c, d, de, priv };
    r = run_ext(0x1000, t_dread, &x);
    r = (r >= 0) ? 1 : (r == FAT_ENOENT) ? 0 : r;                       /* 1 = entry, 0 = end */
    UNLOCK(c);
    return r;
}

/* An assign whose block device is an alias reaches IoMount/IoUmount with iob attr 0x100. On the PSP
   Go that is ms0: as internal storage (fatef0: through msemu's msstoremu0:), mounted on the fatms
   volume instead (fatfs_redirect). Elsewhere vol_emulated() is 0 (Sony's FUN_0000c8d4). */
static int fatms_IoMount(PspIoDrvFileArg *a, void *a2, const char *blockdev, unsigned flags)   /* 0x4158 */
{
    if (a->fs_num != 0) return FAT_ENODEV;
    fatms_ctx *c = CTX(a);
    if (vol_emulated() && (a->unk2 & 0x100) && !(c = fatfs_redirect())) return FAT_ENODEV;
    int r = LOCK(c);
    if (r < 0) return r;
    r = c->ops->mount(c, (const char *)a2, blockdev, flags);
    UNLOCK(c);
    return (r < 0) ? r : 0;
}
static int fatms_IoUmount(PspIoDrvFileArg *a, const char *b)                         /* 0x4268 -> vt14 */
{
    (void)b;
    if (a->fs_num != 0) return FAT_ENODEV;
    fatms_ctx *c = CTX(a);
    if (vol_emulated() && (a->unk2 & 0x100) && !(c = fatfs_redirect())) return FAT_ENODEV;
    if (c->flags & ST_NOMEDIA) return FAT_ENXIO;
    int r = LOCK(c);
    if (r < 0) return r;
    r = c->ops->umount(c);
    UNLOCK(c);
    return r > 0 ? 0 : r;
}

struct a_devctl { fatms_ctx *c; unsigned int cmd; void *in; int il; void *out; int ol; };
static int t_devctl(void *v) { struct a_devctl *x = v; return fatfs_devctl(x->c, x->cmd, x->in, x->il, x->out, x->ol); }
static int fatms_IoDevctl(PspIoDrvFileArg *a, const char *dev, unsigned int cmd, void *i, int il, void *o, int ol) /* 0x4344 -> FUN_00006c18 */
{
    (void)dev;
    if (a->fs_num != 0) return FAT_ENODEV;
    if (cmd == 0x00208813 || cmd == 0x00208814) return 0;   /* loadexec probe (ioctlAndDevctl 0x208813) */
    fatms_ctx *c = CTX(a);
    if (c->flags & ST_NOMEDIA) return FAT_ENXIO;
    switch (cmd) {                                          /* allowed while ms0: is unassigned */
    case 0x02415821: case 0x02415822: case 0x0240D81E: case 0x02415857: case 0x02415859:
    case 0x02425856: case 0x0241D813: case 0x02425823: case 0x02425858: case 0x0242585A: break;
    default: if (!(c->flags & ST_ASSIGNED)) return FAT_ENODEV;
    }
    int r = LOCK(c);
    if (r < 0) return r;
    struct a_devctl x = { c, cmd, i, il, o, ol };
    r = run_tm(tm_ext_devctl, t_devctl, &x);
    UNLOCK(c);
    return r > 0 ? 0 : r;
}

/* Sony's PspIoDrvFuncs @0xcf9c (IoChdir + IoUnk21 are NULL). */
static PspIoDrvFuncs fatms_funcs = {
    fatms_IoInit, fatms_IoExit, fatms_IoOpen, fatms_IoClose, fatms_IoRead,
    fatms_IoWrite, fatms_IoLseek, fatms_IoIoctl, fatms_IoRemove, fatms_IoMkdir,
    fatms_IoRmdir, fatms_IoDopen, fatms_IoDclose, fatms_IoDread, fatms_IoGetstat,
    fatms_IoChstat, fatms_IoRename, NULL /* IoChdir */,
    (void *)fatms_IoMount, (void *)fatms_IoUmount, fatms_IoDevctl, NULL /* IoUnk21 */
};
/* one driver per volume, all with these handlers (Sony 0xd074; fatmsef 0xf304 / 0xf54c / 0xf7c8) */
static PspIoDrv fatms_driver = { "fatms", 0xa0010, 0x800, "MS", &fatms_funcs };
#ifdef PSPGO
static PspIoDrv fatef_driver = { "fatef", 0xa0010, 0x800, "EF", &fatms_funcs };
static PspIoDrv fateh_driver = { "fateh", 0xa0010, 0x800, "EH", &fatms_funcs };
#endif

/* Sony ignores every result: a non-zero return would unload us with drivers live */
int module_start(SceSize args, void *argp)                                           /* 0xc8a8 / fatmsef 0xe6cc */
{
    (void)args; (void)argp;
#ifdef PSPGO
    fatfs_register(&fatef_driver, &g_ctx_ef);       /* internal storage first: these mount right away */
    fatfs_register(&fateh_driver, &g_ctx_eh);
    if (vol_emulated()) {                           /* a game from internal storage: ms0: is that storage */
        ef_alias_ms0();
        g_ctx.ops = &g_ef_ops;
    }
#endif
    fatfs_register(&fatms_driver, &g_ctx);
    return 0;
}
