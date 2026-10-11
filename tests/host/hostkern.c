/* hostkern.c - host stand-ins for everything fatms imports: ThreadMan (pthreads, so Sony's
 * media thread really runs), SysMem, iofilemgr below fatms (the card as msstor0: / msstor0p1:,
 * a fake msstor module, the "mscmhc0:" card), sceCodepage_driver (UTF-8 + a Shift-JIS subset: ASCII, kana, half-width
 * katakana) and sceRtc_driver (fixed clock 2026-10-10 12:34:56, so on-disk bytes are stable).
 * PSP Go (fatmsef): the internal flash as eflash0a0f0p0: / eflash0a0f1p0: (two image files), the
 * boot-medium / application-type knobs, IdStorage, MD5, and iofilemgr's assign table with aliases. */
#include "fatms.h"
#include "host.h"
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <psploadcore.h>

int module_start(SceSize args, void *argp);

u8  *host_disk;                                   /* sector 0 (the MBR or boot sector), kept current */
u64  host_disk_bytes;
static int disk_fd = -1;                          /* the image file itself: cards > 4 GB stay sparse */
volatile int host_wp, host_ms_status = 4, host_illegal_writes;
volatile u32 host_sdk = 0x06060110;
u32 host_k1;
volatile u32 host_rd_calls, host_rd_sectors, host_wr_calls, host_wr_sectors;
volatile SceUID host_last_cb;
volatile int host_last_cb_arg = -1;
struct host_cp host_cp;
PspSysEventHandler *host_sysev;
PspIoDrv *host_drv;                                /* the fatms driver */
void (*host_on_terminate)(const char *drvname);
static volatile SceUID host_evf = -1;

/* ---------------- interrupts / dispatch: one recursive lock ---------------- */
static pthread_mutex_t intr;
static pthread_once_t intr_once = PTHREAD_ONCE_INIT;
static void intr_init(void)
{
    pthread_mutexattr_t a;
    pthread_mutexattr_init(&a);
    pthread_mutexattr_settype(&a, PTHREAD_MUTEX_RECURSIVE);
    pthread_mutex_init(&intr, &a);
}
int  sceKernelCpuSuspendIntr(void) { pthread_once(&intr_once, intr_init); pthread_mutex_lock(&intr); return 1; }
void sceKernelCpuResumeIntr(int s) { (void)s; pthread_mutex_unlock(&intr); }
int  sceKernelSuspendDispatchThread(void) { return sceKernelCpuSuspendIntr(); }
int  sceKernelResumeDispatchThread(int s) { sceKernelCpuResumeIntr(s); return 0; }

/* ---------------- kernel objects ---------------- */
#define NOBJ 64
static struct obj {
    int kind;                                      /* 1 mutex, 2 evf, 3 thread, 4 fpl, 5 heap */
    pthread_mutex_t m;
    pthread_cond_t c;
    pthread_t owner;
    int count;                                     /* mutex depth */
    u32 bits;                                      /* event flag */
    int (*entry)(SceSize, void *);                 /* thread */
    void *block;                                   /* fpl */
} objs[NOBJ];
static pthread_mutex_t objs_lock = PTHREAD_MUTEX_INITIALIZER;

static SceUID obj_new(int kind)
{
    pthread_mutex_lock(&objs_lock);
    for (int i = 1; i < NOBJ; i++)
        if (!objs[i].kind) {
            memset(&objs[i], 0, sizeof objs[i]);
            objs[i].kind = kind;
            pthread_mutex_init(&objs[i].m, 0);
            pthread_cond_init(&objs[i].c, 0);
            pthread_mutex_unlock(&objs_lock);
            return 0x100 + i;
        }
    pthread_mutex_unlock(&objs_lock);
    fprintf(stderr, "hostkern: out of kernel objects\n");
    abort();
}
static struct obj *obj(SceUID id, int kind)
{
    int i = id - 0x100;
    if (i < 1 || i >= NOBJ || objs[i].kind != kind) { fprintf(stderr, "hostkern: bad uid 0x%x (kind %d)\n", id, kind); abort(); }
    return &objs[i];
}

SceUID sceKernelCreateMutex(const char *name, u32 attr, int init, void *opt)
{
    (void)name; (void)opt;
    if (!(attr & 0x200)) { fprintf(stderr, "hostkern: fatms mutex must be recursive (attr 0x%x)\n", attr); abort(); }
    SceUID id = obj_new(1);
    if (init) sceKernelLockMutex(id, init, 0);
    return id;
}
int sceKernelLockMutex(SceUID id, int n, unsigned int *timeout)
{
    (void)timeout;
    struct obj *o = obj(id, 1);
    pthread_mutex_lock(&o->m);
    while (o->count > 0 && !pthread_equal(o->owner, pthread_self())) pthread_cond_wait(&o->c, &o->m);
    o->owner = pthread_self();
    o->count += n;
    pthread_mutex_unlock(&o->m);
    return 0;
}
int sceKernelUnlockMutex(SceUID id, int n)
{
    struct obj *o = obj(id, 1);
    pthread_mutex_lock(&o->m);
    if (o->count < n || !pthread_equal(o->owner, pthread_self())) {
        pthread_mutex_unlock(&o->m);
        fprintf(stderr, "hostkern: unlock of a mutex this thread doesn't hold\n");
        abort();
    }
    o->count -= n;
    if (!o->count) pthread_cond_broadcast(&o->c);
    pthread_mutex_unlock(&o->m);
    return 0;
}
int sceKernelDeleteMutex(SceUID id) { obj(id, 1)->kind = 0; return 0; }
int host_mutex_count(SceUID id) { return obj(id, 1)->count; }

SceUID sceKernelCreateEventFlag(const char *name, int attr, int bits, void *opt)
{
    (void)name; (void)attr; (void)opt;
    SceUID id = obj_new(2);
    obj(id, 2)->bits = (u32)bits;
    return id;
}
/* On the PSP a set that readies the (higher-priority) media thread switches to it at once, so two
   back-to-back sets (fatfs_rescan: eject, then insert) reach it as two wake-ups, in order. pthreads
   would merge them into one, so for an OR+CLEAR waiter each set is queued and delivered on its own. */
#define EVQ 8
static struct { SceUID id; u32 q[EVQ]; int n; } evq[4];
static u32 *evq_of(SceUID id, int **n)
{
    for (int i = 0; i < 4; i++) if (evq[i].id == id || !evq[i].id) { evq[i].id = id; *n = &evq[i].n; return evq[i].q; }
    abort();
}
int sceKernelSetEventFlag(SceUID id, u32 bits)
{
    if (id <= 0) return (int)0x800201a9;           /* no flag (the Go's storage has no media thread) */
    struct obj *o = obj(id, 2);
    int *n;
    pthread_mutex_lock(&o->m);
    u32 *q = evq_of(id, &n);
    if (*n < EVQ) q[(*n)++] = bits;
    o->bits |= bits;
    pthread_cond_broadcast(&o->c);
    pthread_mutex_unlock(&o->m);
    return 0;
}
int sceKernelWaitEventFlag(SceUID id, u32 bits, u32 wait, u32 *out, SceUInt *timeout)
{
    (void)timeout;
    struct obj *o = obj(id, 2);
    int *n;
    pthread_mutex_lock(&o->m);
    u32 *q = evq_of(id, &n);
    while ((wait & 1) ? !(o->bits & bits) : (o->bits & bits) != bits) pthread_cond_wait(&o->c, &o->m);
    u32 got = o->bits;
    if ((wait & 0x21) == 0x21 && *n > 0) {        /* OR + CLEAR: one set at a time */
        got = q[0] & bits;
        memmove(q, q + 1, (size_t)(--*n) * sizeof *q);
        o->bits = 0;
        for (int i = 0; i < *n; i++) o->bits |= q[i];
    } else if (wait & 0x20) { o->bits &= ~bits; *n = 0; }   /* PSP_EVENT_WAITCLEAR */
    if (out) *out = got;
    pthread_mutex_unlock(&o->m);
    return 0;
}

struct thread_start { int (*entry)(SceSize, void *); SceSize len; u8 arg[64]; };
static void *thread_main(void *p)
{
    struct thread_start *t = p;
    t->entry(t->len, t->arg);
    return 0;
}
SceUID sceKernelCreateThread(const char *name, int (*entry)(SceSize, void *), int prio, int stack, SceUInt attr, void *opt)
{
    (void)name; (void)prio; (void)stack; (void)attr; (void)opt;
    SceUID id = obj_new(3);
    obj(id, 3)->entry = entry;
    return id;
}
int sceKernelStartThread(SceUID id, SceSize len, void *argp)
{
    struct thread_start *t = calloc(1, sizeof *t);
    pthread_t th;
    t->entry = obj(id, 3)->entry;
    t->len = len;
    memcpy(t->arg, argp, len < sizeof t->arg ? len : sizeof t->arg);
    if (pthread_create(&th, 0, thread_main, t)) return -1;
    pthread_detach(th);
    return 0;
}
int sceKernelDeleteThread(SceUID id) { (void)id; return 0; }

SceUID sceKernelCreateFpl(const char *name, int part, int attr, unsigned int size, unsigned int blocks, void *opt)
{
    (void)part; (void)attr; (void)blocks; (void)opt;
    SceUID id = obj_new(4);
    void *p = 0;
    if (posix_memalign(&p, 64, size)) abort();
    memset(p, 0xa5, size);                         /* garbage, as fresh kernel memory is */
    obj(id, 4)->block = p;
    if (!strcmp(name, "SceFatfsCPTable")) { host_cp.block = p; host_cp.block_size = size; }
    return id;
}
int sceKernelTryAllocateFpl(SceUID id, void **data) { *data = obj(id, 4)->block; return 0; }
int sceKernelDeleteFpl(SceUID id) { struct obj *o = obj(id, 4); free(o->block); o->kind = 0; return 0; }

/* heap blocks are 8 mod 64 (word aligned, not DMA aligned) so the block layer's bounce paths run */
SceUID sceKernelCreateHeap(SceUID part, SceSize size, int attr, const char *name)
{
    (void)part; (void)size; (void)attr; (void)name;
    return obj_new(5);
}
void *sceKernelAllocHeapMemory(SceUID heap, SceSize size)
{
    (void)heap;
    u8 *p = 0;
    if (posix_memalign((void **)&p, 64, size + 8)) return 0;
    *(void **)p = p;
    memset(p + 8, 0x5a, size);
    return p + 8;
}
int sceKernelFreeHeapMemory(SceUID heap, void *b) { (void)heap; free(*(void **)((u8 *)b - 8)); return 0; }

void sceKernelDcacheWritebackInvalidateRange(const void *p, unsigned int n) { (void)p; (void)n; }
int sceKernelCheckThreadKernelStack(void)          /* alternate: half the calls take the extend path */
{
    static volatile u32 n;
    return (n++ & 1) ? 0 : 0x4000;
}
int sceKernelExtendKernelStack(int size, void (*cb)(void *), void *arg)
{
    (void)size;
    return ((int (*)(void *))(void (*)(void))cb)(arg);
}
typedef int (*worker_t)(void *);
int tm_ext_open(int size, worker_t w, void *a)   { return sceKernelExtendKernelStack(size, (void (*)(void *))(void (*)(void))w, a); }
int tm_ext_dopen(int size, worker_t w, void *a)  { return sceKernelExtendKernelStack(size, (void (*)(void *))(void (*)(void))w, a); }
int tm_ext_devctl(int size, worker_t w, void *a) { return sceKernelExtendKernelStack(size, (void (*)(void *))(void (*)(void))w, a); }

int sceKernelNotifyCallback(SceUID cb, int arg) { host_last_cb = cb; host_last_cb_arg = arg; return 0; }
int sceKernelRegisterSysEventHandler(PspSysEventHandler *h) { host_sysev = h; return 0; }
int sceKernelGetCompiledSdkVersion(void) { return (int)host_sdk; }
int sceKernelGetUIDcontrolBlock(SceUID uid, SceUidControlBlock **blk)
{
    static SceUidControlBlock b = { .raw = { [0x16] = 2 } };   /* attr: user-accessible */
    if (uid <= 0) return -1;
    *blk = &b;
    return 0;
}

/* ---------------- the card below fatms: msstor0: + msstor0p1: ----------------
 * As Sony's msstor (research/msstor_661_pseudocode): msstor0p1: starts at MBR entry 0's LBA when
 * sector 0 is an MBR (55 AA, entry 0 status 0x80, type != 0; FUN_00003dbc), else at 0 (superfloppy).
 * A read/write whose end passes the capacity fails with 0x80220183 (FUN_00000d2c / 0dd4); READ
 * CAPACITY (ioctl 0x2125006) returns it (msstor0p1: minus the partition start). The capacity comes
 * through a fake msstor module mapped at kernel addresses: its getter at text+0x3334 dispatches to
 * dev+0x4E60, which every card setup (load, insert, devctl 0x210d816) resets to the adapter's
 * attribute capacity -- unless something has patched the getter to return its own value. */
#define MSP_FD 0x10                                     /* msstor0p1: */
#define MS0_FD 0x11                                     /* msstor0: */
#define MSTEXT 0x88000000u                              /* fake sceMSstor_Driver text */
#define MSDATA 0x8800f300u                              /* its data segment (vaddr 0xf300) */
#define MSDEV  0x88016000u                              /* the dev struct DAT_00014394 points at */
static u64 pos_p, pos_0;
u32 host_part_lba, host_attr_cap;
int host_ms0_opens;                                     /* msstor0: handles currently open */
u32 *host_ms_text;                                      /* fake msstor text (tests poke it) */
static SceModule ms_mod = { "sceMSstor_Driver", MSTEXT };

static void ms_module_init(void)
{
    if (host_ms_text) return;
    void *m = mmap((void *)MSTEXT, 0x20000, PROT_READ | PROT_WRITE, MAP_FIXED | MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (m != (void *)MSTEXT) { fprintf(stderr, "can't map the fake msstor module\n"); exit(2); }
    u32 *t = host_ms_text = (u32 *)MSTEXT;
    u32 vt = MSDATA + 0x164, dp = MSDATA + 0x5094;      /* PTR_PTR_0000f464, DAT_00014394 */
    static const u32 code[12] = { 0x27bdfff0, 0xafbf0000, 0, 0, 0, 0, 0x8c620010, 0x0040f809, 0, 0x8fbf0000, 0x03e00008, 0x27bd0010 };
    memcpy(t + 0x3334 / 4, code, sizeof code);
    t[0x333c / 4] = 0x3c020000 | ((vt + 0x8000) >> 16); t[0x3340 / 4] = 0x8c430000 | (vt & 0xffff);   /* relocated */
    t[0x3344 / 4] = 0x3c020000 | ((dp + 0x8000) >> 16); t[0x3348 / 4] = 0x8c440000 | (dp & 0xffff);
    t[0x4554 / 4] = 0x03e00008; t[0x4558 / 4] = 0x8c824e60;    /* jr ra; lw v0, 0x4e60(a0) */
    *(u32 *)dp = MSDEV;
}
SceModule *sceKernelFindModuleByName(const char *name) { return strcmp(name, ms_mod.modname) ? 0 : &ms_mod; }
void sceKernelIcacheInvalidateRange(const void *p, unsigned int size) { (void)p; (void)size; }

/* the capacity getter (text+0x3334): Sony's dispatches to dev+0x4E60; a patched one is decoded */
u32 host_ms_capacity(void)
{
    u32 *t = host_ms_text + 0x3334 / 4;
    if ((t[0] >> 16) == 0x3c02 && (t[1] >> 16) == 0x8c42 && t[2] == 0x03e00008u)   /* lui v0; lw v0; jr ra */
        return *(u32 *)((t[0] << 16) + (u32)(s32)(s16)t[1]);
    return *(u32 *)(MSDEV + 0x4e60);
}
/* msstor card setup: attribute capacity + partition start (FUN_00003d08 + FUN_00003dbc) */
void host_ms_setup(void)
{
    ms_module_init();
    *(u32 *)(MSDEV + 0x4e60) = host_attr_cap;
    const u8 *m = host_disk;
    host_part_lba = 0;
    if (host_disk_bytes >= 512 && m[510] == 0x55 && m[511] == 0xaa && m[0x1be] == 0x80 && m[0x1c2] != 0)
        host_part_lba = (u32)(m[0x1c6] | m[0x1c7] << 8 | m[0x1c8] << 16 | (u32)m[0x1c9] << 24);
}

/* the card is the image file, used in place (the scripts hand over copies) */
int host_disk_load(const char *path)
{
    /* The goldens count read calls, which depend on the test buffers' alignment, which depends on the
       allocator's state: keep this exact sequence (fopen, a disk-sized malloc while it is open, fclose)
       from when the whole card lived in RAM, so the golden numbers stay comparable to the baseline's. */
    FILE *f = fopen(path, "rb");
    if (!f) return -1;
    fseeko(f, 0, SEEK_END);
    host_disk_bytes = (u64)ftello(f);
    fseeko(f, 0, SEEK_SET);
    host_disk = malloc(host_disk_bytes < (1u << 30) ? (size_t)host_disk_bytes : 512);
    int ok = host_disk && fread(host_disk, 1, 512, f) == 512;
    fclose(f);
    disk_fd = open(path, O_RDWR);
    if (!ok || disk_fd < 0) return -1;
    if (!host_attr_cap) host_attr_cap = host_disk_bytes / 512 > 0xffffffffu ? 0xffffffffu : (u32)(host_disk_bytes / 512);
    host_ms_setup();
    return 0;
}
int host_disk_save(const char *path)                 /* a copy of the card as it is now */
{
    static u8 buf[1 << 16];
    int o = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (o < 0) return -1;
    int ok = ftruncate(o, (off_t)host_disk_bytes) == 0;
    for (u64 off = 0; ok && off < host_disk_bytes; off += sizeof buf) {
        ssize_t n = pread(disk_fd, buf, sizeof buf, (off_t)off);
        if (n <= 0) { ok = n == 0; break; }
        int zero = 1;
        for (ssize_t i = 0; i < n && zero; i++) zero = !buf[i];
        if (!zero && pwrite(o, buf, (size_t)n, (off_t)off) != n) ok = 0;   /* keep it sparse */
    }
    return (close(o) == 0 && ok) ? 0 : -1;
}

/* ---------------- iofilemgr: drivers, assigns, aliases ----------------
 * As 6.61 iofilemgr (uofw sceIoAssign/sceIoUnassign/IoFileMgrForKernel_E5323C5B): unless the mode
 * has bit 31, an assign resolves its fs and block device through the alias table, and a block-device
 * alias puts 0x100 in the mount iob's attr (iob+0xc, which the Go's IoMount looks at). Unassign
 * calls the driver's umount and fails (keeping the assign) when that fails. */
#define NDRV 4
static struct hdrv { PspIoDrv *drv; PspIoDrvArg arg; } drvs[NDRV];
static struct asg { char dev[16], blk[32]; struct hdrv *d; u32 attr; } asgs[8];
static struct { char name[16], dev[32]; } aliases[8];
int host_msemu = 1;                                         /* msemu.prx loaded (mscmhcemu0:, msstoremu0:) */
static void devname(char *o, const char *p)                 /* "ms0:/x" -> "ms0" */
{
    int n = 0;
    while (p[n] && p[n] != ':' && n < 15) { o[n] = p[n]; n++; }
    o[n] = 0;
}
static struct hdrv *drv_named(const char *dev)              /* "fatef0" / "fatef" -> the fatef driver */
{
    char n[16];
    devname(n, dev);
    for (int k = (int)strlen(n); k > 0 && n[k - 1] >= '0' && n[k - 1] <= '9'; ) n[--k] = 0;
    for (int i = 0; i < NDRV; i++) if (drvs[i].drv && !strcmp(drvs[i].drv->name, n)) return &drvs[i];
    return 0;
}
static const char *alias_of(const char *dev)
{
    char n[16];
    devname(n, dev);
    for (int i = 0; i < 8; i++) if (aliases[i].name[0] && !strcmp(aliases[i].name, n)) return aliases[i].dev;
    return 0;
}
PspIoDrvArg *host_devarg(const char *path)                  /* the driver a path's device reaches */
{
    char n[16];
    devname(n, path);
    for (int i = 0; i < 8; i++) if (asgs[i].d && !strcmp(asgs[i].dev, n)) return &asgs[i].d->arg;
    const char *a = alias_of(path);
    struct hdrv *d = drv_named(a ? a : path);
    if (!d) d = drv_named("fatms");
    return &d->arg;
}
int k_sceIoAddDrv(PspIoDrv *drv)
{
    struct hdrv *d = 0;
    for (int i = 0; i < NDRV && !d; i++) if (!drvs[i].drv) d = &drvs[i];
    if (!d) return (int)0x80020320;
    d->drv = drv;
    d->arg.drv = drv;
    if (!strcmp(drv->name, "fatms")) host_drv = drv;
    return drv->funcs->IoInit(&d->arg);
}
int k_sceIoAddAlias(const char *name, const char *dev)
{
    char n[16], t[16];
    devname(n, name); devname(t, dev);
    for (int k = (int)strlen(t); k > 0 && t[k - 1] >= '0' && t[k - 1] <= '9'; ) t[--k] = 0;
    if (alias_of(name)) return (int)0x80020322;
    if (!drv_named(dev) && !(host_msemu && (!strcmp(t, "mscmhcemu") || !strcmp(t, "msstoremu")))) return (int)0x80020321;
    for (int i = 0; i < 8; i++)
        if (!aliases[i].name[0]) { strcpy(aliases[i].name, n); snprintf(aliases[i].dev, 32, "%s", dev); return 0; }
    return (int)0x80020320;
}
int k_sceIoAssign(const char *dev, const char *blk, const char *fs, int mode, void *unk1, int unk2)
{
    (void)unk1; (void)unk2;
    const char *b = blk, *f = fs, *a;
    u32 attr = 0x10;
    char n[16];
    devname(n, dev);
    if (!((u32)mode & 0x80000000u)) {
        if ((a = alias_of(fs))) f = a;
        if ((a = alias_of(blk))) { b = a; attr |= 0x100; }
    }
    struct hdrv *d = drv_named(f);
    if (!d) return (int)0x80020321;
    for (int i = 0; i < 8; i++) if (asgs[i].d && !strcmp(asgs[i].dev, n)) return (int)0x80020322;
    PspIoDrvFileArg iob = { .drv = &d->arg, .unk2 = attr };
    int r = ((int (*)(PspIoDrvFileArg *, void *, const char *, unsigned))(void (*)(void))d->drv->funcs->IoMount)
            (&iob, (void *)f, b, (unsigned)mode);
    if (r < 0) return r;
    for (int i = 0; i < 8; i++)
        if (!asgs[i].d) { strcpy(asgs[i].dev, n); snprintf(asgs[i].blk, 32, "%s", b); asgs[i].attr = attr; asgs[i].d = d; break; }
    if (!strcmp(n, "ms0")) host_mounts++;
    return 0;
}
int k_sceIoUnassign(const char *dev)
{
    char n[16];
    devname(n, dev);
    for (int i = 0; i < 8; i++) {
        if (!asgs[i].d || strcmp(asgs[i].dev, n)) continue;
        PspIoDrvFileArg iob = { .drv = &asgs[i].d->arg, .unk2 = asgs[i].attr };
        int r = ((int (*)(PspIoDrvFileArg *, const char *))(void (*)(void))asgs[i].d->drv->funcs->IoUmount)(&iob, asgs[i].blk);
        if (r < 0) return r;
        asgs[i].d = 0;
        return 0;
    }
    return (int)0x80020321;
}
int host_assigned(const char *dev)                          /* 1 if `dev` is assigned, to which driver: name in *drv */
{
    char n[16];
    devname(n, dev);
    for (int i = 0; i < 8; i++) if (asgs[i].d && !strcmp(asgs[i].dev, n)) return 1;
    return 0;
}
const char *host_assigned_drv(const char *dev)
{
    char n[16];
    devname(n, dev);
    for (int i = 0; i < 8; i++) if (asgs[i].d && !strcmp(asgs[i].dev, n)) return asgs[i].d->drv->name;
    return 0;
}

/* ---------------- PSP Go: the internal flash ----------------
 * eflash0a0f0p0: (ef0:'s storage) and eflash0a0f1p0: (eh0:'s), each an image file used in place */
#define EF_FD 0x20
static struct { int fd; u64 bytes, pos; int opens; } efl[2] = { { -1, 0, 0, 0 }, { -1, 0, 0, 0 } };
int host_ef_load(int i, const char *path)
{
    if ((efl[i].fd = open(path, O_RDWR)) < 0) return -1;
    efl[i].bytes = (u64)lseek(efl[i].fd, 0, SEEK_END);
    return 0;
}
int host_ef_opens(int i) { return efl[i].opens; }
static int ef_of(SceUID fd) { return (fd == EF_FD || fd == EF_FD + 1) && efl[fd - EF_FD].fd >= 0 ? (int)(fd - EF_FD) : -1; }
static int ef_named(const char *name) { return !strcmp(name, "eflash0a0f0p0:") ? 0 : !strcmp(name, "eflash0a0f1p0:") ? 1 : -1; }
int host_bootfrom = 0x40, host_apptype = 0x100, host_apitype = 0x200, host_model = 4;
u8  host_idstorage[0x200];
int host_idstorage_ok;
int sceKernelBootFrom(void) { return host_bootfrom; }
int sceKernelApplicationType(void) { return host_apptype; }
int sceKernelInitApitype(void) { return host_apitype; }
int sceKernelGetModel(void) { return host_model; }
int sceEFlash_driver_8FB8591D(int timeout) { (void)timeout; return 0; }
int sceIdStorageLookup(u16 key, u32 off, void *buf, u32 len)
{
    if (!host_idstorage_ok || key != 0x48 || off + len > sizeof host_idstorage) return (int)0x80000025;
    memcpy(buf, host_idstorage + off, len);
    return 0;
}
/* MD5 (RFC 1321) */
static const u32 md5_k[64] = {
    0xd76aa478, 0xe8c7b756, 0x242070db, 0xc1bdceee, 0xf57c0faf, 0x4787c62a, 0xa8304613, 0xfd469501,
    0x698098d8, 0x8b44f7af, 0xffff5bb1, 0x895cd7be, 0x6b901122, 0xfd987193, 0xa679438e, 0x49b40821,
    0xf61e2562, 0xc040b340, 0x265e5a51, 0xe9b6c7aa, 0xd62f105d, 0x02441453, 0xd8a1e681, 0xe7d3fbc8,
    0x21e1cde6, 0xc33707d6, 0xf4d50d87, 0x455a14ed, 0xa9e3e905, 0xfcefa3f8, 0x676f02d9, 0x8d2a4c8a,
    0xfffa3942, 0x8771f681, 0x6d9d6122, 0xfde5380c, 0xa4beea44, 0x4bdecfa9, 0xf6bb4b60, 0xbebfbc70,
    0x289b7ec6, 0xeaa127fa, 0xd4ef3085, 0x04881d05, 0xd9d4d039, 0xe6db99e5, 0x1fa27cf8, 0xc4ac5665,
    0xf4292244, 0x432aff97, 0xab9423a7, 0xfc93a039, 0x655b59c3, 0x8f0ccc92, 0xffeff47d, 0x85845dd1,
    0x6fa87e4f, 0xfe2ce6e0, 0xa3014314, 0x4e0811a1, 0xf7537e82, 0xbd3af235, 0x2ad7d2bb, 0xeb86d391 };
static const u8 md5_r[64] = { 7, 12, 17, 22, 7, 12, 17, 22, 7, 12, 17, 22, 7, 12, 17, 22, 5, 9, 14, 20, 5, 9, 14, 20,
    5, 9, 14, 20, 5, 9, 14, 20, 4, 11, 16, 23, 4, 11, 16, 23, 4, 11, 16, 23, 4, 11, 16, 23,
    6, 10, 15, 21, 6, 10, 15, 21, 6, 10, 15, 21, 6, 10, 15, 21 };
int sceKernelUtilsMd5Digest(u8 *data, u32 size, u8 *digest)
{
    u32 h[4] = { 0x67452301, 0xefcdab89, 0x98badcfe, 0x10325476 };
    u64 total = size + 9, nblk = (total + 63) / 64;
    u8 blk[64];
    for (u64 b = 0; b < nblk; b++) {
        for (u32 i = 0; i < 64; i++) {
            u64 at = b * 64 + i;
            blk[i] = at < size ? data[at] : at == size ? 0x80 : 0;
        }
        if (b == nblk - 1) for (int i = 0; i < 8; i++) blk[56 + i] = (u8)(((u64)size * 8) >> (8 * i));
        u32 w[16], a = h[0], bb = h[1], c = h[2], d = h[3];
        for (int i = 0; i < 16; i++) w[i] = (u32)(blk[4 * i] | blk[4 * i + 1] << 8 | blk[4 * i + 2] << 16 | (u32)blk[4 * i + 3] << 24);
        for (int i = 0; i < 64; i++) {
            u32 f, g;
            if (i < 16) { f = (bb & c) | (~bb & d); g = (u32)i; }
            else if (i < 32) { f = (d & bb) | (~d & c); g = (5u * (u32)i + 1) % 16; }
            else if (i < 48) { f = bb ^ c ^ d; g = (3u * (u32)i + 5) % 16; }
            else { f = c ^ (bb | ~d); g = (7u * (u32)i) % 16; }
            u32 t = d; d = c; c = bb;
            u32 x = a + f + md5_k[i] + w[g];
            bb = bb + (x << md5_r[i] | x >> (32 - md5_r[i]));
            a = t;
        }
        h[0] += a; h[1] += bb; h[2] += c; h[3] += d;
    }
    for (int i = 0; i < 16; i++) digest[i] = (u8)(h[i / 4] >> (8 * (i % 4)));
    return 0;
}

SceUID k_sceIoOpen(const char *name, int flags, SceMode mode)
{
    (void)flags; (void)mode;
    int e = ef_named(name);
    if (e >= 0) { if (efl[e].fd < 0) return (SceUID)0x80010002; efl[e].opens++; efl[e].pos = 0; return EF_FD + e; }
    if (!host_disk) return (SceUID)0x80010002;
    if (!strcmp(name, "msstor0p1:")) return MSP_FD;
    if (!strcmp(name, "msstor0:")) { host_ms0_opens++; return MS0_FD; }
    return (SceUID)0x80010002;
}
int k_sceIoClose(SceUID fd)
{
    int e = ef_of(fd);
    if (e >= 0) { efl[e].opens--; return 0; }
    if (fd == MS0_FD) { host_ms0_opens--; return 0; }
    return fd == MSP_FD ? 0 : (int)0x80020323;
}
SceOff k_sceIoLseek64k(SceUID fd, SceOff ofs, int whence)
{
    int e = ef_of(fd);
    if (e >= 0) {
        if (whence != 0 || ofs < 0 || (u64)ofs > efl[e].bytes) return (SceOff)(int)0x80010016;
        efl[e].pos = (u64)ofs;
        return ofs;
    }
    if ((fd != MSP_FD && fd != MS0_FD) || whence != 0 || ofs < 0 || ofs > (SceOff)host_disk_bytes) return (SceOff)(int)0x80010016;
    *(fd == MSP_FD ? &pos_p : &pos_0) = (u64)ofs;
    return ofs;
}
/* the transfer's position, or 0 with *err set (msstor's capacity check, then the image's end) */
static u64 *ms_pos(SceUID fd, int n, u64 *disk_off, int *err)
{
    *err = (int)0x80010016;
    if ((fd != MSP_FD && fd != MS0_FD) || n < 0 || (n & 511)) return 0;
    u64 *pos = fd == MSP_FD ? &pos_p : &pos_0;
    u32 base = fd == MSP_FD ? host_part_lba : 0;
    u64 cap = host_ms_capacity(), end = (*pos + (u32)n) >> 9;
    if (cap < base || end > cap - base) { *err = (int)0x80220183; return 0; }
    *disk_off = (u64)base * 512 + *pos;
    if (*disk_off + (u32)n > host_disk_bytes) return 0;
    return pos;
}
static int ef_io(int e, void *buf, int n, int write)
{
    if (n < 0 || (n & 511) || efl[e].pos + (u32)n > efl[e].bytes) return (int)0x80010016;
    ssize_t r = write ? pwrite(efl[e].fd, buf, (size_t)n, (off_t)efl[e].pos) : pread(efl[e].fd, buf, (size_t)n, (off_t)efl[e].pos);
    if (r != n) return (int)0x80010016;
    efl[e].pos += (u32)n;
    return n;
}
int k_sceIoRead(SceUID fd, void *buf, int n)
{
    if (ef_of(fd) >= 0) return ef_io(ef_of(fd), buf, n, 0);
    u64 off; int err;
    u64 *pos = ms_pos(fd, n, &off, &err);
    if (!pos) return err;
    if (pread(disk_fd, buf, (size_t)n, (off_t)off) != n) return (int)0x80010016;
    *pos += (u32)n;
    host_rd_calls++;
    host_rd_sectors += (u32)n / 512;
    return n;
}
int k_sceIoWrite(SceUID fd, const void *buf, int n)
{
    if (ef_of(fd) >= 0) return ef_io(ef_of(fd), (void *)buf, n, 1);
    u64 off; int err;
    u64 *pos = ms_pos(fd, n, &off, &err);
    if (!pos) return err;
    if (host_wp) { host_illegal_writes++; return (int)0x8001000d; }
    if (pwrite(disk_fd, buf, (size_t)n, (off_t)off) != n) return (int)0x80010016;
    if (off < 512) memcpy(host_disk + off, buf, (u32)(n < 512 - (int)off ? n : 512 - (int)off));
    *pos += (u32)n;
    host_wr_calls++;
    host_wr_sectors += (u32)n / 512;
    return n;
}
static void be16(u8 *p, u32 v) { p[0] = (u8)(v >> 8); p[1] = (u8)v; }
int k_sceIoIoctl(SceUID fd, unsigned int cmd, void *in, int il, void *out, int ol)
{
    (void)in; (void)il;
    if (fd != MSP_FD && fd != MS0_FD) return (int)0x80020323;
    switch (cmd) {
    case 0x2125001: *(int *)out = 1; return 0;                    /* media present */
    case 0x2125006:                                               /* READ CAPACITY (sectors) */
        if (ol != 4 || !out) return (int)0x80220183;
        *(u32 *)out = host_ms_capacity() - (fd == MSP_FD ? host_part_lba : 0);
        return 0;
    case 0x2125803:                                               /* MS attributes: 16 KB erase block */
        memset(out, 0, 0x60);
        ((u8 *)out)[0] = 2;
        be16((u8 *)out + 0x02, 0x20);
        be16((u8 *)out + 0x2a, 0x20);
        be16((u8 *)out + 0x2c, 0x200);
        return 0;
    case 0x2125008: *(int *)out = host_wp ? 0 : 1; return 0;      /* writable */
    case 0x2125009: *(int *)out = 0; return 0;                    /* WP switch */
    }
    return (int)0x80020325;
}
int k_sceIoDevctl(const char *dev, unsigned int cmd, void *in, int il, void *out, int ol)
{
    (void)il; (void)ol;
    int e = ef_named(dev);
    if (e >= 0 && cmd == 0x602a102 && out && ol >= 8 && efl[e].fd >= 0) { *(u64 *)out = efl[e].bytes / 512; return 0; }
    if (!strcmp(dev, "mscmhc0:")) {
        if (cmd == 0x203d802) { host_evf = *(SceUID *)in; return 0; }   /* arm insert/eject events */
        if (cmd == 0x2025801) { *(int *)out = host_ms_status; return 0; }
        if (cmd == 0x2025806) { *(int *)out = 1; return 0; }
    }
    if (!strcmp(dev, "msstor0:") && cmd == 0x211d814) return 0;
    if (!strcmp(dev, "msstor0:") && cmd == 0x210d816) { host_ms_setup(); return 0; }   /* msstor re-runs card setup */
    return (int)0x80020325;
}
int k_sceIoTerminateFd(const char *name) { if (host_on_terminate) host_on_terminate(name); return 0; }

/* ---------------- sceCodepage_driver ---------------- */
static int cp_sizes[4] = { 0, 0, 0x1000, 0x2000 };
static int codepage = 0xd;
int sceCodepage_driver_EE932176(void) { return 0; }
int sceCodepage_driver_1D0DE569(int *a, int *b, int *c, int *d) { *a = cp_sizes[0]; *b = cp_sizes[1]; *c = cp_sizes[2]; *d = cp_sizes[3]; return 0; }
int sceCodepage_driver_039BF9E9(void *t0, int s0, void *t1, int s1, void *t2, int s2, void *t3, int s3)
{
    host_cp.t[0] = t0; host_cp.t[1] = t1; host_cp.t[2] = t2; host_cp.t[3] = t3;
    host_cp.s[0] = s0; host_cp.s[1] = s1; host_cp.s[2] = s2; host_cp.s[3] = s3;
    memcpy(host_cp.sizes, cp_sizes, sizeof cp_sizes);
    host_cp.calls++;
    /* the driver fills its tables: every pointer must lie inside the CPTable block */
    for (int i = 0; i < 4; i++) {
        u8 *p = host_cp.t[i], *b = host_cp.block;
        if (!b || p < b || p + host_cp.s[i] > b + host_cp.block_size) return (int)0x80000023;
        memset(p, 0, (u32)host_cp.s[i]);
    }
    return 0;
}
int sceCodepage_driver_B0AE63AA(int cp) { codepage = cp; return 1; }
int sceCodepage_driver_0AA54D6D(int *out) { *out = codepage; return 1; }
char sceCodepage_driver_DCD95405(int ch) { return (char)((ch >= 'a' && ch <= 'z') ? ch - 0x20 : ch); }
int sceCodepage_driver_11123ED1(int ch) { return ch >= 0x80 && !(ch >= 0xff61 && ch <= 0xff9f); }
int sceCodepage_driver_014E0C72(const void *p) { u8 b = *(const u8 *)p; return (b >= 0x81 && b <= 0x9f) || (b >= 0xe0 && b <= 0xfc); }

/* Shift-JIS subset <-> UCS-2: hiragana 829F-82F1, katakana 8340-837E / 8380-8396, half-width A1-DF */
static u16 sjis_dec(const u8 **s)
{
    u8 a = **s;
    if (a < 0x80) { (*s)++; return a; }
    if (a >= 0xa1 && a <= 0xdf) { (*s)++; return (u16)(0xff61 + a - 0xa1); }
    u8 b = (*s)[1];
    if (!b) { (*s)++; return '?'; }
    *s += 2;
    u16 c = (u16)(a << 8 | b);
    if (c >= 0x829f && c <= 0x82f1) return (u16)(0x3041 + c - 0x829f);
    if (c >= 0x8340 && c <= 0x837e) return (u16)(0x30a1 + c - 0x8340);
    if (c >= 0x8380 && c <= 0x8396) return (u16)(0x30e0 + c - 0x8380);
    return 0x30fb;                                         /* outside the subset */
}
static int sjis_enc(u16 u, u8 *o)
{
    u16 c;
    if (u < 0x80) { o[0] = (u8)u; return 1; }
    if (u >= 0xff61 && u <= 0xff9f) { o[0] = (u8)(0xa1 + u - 0xff61); return 1; }
    if (u >= 0x3041 && u <= 0x3093) c = (u16)(0x829f + u - 0x3041);
    else if (u >= 0x30a1 && u <= 0x30df) c = (u16)(0x8340 + u - 0x30a1);
    else if (u >= 0x30e0 && u <= 0x30f6) c = (u16)(0x8380 + u - 0x30e0);
    else { o[0] = '?'; return 1; }
    o[0] = (u8)(c >> 8); o[1] = (u8)c;
    return 2;
}
int sceCodepage_driver_855C5C2E(u16 *d, int cap, const char *src)        /* SJIS -> UCS-2 */
{
    const u8 *s = (const u8 *)src;
    int n = 0;
    while (*s && (n + 2) * 2 <= cap) d[n++] = sjis_dec(&s);
    d[n] = 0;
    return n;
}
int sceCodepage_driver_47BDF633(char *d, int cap, const u16 *s)          /* UCS-2 -> SJIS */
{
    int n = 0;
    u8 t[2];
    for (; *s; s++) { int k = sjis_enc(*s, t); if (n + k + 1 > cap) break; memcpy(d + n, t, (u32)k); n += k; }
    d[n] = 0;
    return n;
}
int sceCodepage_driver_C899572E(u16 *d, int cap, const char *src)        /* UTF-8 -> UCS-2 */
{
    const u8 *s = (const u8 *)src;
    int n = 0;
    while (*s && (n + 2) * 2 <= cap) {
        u32 c = *s++;
        if (c >= 0xe0 && s[0] && s[1]) { c = (c & 0x0f) << 12 | (s[0] & 0x3fu) << 6 | (s[1] & 0x3fu); s += 2; }
        else if (c >= 0xc0 && s[0]) { c = (c & 0x1f) << 6 | (s[0] & 0x3fu); s++; }
        else if (c >= 0x80) c = '?';
        d[n++] = (u16)c;
    }
    d[n] = 0;
    return n;
}
int sceCodepage_driver_907CBFD2(char *d, int cap, const u16 *s)          /* UCS-2 -> UTF-8 */
{
    int n = 0;
    for (; *s; s++) {
        u32 c = *s;
        int k = c < 0x80 ? 1 : c < 0x800 ? 2 : 3;
        if (n + k + 1 > cap) break;
        if (k == 1) d[n++] = (char)c;
        else if (k == 2) { d[n++] = (char)(0xc0 | c >> 6); d[n++] = (char)(0x80 | (c & 0x3f)); }
        else { d[n++] = (char)(0xe0 | c >> 12); d[n++] = (char)(0x80 | (c >> 6 & 0x3f)); d[n++] = (char)(0x80 | (c & 0x3f)); }
    }
    d[n] = 0;
    return n;
}

/* ---------------- sceRtc_driver ---------------- */
int sceRtc_driver_9012B140(ScePspDateTime *t)
{
    ScePspDateTime now = { 2026, 10, 10, 12, 34, 56, 0 };
    *t = now;
    return 0;
}
int sceRtc_driver_A4A5BF1B(const ScePspDateTime *t, u32 *dos)
{
    *dos = (u32)(t->year - 1980) << 25 | (u32)t->month << 21 | (u32)t->day << 16 |
           (u32)t->hour << 11 | (u32)t->minute << 5 | t->second / 2u;
    return 0;
}
int sceRtc_driver_74772CCC(ScePspDateTime *t, u32 dos)
{
    t->year = (unsigned short)(1980 + (dos >> 25)); t->month = dos >> 21 & 15; t->day = dos >> 16 & 31;
    t->hour = dos >> 11 & 31; t->minute = dos >> 5 & 63; t->second = (dos & 31) * 2; t->microsecond = 0;
    return (t->month < 1 || t->month > 12 || t->day < 1) ? -1 : 0;
}

/* ---------------- media ---------------- */
static int wait_for(int (*cond)(void))
{
    struct timespec ts = { 0, 1000000 };
    for (int i = 0; i < 3000; i++) { if (cond()) return 0; nanosleep(&ts, 0); }
    return -1;
}
static u32 ctx_flags(void) { return __atomic_load_n(&g_ctx.flags, __ATOMIC_SEQ_CST); }
static int evf_ready(void) { return host_evf >= 0; }
static int inserted(void) { return (ctx_flags() & (ST_OPEN | ST_ASSIGNED)) == (ST_OPEN | ST_ASSIGNED) && !g_ctx.in_insert; }
static int ejected(void) { return !(ctx_flags() & (ST_OPEN | ST_ASSIGNED)); }

int host_insert(void)
{
    if (wait_for(evf_ready)) return -1;
    host_ms_status = 4;
    host_ms_setup();                                             /* msstor sets the card up first */
    sceKernelSetEventFlag(host_evf, 1);
    return wait_for(inserted);
}
int host_eject(void)
{
    sceKernelSetEventFlag(host_evf, 2);
    return wait_for(ejected);
}
volatile u32 host_mounts;                             /* successful IoMounts (k_sceIoAssign) */
static u32 mounts_before;
static int remounted(void) { return host_mounts > mounts_before && inserted(); }
int host_remounted(u32 before)                       /* after a rescan: ms0: assigned again since `before` */
{
    mounts_before = before;
    return wait_for(remounted);
}
int host_boot(void)
{
    if (module_start(0, 0) != 0 || !host_drv) return -1;
    return host_insert();
}
