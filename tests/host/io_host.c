/* io_host.c - the fstest backend on the host: the baseline driver behind the parts of
 * iofilemgr that change results, for a user-mode caller like a game (6.61 iofilemgr, uofw):
 * the device prefix is stripped from paths, the rename's new name too when it has one (6.61 asm
 * text+0x190c: beql delay slot, missing from uofw's C); at most 10 user fds per device (dev_type
 * 0xa0010 bits 16-23, else 0x80020320); devctl cmd bit 15 is kernel-only (0x800200D1); lseek
 * whence > 2 is 0x80020324; read/write need FREAD/FWRITE on the fd (else 0x80020323); fds of a
 * terminated device fail; async I/O completes at once. host_kcall = a kernel-mode caller.
 * A path's device goes to the driver iofilemgr would pick (host_devarg: assigns, aliases, driver
 * names), so ms0:, ef0: and eh0: reach their own drivers on the PSP Go build. */
#include "fstest.h"
#include "host.h"
#include <pthread.h>
#include <stdarg.h>
#include <stdio.h>
#include <stddef.h>
#include <time.h>
#include <stdlib.h>

#define MAXFD 64
struct hiob {
    PspIoDrvFileArg a;                             /* what the driver sees (Sony's iob prefix) */
    u32 pad;
    s64 fpos;                                      /* iob+0x18: i_fpos, written by IoLseek */
    int used, flags, dir, dead;
    s64 async_res;
};
_Static_assert(offsetof(struct hiob, fpos) == 0x18, "iob i_fpos");
static struct hiob iobs[MAXFD];
static pthread_mutex_t iolock = PTHREAD_MUTEX_INITIALIZER;
int host_kcall;

static PspIoDrvFuncs *F(const PspIoDrvFileArg *a) { return a->drv->drv->funcs; }
static const char *strip(const char *p) { const char *c = strchr(p, ':'); return c ? c + 1 : p; }
static void terminate(const char *drvname)                  /* sceIoTerminateFd: that driver's fds */
{
    pthread_mutex_lock(&iolock);
    for (int i = 0; i < MAXFD; i++) if (iobs[i].used && !strcmp(iobs[i].a.drv->drv->name, drvname)) iobs[i].dead = 1;
    pthread_mutex_unlock(&iolock);
}
static struct hiob *get(int fd)
{
    int i = fd - 3;
    return (i >= 0 && i < MAXFD && iobs[i].used) ? &iobs[i] : 0;
}
static int alloc(const char *path, int flags, int dir)
{
    host_on_terminate = terminate;
    PspIoDrvArg *d = host_devarg(path);
    pthread_mutex_lock(&iolock);
    int quota = (d->drv->dev_type >> 16) & 0xff, n = 0;   /* per device, as iofilemgr counts */
    for (int i = 0; i < MAXFD; i++) n += iobs[i].used && iobs[i].a.drv == d;
    if (!host_kcall && quota && n >= quota) { pthread_mutex_unlock(&iolock); return E_IO_MFILE; }
    for (int i = 0; i < MAXFD; i++)
        if (!iobs[i].used) {
            memset(&iobs[i], 0, sizeof iobs[i]);
            iobs[i].used = 1;
            iobs[i].flags = flags;
            iobs[i].dir = dir;
            iobs[i].a.drv = d;
            pthread_mutex_unlock(&iolock);
            return i + 3;
        }
    pthread_mutex_unlock(&iolock);
    return (int)0x80020320;                        /* too many fds (iofilemgr) */
}
static void release(int fd) { pthread_mutex_lock(&iolock); iobs[fd - 3].used = 0; pthread_mutex_unlock(&iolock); }
static void tmp(struct hiob *t, const char *path) { memset(t, 0, sizeof *t); t->a.drv = host_devarg(path); }

int fs_open(const char *path, int flags, int mode)
{
    int fd = alloc(path, flags, 0);
    if (fd < 0) return fd;
    int r = F(&get(fd)->a)->IoOpen(&get(fd)->a, (char *)strip(path), flags, mode);
    if (r < 0) { release(fd); return r; }
    return fd;
}
int fs_close(int fd)
{
    struct hiob *h = get(fd);
    if (!h || h->dir) return E_IO_BADFD;
    if (!h->dead) F(&h->a)->IoClose(&h->a);
    release(fd);
    return 0;
}
int fs_read(int fd, void *buf, int n)
{
    struct hiob *h = get(fd);
    if (!h || h->dir || !(h->flags & PSP_O_RDONLY)) return E_IO_BADFD;
    return h->dead ? E_UNSUP : F(&h->a)->IoRead(&h->a, buf, n);
}
int fs_write(int fd, const void *buf, int n)
{
    struct hiob *h = get(fd);
    if (!h || h->dir || !(h->flags & PSP_O_WRONLY)) return E_IO_BADFD;
    return h->dead ? E_UNSUP : F(&h->a)->IoWrite(&h->a, buf, n);
}
s64 fs_lseek(int fd, s64 ofs, int whence)
{
    struct hiob *h = get(fd);
    if (!h || h->dir) return E_IO_BADFD;
    if (whence < 0 || whence > 2) return E_IO_INVAL;
    return h->dead ? E_UNSUP : F(&h->a)->IoLseek(&h->a, ofs, whence);
}
int fs_read_async(int fd, void *buf, int n)
{
    struct hiob *h = get(fd);
    if (!h) return E_IO_BADFD;
    h->async_res = fs_read(fd, buf, n);
    return 0;
}
int fs_wait_async(int fd, s64 *res)
{
    struct hiob *h = get(fd);
    if (!h) return E_IO_BADFD;
    *res = h->async_res;
    return 0;
}
int fs_remove(const char *p) { struct hiob t; tmp(&t, p); return F(&t.a)->IoRemove(&t.a, strip(p)); }
int fs_mkdir(const char *p, int mode) { struct hiob t; tmp(&t, p); return F(&t.a)->IoMkdir(&t.a, strip(p), mode); }
int fs_rmdir(const char *p) { struct hiob t; tmp(&t, p); return F(&t.a)->IoRmdir(&t.a, strip(p)); }
int fs_rename(const char *o, const char *n) { struct hiob t; tmp(&t, o); return F(&t.a)->IoRename(&t.a, strip(o), strip(n)); }
int fs_getstat(const char *p, SceIoStat *st) { struct hiob t; tmp(&t, p); return F(&t.a)->IoGetstat(&t.a, strip(p), st); }
int fs_chstat(const char *p, SceIoStat *st, int bits) { struct hiob t; tmp(&t, p); return F(&t.a)->IoChstat(&t.a, strip(p), st, bits); }
int fs_devctl(const char *dev, unsigned int cmd, void *in, int il, void *out, int ol)
{
    struct hiob t;
    if (!host_kcall && (cmd & 0x8000)) return E_IO_PERM;
    tmp(&t, dev);
    return F(&t.a)->IoDevctl(&t.a, dev, cmd, in, il, out, ol);
}
int fs_ioctl(int fd, unsigned int cmd, void *in, int il, void *out, int ol)
{
    struct hiob *h = get(fd);
    if (!h) return E_IO_BADFD;
    return h->dead ? E_UNSUP : F(&h->a)->IoIoctl(&h->a, cmd, in, il, out, ol);
}
int fs_dopen(const char *p)
{
    int fd = alloc(p, 0, 1);
    if (fd < 0) return fd;
    int r = F(&get(fd)->a)->IoDopen(&get(fd)->a, strip(p));
    if (r < 0) { release(fd); return r; }
    return fd;
}
int fs_dread(int fd, SceIoDirent *d)
{
    struct hiob *h = get(fd);
    if (!h || !h->dir) return E_IO_BADFD;
    return h->dead ? E_UNSUP : F(&h->a)->IoDread(&h->a, d);
}
int fs_dclose(int fd)
{
    struct hiob *h = get(fd);
    if (!h || !h->dir) return E_IO_BADFD;
    int r = h->dead ? 0 : F(&h->a)->IoDclose(&h->a);
    if (r >= 0) release(fd);
    return r;
}

u32 fs_get_sdk(void) { return host_sdk; }

struct targ { int (*fn)(void *); void *arg; int ret; };
static void *trun(void *p) { struct targ *t = p; t->ret = t->fn(t->arg); return 0; }
int fs_threads(int n, int (*fn)(void *), void **args)
{
    pthread_t th[8];
    struct targ t[8];
    int bad = 0;
    for (int i = 0; i < n; i++) { t[i].fn = fn; t[i].arg = args[i]; pthread_create(&th[i], 0, trun, &t[i]); }
    for (int i = 0; i < n; i++) { pthread_join(th[i], 0); if (t[i].ret) { fs_print("  thread %d returned %d", i, t[i].ret); bad = 1; } }
    return bad;
}

static int cb_next = 0x7000;
int  fs_cb_create(void) { return ++cb_next; }
int  fs_cb_seen(int cb) { return host_last_cb == cb ? host_last_cb_arg : -1; }
void fs_cb_delete(int cb) { (void)cb; }

u32 fs_now_ms(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (u32)(ts.tv_sec * 1000 + ts.tv_nsec / 1000000);
}
void fs_print(const char *fmt, ...)
{
    va_list ap;
    va_start(ap, fmt);
    vprintf(fmt, ap);
    va_end(ap);
    putchar('\n');
    fflush(stdout);
}

/* FST_FSCK_EACH=1: flush, snapshot the card and run fsck.fat after every group (finds the group
   that damaged the filesystem) */
void fs_after_group(const char *name)
{
    if (!getenv("FST_FSCK_EACH")) return;
    fs_devctl("ms0:", 0x00000100, 0, 0, 0, 0);
    host_disk_save("/tmp/fst_group.img");
    printf("   fsck after %s:\n", name);
    fflush(stdout);
    if (system("fsck.fat -n /tmp/fst_group.img 2>&1 | grep -E 'Reclaim|Bad|chain|lost|cross|Unused|differ|Contains' | sed 's/^/     /'")) {}
}
