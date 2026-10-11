/* io_psp.c - the fstest backend on the PSP: sceIo* itself, plus threads, callbacks, a clock and
 * output (screen + ms0:/FATMSTEST.LOG, appended after every group so a crash still leaves a trail). */
#include "fstest.h"
#include <pspthreadman.h>
#include <pspsysmem.h>
#include <pspdebug.h>
#include <pspdisplay.h>
#include <stdarg.h>
#include <stdio.h>

int  fs_open(const char *p, int flags, int mode) { return sceIoOpen(p, flags, mode); }
int  fs_close(int fd) { return sceIoClose(fd); }
int  fs_read(int fd, void *buf, int n) { return sceIoRead(fd, buf, n); }
int  fs_write(int fd, const void *buf, int n) { return sceIoWrite(fd, buf, n); }
s64  fs_lseek(int fd, s64 ofs, int whence) { return sceIoLseek(fd, ofs, whence); }
int  fs_read_async(int fd, void *buf, int n) { return sceIoReadAsync(fd, buf, n); }
int  fs_wait_async(int fd, s64 *res) { SceInt64 r = 0; int e = sceIoWaitAsync(fd, &r); *res = r; return e; }
int  fs_remove(const char *p) { return sceIoRemove(p); }
int  fs_mkdir(const char *p, int mode) { return sceIoMkdir(p, mode); }
int  fs_rmdir(const char *p) { return sceIoRmdir(p); }
int  fs_rename(const char *o, const char *n) { return sceIoRename(o, n); }
int  fs_dopen(const char *p) { return sceIoDopen(p); }
int  fs_dread(int fd, SceIoDirent *d) { return sceIoDread(fd, d); }
int  fs_dclose(int fd) { return sceIoDclose(fd); }
int  fs_getstat(const char *p, SceIoStat *st) { return sceIoGetstat(p, st); }
int  fs_chstat(const char *p, SceIoStat *st, int bits) { return sceIoChstat(p, st, bits); }
int  fs_devctl(const char *dev, unsigned int cmd, void *in, int il, void *out, int ol) { return sceIoDevctl(dev, cmd, in, il, out, ol); }
int  fs_ioctl(int fd, unsigned int cmd, void *in, int il, void *out, int ol) { return sceIoIoctl(fd, cmd, in, il, out, ol); }
u32  fs_get_sdk(void) { return (u32)sceKernelGetCompiledSdkVersion(); }
u32  fs_now_ms(void) { return sceKernelGetSystemTimeLow() / 1000; }

struct tstart { int (*fn)(void *); void *arg; volatile int ret; };
static int tentry(SceSize len, void *argp)
{
    struct tstart *t = *(struct tstart **)argp;
    (void)len;
    t->ret = t->fn(t->arg);
    return 0;
}
int fs_threads(int n, int (*fn)(void *), void **args)
{
    struct tstart t[8];
    SceUID th[8];
    int bad = 0;
    for (int i = 0; i < n; i++) {
        struct tstart *p = &t[i];
        t[i].fn = fn; t[i].arg = args[i]; t[i].ret = -1;
        th[i] = sceKernelCreateThread("fstest_worker", tentry, 0x20, 0x10000, PSP_THREAD_ATTR_USER, 0);
        if (th[i] < 0 || sceKernelStartThread(th[i], sizeof p, &p) < 0) { fs_print("  can't start thread %d", i); return 1; }
    }
    for (int i = 0; i < n; i++) {
        sceKernelWaitThreadEnd(th[i], 0);
        sceKernelDeleteThread(th[i]);
        if (t[i].ret) { fs_print("  thread %d returned %d", i, t[i].ret); bad = 1; }
    }
    return bad;
}

static volatile int cb_arg = -1;
static int cb_fn(int count, int arg, void *common) { (void)count; (void)common; cb_arg = arg; return 0; }
int  fs_cb_create(void) { cb_arg = -1; return sceKernelCreateCallback("fstest_ms", cb_fn, 0); }
int  fs_cb_seen(int cb) { (void)cb; sceKernelCheckCallback(); return cb_arg; }
void fs_cb_delete(int cb) { sceKernelDeleteCallback(cb); }

/* ---- output ---- */
static char logbuf[16384];
static int loglen, row;
void log_flush(void)
{
    if (!loglen) return;
    int fd = sceIoOpen("ms0:/FATMSTEST.LOG", PSP_O_WRONLY | PSP_O_CREAT | PSP_O_APPEND, 0777);
    if (fd >= 0) { sceIoWrite(fd, logbuf, loglen); sceIoClose(fd); }
    loglen = 0;
}
void fs_print(const char *fmt, ...)
{
    char line[256];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(line, sizeof line, fmt, ap);
    va_end(ap);
    if (row >= 33) { pspDebugScreenClear(); row = 0; }
    pspDebugScreenSetTextColor(strstr(line, "FAIL") ? 0xff4040ff : strstr(line, "SKIP") ? 0xff40c0ff : 0xffffffff);
    pspDebugScreenPrintf("%.67s\n", line);
    row++;
    int n = (int)strlen(line);
    if (loglen + n + 2 > (int)sizeof logbuf) log_flush();
    memcpy(logbuf + loglen, line, n);
    logbuf[loglen + n] = '\r'; logbuf[loglen + n + 1] = '\n';
    loglen += n + 2;
}
void fs_after_group(const char *name) { (void)name; log_flush(); }
