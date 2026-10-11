// kernel-mode flash0 writer sibling PRX for the ExtremeSpeed EBOOT
// basically going to use this to allow our standard vsh eboot to write to flash0 just for the dratini installer to work
//
// v2: the EBOOT reads the file and hands over a buffer (kflash_args_t), so this module opens nothing
// but the destination. It writes the buffer, reads the file back and compares every byte.
// Results are positive on purpose: when module_start's thread exits with a negative status, threadman
// replaces it with 0x800200D2 (ILLEGAL_ARGUMENT), which hid the real error. 0 = written and verified,
// else (step << 24) | (error & 0xFFFFFF), the error being an 0x80xxxxxx code.

#include <pspkernel.h>
#include <pspiofilemgr.h>
#include <string.h>

PSP_MODULE_INFO("DratiniFSKernelFlash", 0x1006, 2, 0);
PSP_MAIN_THREAD_ATTR(0);

#define KFLASH_MAGIC 0x324B4644            /* "DFK2" */
typedef struct {
    char zero;                             /* 0: an old (v1) flasher reads src = "" and stops */
    char pad[3];
    unsigned int magic, size;
    const unsigned char *buf;              /* the new file, in the EBOOT's memory */
    char dst[128];
    char unused[112];
} kflash_args_t;                           /* 256 bytes, like v1's {src[128], dst[128]} */

#ifndef FIO_CST_MODE
#define FIO_CST_MODE 0x0001                /* sceIoChstat bits: st_mode (FAT: the read-only flag) */
#endif

enum { ST_ARGS = 1, ST_ASSIGN, ST_OPEN, ST_WRITE, ST_REOPEN, ST_READBACK, ST_MISMATCH };
static int fail(int step, int err) { return (step << 24) | (err & 0xFFFFFF); }

static void open_flash(void)
{
    // unassign and reassign as i've seen done in other installers
    while (sceIoUnassign("flash0:") < 0)
        sceKernelDelayThread(500000);
    while (sceIoAssign("flash0:", "lflash0:0,0", "flashfat0:", 0, NULL, 0) < 0)
        sceKernelDelayThread(500000);
    while (sceIoUnassign("flash1:") < 0)
        sceKernelDelayThread(500000);
    while (sceIoAssign("flash1:", "lflash0:0,1", "flashfat1:", 0, NULL, 0) < 0)
        sceKernelDelayThread(500000);
}

int module_start(SceSize args, void *argp)
{
    kflash_args_t *a = (kflash_args_t *)argp;
    if (args < sizeof(kflash_args_t) || !argp || a->magic != KFLASH_MAGIC || !a->buf || !a->size)
        return fail(ST_ARGS, 0);
    a->dst[sizeof(a->dst) - 1] = 0;

    open_flash();

    SceIoStat st;                                          // a read-only file can't be opened for writing
    memset(&st, 0, sizeof(st));
    if (sceIoGetstat(a->dst, &st) >= 0 && !(st.st_mode & FIO_S_IWUSR)) {
        st.st_mode |= FIO_S_IRWXU | FIO_S_IRWXG | FIO_S_IRWXO;
        sceIoChstat(a->dst, &st, FIO_CST_MODE);
    }

    SceUID out = sceIoOpen(a->dst, PSP_O_WRONLY | PSP_O_CREAT | PSP_O_TRUNC, 0777);
    if (out < 0) return fail(ST_OPEN, out);
    int w = sceIoWrite(out, a->buf, (int)a->size);
    int c = sceIoClose(out);
    if (w != (int)a->size) return fail(ST_WRITE, w < 0 ? w : 0);
    if (c < 0) return fail(ST_WRITE, c);

    static unsigned char chk[4096];                        // read it back: every byte must match
    SceUID in = sceIoOpen(a->dst, PSP_O_RDONLY, 0);
    if (in < 0) return fail(ST_REOPEN, in);
    unsigned int off = 0;
    int bad = 0;
    for (;;) {
        int n = sceIoRead(in, chk, sizeof(chk));
        if (n < 0) { sceIoClose(in); return fail(ST_READBACK, n); }
        if (n == 0) break;
        for (int i = 0; i < n && !bad; i++) bad = off + i >= a->size || chk[i] != a->buf[off + i];
        off += (unsigned int)n;
        if (bad) break;
    }
    sceIoClose(in);
    if (bad || off != a->size) return fail(ST_MISMATCH, 0);
    return 0;
}

int module_stop(SceSize args, void *argp)
{
    (void)args; (void)argp;
    return 0;
}
