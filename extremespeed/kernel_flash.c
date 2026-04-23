// kernel-mode flash0 writer sibling PRX for the ExtremeSpeed EBOOT
// basically going to use this to allow our standard vsh eboot to write to flash0 just for the dratini installer to work

#include <pspkernel.h>
#include <pspiofilemgr.h>
#include <string.h>

PSP_MODULE_INFO("DratiniFSKernelFlash", 0x1006, 1, 0);
PSP_MAIN_THREAD_ATTR(0);

typedef struct {
    char src[128];
    char dst[128];
} kflash_args_t;

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
    if (args < sizeof(kflash_args_t) || !argp) return -1;
    kflash_args_t *a = (kflash_args_t *)argp;

    open_flash();

    SceUID in = sceIoOpen(a->src, PSP_O_RDONLY, 0);
    if (in < 0) return in;

    SceUID out = sceIoOpen(a->dst, PSP_O_WRONLY | PSP_O_CREAT | PSP_O_TRUNC, 0777);
    if (out < 0) { sceIoClose(in); return out; }

    static char buf[4096];
    for (;;) {
        int n = sceIoRead(in, buf, sizeof(buf));
        if (n <= 0) break;
        int w = sceIoWrite(out, buf, n);
        if (w != n) { sceIoClose(in); sceIoClose(out); return -2; }
    }
    sceIoClose(in);
    sceIoClose(out);
    return 0;
}

int module_stop(SceSize args, void *argp)
{
    (void)args; (void)argp;
    return 0;
}
