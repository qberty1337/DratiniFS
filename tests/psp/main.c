/* fatms on-device conformance test (EBOOT). Runs tests/common/scenarios.c against ms0: through the
 * real PSP stack (iofilemgr -> fatms -> msstor -> card), in both name encodings:
 *   pass 1: compiled SDK 6.60 (UTF-8 names, what most retail games use) - every group, ~45 MB I/O
 *   pass 2: compiled SDK 3.70 (Shift-JIS names, homebrew / older games) - names and listings
 * plus a persistence check across runs (ms0:/FATMSPERSIST: written now, verified next launch) and, under
 * DratiniFS, card checks: partition table, FS type, the large-card patch, ExtremeSpeed ISOs.
 * Everything is written under ms0:/FATMSTEST and removed at the end. Log: ms0:/FATMSTEST.LOG.
 * Expected results are Sony 6.61 fatms's, so the stock driver must pass too. */
#include "fstest.h"
#include <pspkernel.h>
#include <pspdebug.h>
#include <pspctrl.h>
#include <pspsysmem.h>
#include <stdio.h>
#include <stdlib.h>
#include <malloc.h>

PSP_MODULE_INFO("fatms_tests", 0, 1, 0);
PSP_MAIN_THREAD_ATTR(THREAD_ATTR_USER);
PSP_HEAP_SIZE_KB(-1024);

int sceKernelSetCompiledSdkVersion370(u32 ver);      /* sdkver.S */
int sceKernelSetCompiledSdkVersion660(u32 ver);
void log_flush(void);                                /* io_psp.c */

static int exit_cb(int a, int b, void *c) { (void)a; (void)b; (void)c; sceKernelExitGame(); return 0; }
static int cb_thread(SceSize a, void *b)
{
    (void)a; (void)b;
    sceKernelRegisterExitCallback(sceKernelCreateCallback("exit", exit_cb, 0));
    sceKernelSleepThreadCB();
    return 0;
}

static u32 wait_button(u32 mask)
{
    SceCtrlData pad;
    do { sceCtrlReadBufferPositive(&pad, 1); } while (pad.Buttons & mask);    /* release first */
    do { sceCtrlReadBufferPositive(&pad, 1); } while (!(pad.Buttons & mask));
    return pad.Buttons;
}

static u32 cluster_bytes(void)
{
    u32 o[5] = { 0 }, *p = o;
    sceIoDevctl("ms0:", 0x02425818, &p, 4, 0, 0);
    return o[3] * o[4];
}

/* written by one run, verified by the next (after a reboot it's a cold read of the card) */
static void persistence(fst_result *r)
{
    u32 state[2] = { 0, 0 }, cs = cluster_bytes();
    char a[64], b[96];
    fs_print("-- persistence");
    sceIoMkdir("ms0:/FATMSPERSIST", 0777);
    int fd = sceIoOpen("ms0:/FATMSPERSIST/STATE.BIN", PSP_O_RDONLY, 0);
    if (fd >= 0 && sceIoRead(fd, state, 8) == 8 && state[0] == 0x46415453) {
        snprintf(a, sizeof a, "ms0:/FATMSPERSIST/P%u.BIN", (unsigned)(state[1] % 2));
        snprintf(b, sizeof b, "ms0:/FATMSPERSIST/Persisted long name %u.dat", (unsigned)(state[1] % 2));
        int e1 = fst_verify_file(a, 1000 + state[1], 3 * cs + 1234), e2 = fst_verify_file(b, 2000 + state[1], 5000);
        if (e1 || e2) { r->fail++; fs_print("  FAIL persistence: files from run %u read back wrong (%d, %d)", (unsigned)state[1], e1, e2); }
        else { r->pass++; fs_print("[ ok ] files written by run %u are intact", (unsigned)state[1]); }
        sceIoRemove(a);
        sceIoRemove(b);
    } else fs_print("  (first run: nothing to verify yet)");
    if (fd >= 0) sceIoClose(fd);
    state[0] = 0x46415453;
    state[1]++;
    snprintf(a, sizeof a, "ms0:/FATMSPERSIST/P%u.BIN", (unsigned)(state[1] % 2));
    snprintf(b, sizeof b, "ms0:/FATMSPERSIST/Persisted long name %u.dat", (unsigned)(state[1] % 2));
    int e = fst_write_file(a, 1000 + state[1], 3 * cs + 1234);
    if (!e) e = fst_write_file(b, 2000 + state[1], 5000);
    fd = sceIoOpen("ms0:/FATMSPERSIST/STATE.BIN", PSP_O_WRONLY | PSP_O_CREAT | PSP_O_TRUNC, 0777);
    if (!e && fd >= 0 && sceIoWrite(fd, state, 8) == 8) fs_print("  run %u written; run again (ideally after a reboot) to verify", (unsigned)state[1]);
    else { r->fail++; fs_print("  FAIL persistence: can't write the files for the next run (%d)", e); }
    if (fd >= 0) sceIoClose(fd);
}

/* ---- DratiniFS card checks: partition table, FS type, the large-card patch, ES ----
 * Uses the driver's ES devctls (0x041000xx: raw sectors through msstor0:), so it is skipped on
 * Sony's / the baseline fatms. Nothing is changed: the only write puts back the bytes just read. */
static int raw(u32 cmd, u32 sector, void *buf, u32 count)
{
    struct { u32 sector, count; void *buf; } a = { sector, count, buf };
    return sceIoDevctl("fatms0:", cmd, &a, sizeof a, 0, 0);
}
static u32 le32(const u8 *p) { return (u32)(p[0] | p[1] << 8 | p[2] << 16 | (u32)p[3] << 24); }
static void check(fst_result *r, int ok, const char *what)
{
    if (ok) { r->pass++; fs_print("[ ok ] %s", what); }
    else { r->fail++; fs_print("  FAIL %s", what); }
}
static void es_checks(fst_result *r, u32 es)
{
    static u8 sb[512] __attribute__((aligned(64))), tab[512] __attribute__((aligned(64)));
    static u8 pool[61440] __attribute__((aligned(64)));
    u8 *a = memalign(64, 65536 + 64), *b = memalign(64, 65536 + 64);
    char path[300], line[96];
    if (!a || !b || raw(0x04100001, es, sb, 1) < 0 || le32(sb) != 0x44505345) { check(r, 0, "ES superblock"); return; }
    u32 n = le32(sb + 8), used = le32(sb + 28), done = 0;
    if (used > sizeof pool) used = sizeof pool;
    for (u32 s = 0; s * 512 < used; s++) raw(0x04100001, es + 80 + s, pool + s * 512, 1);
    fs_print("  ES: %u table entries", (unsigned)n);
    for (u32 i = 0; i < n && i < 632 && done < 4; i++) {
        if (i % 8 == 0 && raw(0x04100001, es + 1 + i / 8, tab, 1) < 0) break;
        const u8 *e = tab + (i % 8) * 64;
        u32 start = le32(e + 16), size = le32(e + 24), off = le32(e + 32), len = e[36] | e[37] << 8;
        if (!(le32(e + 28) & 1) || !len || off + len > used || size < 2048) continue;
        snprintf(path, sizeof path, "ms0:/%.*s", (int)len, (char *)pool + off);
        SceIoStat st;
        int ok = sceIoGetstat(path, &st) >= 0 && (u32)st.st_size == size && st.sce_st_mtime.year == 1992;
        SceUID fd = sceIoOpen(path, PSP_O_RDONLY, 0);
        u32 k = size < 65536 ? size & ~511u : 65536, tail = size % 2048 ? size % 2048 : 2048;
        ok = ok && fd >= 0 && sceIoRead(fd, a, (int)k) == (int)k &&                      /* the start */
             raw(0x04100001, es + start, b, k / 512) >= 0 && !memcmp(a, b, k);
        ok = ok && sceIoLseek(fd, size - tail, PSP_SEEK_SET) == (SceOff)(size - tail) &&   /* the end, unaligned */
             sceIoRead(fd, a + 3, (int)tail) == (int)tail &&
             raw(0x04100001, es + start + (size - tail) / 512, b, (tail + 511 + (size - tail) % 512) / 512) >= 0 &&
             !memcmp(a + 3, b + (size - tail) % 512, tail);
        ok = ok && sceIoRead(fd, a, 100) == 0;                                            /* past the end */
        if (fd >= 0) sceIoClose(fd);
        /* listed in its folder (what ARK reads for the XMB game list), with the ES date */
        char *slash = strrchr(path, '/');
        int seen = 0;
        *slash = 0;
        SceUID dfd = sceIoDopen(path);
        SceIoDirent de;
        while (dfd >= 0 && (memset(&de, 0, sizeof de), sceIoDread(dfd, &de) > 0))
            if (!strcmp(de.d_name, slash + 1) && de.d_stat.sce_st_mtime.year == 1992 && (de.d_stat.st_mode & 0x2000)) seen++;
        if (dfd >= 0) sceIoDclose(dfd);
        *slash = '/';
        ok = ok && seen == 1;
        snprintf(line, sizeof line, "ES %.60s", path + 5);
        check(r, ok, line);
        done++;
    }
    if (!done) fs_print("  (no active ES ISO of 2 KB or more to read)");
    free(a); free(b);
}
static void card_checks(fst_result *r)
{
    static u8 mbr[512] __attribute__((aligned(64))), s1[512] __attribute__((aligned(64))), s2[512] __attribute__((aligned(64)));
    u32 ver = 0, es = 0;
    fs_print("-- card");
    if (sceIoDevctl("fatms0:", 0x04100007, 0, 0, &ver, 4) < 0) { fs_print("  not DratiniFS (no ES devctls): card checks skipped"); return; }
    fs_print("  driver: DratiniFS %u.%u", (unsigned)(ver >> 8), (unsigned)(ver & 0xff));
    if (raw(0x04100001, 0, mbr, 1) < 0) { check(r, 0, "read sector 0"); return; }
    int has_mbr = mbr[510] == 0x55 && mbr[511] == 0xaa && mbr[0x1be] == 0x80 && mbr[0x1c2];
    for (int i = 0; has_mbr && i < 4; i++) {
        const u8 *e = mbr + 0x1be + i * 16;
        u32 lba = le32(e + 8), len = le32(e + 12);
        if (!e[4] || !len) continue;
        if (e[4] == 0xda) es = lba;
        if (raw(0x04100001, lba, s1, 1) < 0) { check(r, 0, "read a partition's first sector"); continue; }
        fs_print("  partition %d: type %02x at %u, %u MB%s", i, e[4], (unsigned)lba, (unsigned)(len / 2048),
                 e[4] == 0xda ? " (ES)" : !memcmp(s1 + 3, "EXFAT   ", 8) ? " (exFAT)" : !memcmp(s1 + 82, "FAT32", 5) ? " (FAT32)" : "");
        /* the partition's last sector: read, put back unchanged, read again. Past the adapter's
           reported size this works only with the large-card patch. */
        u32 last = lba + len - 1;
        int ok = raw(0x04100001, last, s1, 1) >= 0 && raw(0x04100002, last, s1, 1) >= 0 &&
                 raw(0x04100001, last, s2, 1) >= 0 && !memcmp(s1, s2, 512);
        char what[80];
        snprintf(what, sizeof what, "last sector of partition %d (%u MB in) readable + writable", i, (unsigned)(last / 2048));
        check(r, ok, what);
    }
    if (!has_mbr) fs_print("  no MBR (superfloppy card)");
    struct { u32 init, start, total, count, data, next; } info;
    if (sceIoDevctl("fatms0:", 0x04100003, 0, 0, &info, sizeof info) >= 0 && info.init) es_checks(r, info.start);
    else if (es) check(r, 0, "ES partition in the MBR but not loaded");
    else fs_print("  no ES partition");
    log_flush();
}

int main(void)
{
    fst_result r = { 0, 0, 0 };
    SceUID th = sceKernelCreateThread("exit_cb", cb_thread, 0x11, 0xFA0, 0, 0);
    if (th >= 0) sceKernelStartThread(th, 0, 0);
    pspDebugScreenInit();
    sceCtrlSetSamplingCycle(0);
    sceCtrlSetSamplingMode(PSP_CTRL_MODE_DIGITAL);

    pspDebugScreenPrintf("fatms conformance test\n\n");
    pspDebugScreenPrintf("Writes about 45 MB to ms0:/FATMSTEST (removed afterwards) and keeps\n");
    pspDebugScreenPrintf("two small files in ms0:/FATMSPERSIST. Log: ms0:/FATMSTEST.LOG\n\n");
    pspDebugScreenPrintf("X = start      O = exit\n");
    if (wait_button(PSP_CTRL_CROSS | PSP_CTRL_CIRCLE) & PSP_CTRL_CIRCLE) { sceKernelExitGame(); return 0; }
    pspDebugScreenClear();

    sceIoRemove("ms0:/FATMSTEST.LOG");
    u32 t0 = sceKernelGetSystemTimeLow();
    fs_print("fatms test, compiled SDK at start 0x%08x", (unsigned)sceKernelGetCompiledSdkVersion());
    card_checks(&r);
    persistence(&r);
    log_flush();

    sceKernelSetCompiledSdkVersion660(0x06060110);
    if ((u32)sceKernelGetCompiledSdkVersion() != 0x06060110) fs_print("  (can't select SDK 6.60: running pass 1 in the current mode)");
    fs_print("== pass 1: SDK 0x%08x", (unsigned)sceKernelGetCompiledSdkVersion());
    fst_run("ms0:/FATMSTEST", FST_BIG | FST_KEEP, &r);

    sceKernelSetCompiledSdkVersion370(0x03070110);
    if ((u32)sceKernelGetCompiledSdkVersion() == 0x03070110) {
        fs_print("== pass 2: SDK 0x%08x", (unsigned)sceKernelGetCompiledSdkVersion());
        fst_run("ms0:/FATMSTEST", FST_NAMES_ONLY, &r);
    } else {
        r.skip++;
        fs_print("  SKIP pass 2: can't select SDK 3.70 (Shift-JIS names untested)");
        fst_rm_tree("ms0:/FATMSTEST");
    }
    fs_print("== %d passed, %d failed, %d skipped in %u s", r.pass, r.fail, r.skip,
             (unsigned)((sceKernelGetSystemTimeLow() - t0) / 1000000));
    fs_print(r.fail ? "SOME CHECKS FAILED - see ms0:/FATMSTEST.LOG" : "ALL CHECKS PASSED");
    log_flush();
    pspDebugScreenSetTextColor(0xffffffff);
    pspDebugScreenPrintf("\nPress X to exit\n");
    wait_button(PSP_CTRL_CROSS);
    sceKernelExitGame();
    return 0;
}
