/* host_main.c - drives the baseline driver on a FAT image (see tests/run_host.sh).
 *   fstest_host golden <img> <out.img>            deterministic suite; prints the I/O counters
 *   fstest_host full   <img> <out> [seeds.txt]    suite + host-only checks; writes <out>.keep.img
 *                                                  (full test tree), <out>.final.img + survivors
 *   fstest_host fill   <img> <out.img>            root directory full + card full
 *   fstest_host bigcard <disk> <out.img> <attr> patch|guard|nolower   cards past the adapter's size
 *   fstest_host es <disk> <out.img> <seed dir> <attr>                 ExtremeSpeed partition
 *   fstest_host format <disk> <out prefix> <seed dir|-> [attr]       formatting (<out>.0..3.img)
 *   fstest_host walk <disk> -                                         list/stat/read everything (a dumped card)
 * PSP Go build (fstest_host_go, -DPSPGO): ms0: + ef0: + eh0: (go_main below)
 *   fstest_host_go go|goformat|goformat16|goemu|goemu-nomsemu <ms disk> <out> <ef image> <eh image>
 * Exit status 0 = every check passed. */
#include "fstest.h"
#include "fatms.h"
#include "host.h"
#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <strings.h>

int module_reboot_phase(int phase, void *param, int a3, int a4);

static fst_result R;
static const char *G = "host";
#define HCHECK(c) do { if (c) R.pass++; else { R.fail++; printf("  FAIL %s:%d  %s\n", G, __LINE__, #c); } } while (0)
#define HEQ(a, b) do { long long _a = (long long)(a), _b = (long long)(b); if (_a == _b) R.pass++; else { R.fail++; \
                      printf("  FAIL %s:%d  %s == %s  (got 0x%llx, want 0x%llx)\n", G, __LINE__, #a, #b, _a, _b); } } while (0)
static void section(const char *name) { G = name; printf("-- %s\n", name); }

static u32 cluster_bytes(void) { u32 o[5] = { 0 }, *p = o; fs_devctl("ms0:", 0x02425818, &p, 4, 0, 0); return o[3] * o[4]; }
static u32 free_clusters(void) { u32 o[5] = { 0 }, *p = o; fs_devctl("ms0:", 0x02425818, &p, 4, 0, 0); return o[1]; }

static void check_boot(void)
{
    section("boot");
    /* Wipeout Pulse regression: the codepage driver must get its four tables inside the CPTable block */
    HEQ(host_cp.calls, 1);
    HCHECK(host_cp.block != 0);
    for (int i = 0; i < 4; i++) HEQ(host_cp.s[i], host_cp.sizes[i]);
    HCHECK(host_cp.t[0] == host_cp.block);
    HCHECK((u8 *)host_cp.t[1] == (u8 *)host_cp.t[0] + ((host_cp.sizes[0] + 0x3f) & ~0x3f));
    HCHECK((u8 *)host_cp.t[2] == (u8 *)host_cp.t[1] + ((host_cp.sizes[1] + 0x3f) & ~0x3f));
    HCHECK((u8 *)host_cp.t[3] == (u8 *)host_cp.t[2] + ((host_cp.sizes[2] + 0x3f) & ~0x3f));
    HCHECK(host_sysev && !strcmp(host_sysev->name, "SceFatfsSysEvent"));
    HCHECK(cluster_bytes() != 0);                            /* first op: the lazy identify */
    HCHECK(g_ctx.fat_type == 12 || g_ctx.fat_type == 16 || g_ctx.fat_type == 32);
}

static void check_kernel_hooks(void)
{
    section("reboot/sysevent");
    HEQ(module_reboot_phase(3, 0, 0, 0), 0);
    HEQ(host_mutex_count(g_ctx.mutex), 0);
    HEQ(module_reboot_phase(2, 0, 0, 0), 0);                  /* lock: no FS op may be mid-flight */
    HEQ(host_mutex_count(g_ctx.mutex), 1);
    HEQ(module_reboot_phase(1, 0, 0, 0), 0);
    HEQ(host_mutex_count(g_ctx.mutex), 0);
    int (*h)(int, char *, void *, int *) = host_sysev->handler;
    HEQ(h(0x102, 0, 0, 0), 0);                               /* suspending */
    HEQ(g_ctx.flags & ST_SUSPEND, ST_SUSPEND);
    HEQ(h(0x10000, 0, 0, 0), 0);                             /* resumed */
    HEQ(g_ctx.flags & ST_SUSPEND, 0);
    host_ms_status = 3;                                      /* card mid-insert: "busy" */
    HEQ(h(0x100002, 0, 0, 0), (int)0x80000021);
    host_ms_status = 4;
    int busy = 0, r = 0;
    for (int i = 0; i < 10; i++) if ((r = h(0x100002, 0, 0, 0)) != 0) busy++;
    HEQ(busy, 9);                                            /* then busy for 9 more queries */
    HEQ(r, 0);
}

static void check_seeds(const char *manifest)
{
    char line[1024];
    FILE *m = fopen(manifest, "r");
    section("mtools seeds");
    HCHECK(m != 0);
    if (!m) return;
    u8 *exp = malloc(4 << 20), *got = malloc(4 << 20);
    while (fgets(line, sizeof line, m)) {
        char *tab = strchr(line, '\t'), *nl = strchr(line, '\n');
        if (!tab) continue;
        *tab = 0;
        if (nl) *nl = 0;
        FILE *f = fopen(tab + 1, "rb");
        long n = 0;
        if (f) { n = (long)fread(exp, 1, 4 << 20, f); fclose(f); }
        SceIoStat st;
        int ok = fs_getstat(line, &st) == 0 && st.st_size == n;
        int fd = fs_open(line, PSP_O_RDONLY, 0);
        ok = ok && fd >= 0 && fs_read(fd, got, (4 << 20)) == n && !memcmp(exp, got, (size_t)n);
        if (fd >= 0) fs_close(fd);
        if (!ok) printf("  seed mismatch: %s\n", line);
        HCHECK(ok);
    }
    fclose(m);
    free(exp); free(got);
}

static void check_host_only(void)
{
    SceIoStat st;
    SceIoDirent d;
    section("timestamps");                                   /* the stub clock: 2026-10-10 12:34:56 */
    HEQ(fst_write_file("ms0:/FATMSTEST/TIME.TXT", 1, 10), 0);
    HEQ(fs_getstat("ms0:/FATMSTEST/TIME.TXT", &st), 0);
    HCHECK(st.sce_st_ctime.year == 2026 && st.sce_st_ctime.month == 10 && st.sce_st_ctime.day == 10);
    HCHECK(st.sce_st_ctime.hour == 12 && st.sce_st_ctime.minute == 34 && st.sce_st_ctime.second == 56);
    HCHECK(!memcmp(&st.sce_st_mtime, &st.sce_st_ctime, sizeof st.sce_st_ctime));
    HCHECK(st.sce_st_atime.year == 2026 && st.sce_st_atime.day == 10 && st.sce_st_atime.hour == 0);

    section("kernel caller");                                /* no iofilemgr fd quota or bit-15 check */
    host_kcall = 1;
    int fds[12], nfd = 0, last = 0;
    HEQ(fst_write_file("ms0:/FATMSTEST/K.TXT", 2, 10), 0);
    while (nfd < 12 && (last = fs_open("ms0:/FATMSTEST/K.TXT", PSP_O_RDONLY, 0)) >= 0) fds[nfd++] = last;
    HEQ(nfd, 10);                                            /* FdBuf: 10 slots */
    HEQ(last, E_EMFILE);
    while (nfd) HEQ(fs_close(fds[--nfd]), 0);
    /* devctl 0x241D814 = 1 is NOT safe write-through: Sony's dirent put (0x1014) then writes only
       slots that are already dirty and never dirties one (asm 0x10a4), so directory updates made
       in mode 1 never reach the card. Only the switch itself is tested; never write in mode 1. */
    u32 one = 1, zero = 0, two = 2;
    HEQ(fs_devctl("ms0:", 0x0241D814, &one, 4, 0, 0), 0);
    HEQ(fs_devctl("ms0:", 0x0241D814, &zero, 4, 0, 0), 0);
    HEQ(fs_devctl("ms0:", 0x0241D814, &two, 4, 0, 0), E_EINVAL);
    host_kcall = 0;

    section("ioctl");
    int fd = fs_dopen("ms0:/FATMSTEST/LIST_U8"), n = 0;
    const char *pat = "*.txt";
    HCHECK(fd >= 0);
    HEQ(fs_ioctl(fd, 0x02415050, &pat, 4, 0, 0), 0);          /* dread wildcard */
    while (memset(&d, 0, sizeof d), fs_dread(fd, &d) > 0) n++;
    HEQ(n, 3);                                               /* UPPER.TXT lower.txt MiXeD.TxT */
    fs_dclose(fd);
    fd = fs_open("ms0:/FATMSTEST/TIME.TXT", PSP_O_RDONLY, 0);
    HEQ(fs_ioctl(fd, 0x0242D016, 0, 0, 0, 0), 0);
    HEQ(fs_ioctl(fd, 0x00208013, 0, 0, 0, 0), 0);            /* loader probes */
    HEQ(fs_ioctl(fd, 0x00208082, 0, 0, 0, 0), 0);
    HEQ(fs_ioctl(fd, 0x01234567, 0, 0, 0, 0), E_UNSUP);
    HEQ(fs_ioctl(fd, 0x02415050, &pat, 4, 0, 0), E_EINVAL);  /* wildcard needs a directory */
    fs_close(fd);
    host_kcall = 1;                                          /* the loader runs in kernel mode */
    HEQ(fs_devctl("ms0:", 0x00208813, 0, 0, 0, 0), 0);
    host_kcall = 0;

    section("user pointers");                                /* a user caller passing kernel addresses */
    host_k1 = 0x80000000u;
    fd = fs_dopen("ms0:/FATMSTEST");
    memset(&d, 0, sizeof d);
    d.d_private = (void *)0x88000000u;
    HEQ(fs_dread(fd, &d), E_EINVAL);
    fs_dclose(fd);
    const char *kp[2] = { (const char *)0x88000000u, "/FATMSTEST/X" };
    HEQ(fs_devctl("ms0:", 0x02415830, kp, 8, 0, 0), E_EINVAL);
    host_k1 = 0;

    section("remount");                                      /* eject + insert: every cache dropped */
    u32 cs = cluster_bytes();
    HEQ(fst_write_file("ms0:/FATMSTEST/REMOUNT.BIN", 2, 3 * cs + 5), 0);
    fd = fs_open("ms0:/FATMSTEST/TIME.TXT", PSP_O_RDONLY, 0);
    HCHECK(fd >= 0);
    HEQ(host_eject(), 0);
    HEQ(host_insert(), 0);
    char b[4];
    HCHECK(fs_read(fd, b, 4) < 0);                           /* fds die with the card */
    fs_close(fd);
    HEQ(fst_verify_file("ms0:/FATMSTEST/REMOUNT.BIN", 2, 3 * cs + 5), 0);
    HEQ(fst_verify_file("ms0:/FATMSTEST/SZ13.BIN", 113, 307201), 0);
    HEQ(fst_verify_file("ms0:/FATMSTEST/FRAG_C.BIN", 132, 20 * cs + 123), 0);

    section("write protect");
    host_wp = 1;
    HEQ(host_eject(), 0);
    HEQ(host_insert(), 0);
    HEQ(fs_open("ms0:/FATMSTEST/TIME.TXT", PSP_O_WRONLY, 0), E_EACCES);
    HEQ(fs_open("ms0:/FATMSTEST/WP.TXT", PSP_O_WRONLY | PSP_O_CREAT | PSP_O_TRUNC, 0777), E_EACCES);
    HEQ(fs_mkdir("ms0:/FATMSTEST/WPDIR", 0777), E_EACCES);
    HEQ(fs_remove("ms0:/FATMSTEST/TIME.TXT"), E_EACCES);
    HEQ(fs_rmdir("ms0:/FATMSTEST/D1X"), E_EACCES);
    HEQ(fs_rename("ms0:/FATMSTEST/TIME.TXT", "T2.TXT"), E_EACCES);
    memset(&st, 0, sizeof st);
    st.st_attr = 0x20;
    HEQ(fs_chstat("ms0:/FATMSTEST/TIME.TXT", &st, 2), E_EACCES);
    HEQ(fst_verify_file("ms0:/FATMSTEST/TIME.TXT", 1, 10), 0);   /* reads still work */
    HEQ(host_illegal_writes, 0);
    host_wp = 0;
    HEQ(host_eject(), 0);
    HEQ(host_insert(), 0);
}

/* exFAT paths the shared suite can't reach: files and directories fuse-exfat wrote contiguously
   (NoFatChain) being overwritten, grown (-> FAT chain) and freed. fsck.exfat checks the result. */
static void exfat_extras(void)
{
    const char *c = "ms0:/SEED/C.BIN", *b = "ms0:/SEED/B.BIN", *dir = "ms0:/SEED/Long Seed Dir";
    u32 cs = cluster_bytes();
    SceIoStat st;
    char path[256];
    section("exfat contiguous");
    HEQ(fs_getstat(c, &st), 0);
    u32 n = (u32)st.st_size, add = 3 * cs + 5;
    u8 *a = malloc(n + add), *got = malloc(n + add);
    int fd = fs_open(c, PSP_O_RDONLY, 0);
    HEQ(fs_read(fd, a, (int)n), (int)n);
    fs_close(fd);
    fst_fill(a + 100, 77, 0, 1000);                           /* overwrite inside: stays contiguous */
    fd = fs_open(c, PSP_O_WRONLY, 0);
    HEQ(fs_lseek(fd, 100, PSP_SEEK_SET), 100);
    HEQ(fs_write(fd, a + 100, 1000), 1000);
    fs_close(fd);
    fst_fill(a + n, 78, 0, add);                              /* grow: becomes a FAT chain */
    fd = fs_open(c, PSP_O_WRONLY | PSP_O_APPEND, 0);
    HEQ(fs_write(fd, a + n, (int)add), (int)add);
    fs_close(fd);
    fd = fs_open(c, PSP_O_RDONLY, 0);
    HEQ(fs_read(fd, got, (int)(n + add)), (int)(n + add));
    fs_close(fd);
    HCHECK(!memcmp(a, got, n + add));
    free(a); free(got);
    HEQ(fs_getstat(b, &st), 0);                               /* freeing a contiguous file */
    u32 f0 = free_clusters(), bc = ((u32)st.st_size + cs - 1) / cs;
    HEQ(fs_remove(b), 0);
    HEQ(free_clusters(), f0 + bc);
    for (int i = 0; i < 150; i++) {                           /* grow a contiguous directory */
        snprintf(path, sizeof path, "%s/grown directory entry %03d.bin", dir, i);
        HEQ(fst_write_file(path, 500 + (u32)i, (u32)i), 0);
    }
    int dfd = fs_dopen(dir), k = 0;
    SceIoDirent d;
    while (memset(&d, 0, sizeof d), fs_dread(dfd, &d) > 0) k++;
    fs_dclose(dfd);
    HEQ(k, 150 + 1 + 2);                                      /* + the seeded file, ".", ".." */
    for (int i = 0; i < 150; i += 7) {
        snprintf(path, sizeof path, "%s/grown directory entry %03d.bin", dir, i);
        HEQ(fst_verify_file(path, 500 + (u32)i, (u32)i), 0);
    }
}

/* ES (phase 4). The card (tests/run_host.sh "es"): a FAT32 or exFAT partition holding ISO/real.iso
   and ISO/dup.iso, then an ES partition holding ISO/game1.iso, ISO/dup.iso (other bytes),
   ISO/Genre/game2.iso, ISO/Genre/Sub/game3.iso, ROOTFILE.ISO and an inactive ISO/gone.iso. The
   adapter reports the FS partition's end, so the ES reads also need the large-card patch. */
static long load(const char *dir, const char *name, u8 *buf)
{
    char p[512];
    snprintf(p, sizeof p, "%s/%s", dir, name);
    FILE *f = fopen(p, "rb");
    long n = f ? (long)fread(buf, 1, 4 << 20, f) : -1;
    if (f) fclose(f);
    return n;
}
static int same_file(const char *path, const u8 *want, long n)
{
    static u8 got[4 << 20];
    int fd = fs_open(path, PSP_O_RDONLY, 0);
    if (fd < 0) return fd;
    int r = fs_read(fd, got, (4 << 20));
    fs_close(fd);
    return (r == n && !memcmp(got, want, (size_t)n)) ? 0 : 1;
}
static int listed(const char *dir, const char *name, int dirent_kind, int es_date)
{
    SceIoDirent d;
    int fd = fs_dopen(dir), hits = 0;
    if (fd < 0) return fd;
    while (memset(&d, 0, sizeof d), fs_dread(fd, &d) > 0)
        if (!strcasecmp(d.d_name, name) && ((d.d_stat.st_mode & 0x1000) != 0) == dirent_kind &&   /* mtools: REAL.ISO */
            (d.d_stat.sce_st_mtime.year == 1992) == es_date) hits++;
    fs_dclose(fd);
    return hits;
}
static void es_tests(const char *dir)
{
    static u8 g1[4 << 20], dupes[4 << 20], dupfs[4 << 20], g2[4 << 20], g3[4 << 20], rootf[4 << 20];
    long n1 = load(dir, "game1.bin", g1), nde = load(dir, "dup_es.bin", dupes), ndf = load(dir, "dup_fs.bin", dupfs);
    long n2 = load(dir, "game2.bin", g2), n3 = load(dir, "game3.bin", g3), nr = load(dir, "root.bin", rootf);
    SceIoStat st;
    section("es files");
    HEQ(same_file("ms0:/ISO/game1.iso", g1, n1), 0);
    HEQ(same_file("ms0:/iso/GENRE/game2.ISO", g2, n2), 0);              /* any case */
    HEQ(same_file("ms0:/ISO/Genre/Sub/game3.iso", g3, n3), 0);
    HEQ(same_file("ms0:/ROOTFILE.ISO", rootf, nr), 0);
    HEQ(same_file("ms0:/ISO/dup.iso", dupes, nde), 0);                  /* ES first for reads */
    HEQ(fs_open("ms0:/ISO/gone.iso", PSP_O_RDONLY, 0), E_ENOENT);       /* inactive */
    HEQ(fs_getstat("ms0:/ISO/game1.iso", &st), 0);
    HEQ(st.st_size, n1);
    HEQ(st.st_mode, 0x21ff);
    HCHECK(st.sce_st_mtime.year == 1992 && st.sce_st_mtime.month == 3 && st.sce_st_mtime.day == 27);
    int fd = fs_open("ms0:/ISO/game1.iso", PSP_O_RDONLY, 0);
    static u8 b[70000];
    u32 seed = 1, bad = 0;
    for (int i = 0; i < 200; i++) {                                      /* unaligned offsets and buffers */
        seed = seed * 1103515245u + 12345u;
        u32 off = (seed >> 8) % (u32)n1, len = (seed >> 3) % 65536, bo = seed % 64;
        if (off + len > (u32)n1) len = (u32)n1 - off;
        fs_lseek(fd, off, PSP_SEEK_SET);
        if (fs_read(fd, b + bo, (int)len) != (int)len || memcmp(b + bo, g1 + off, len)) bad++;
    }
    HEQ(bad, 0);
    HEQ(fs_lseek(fd, n1 + 5000, PSP_SEEK_SET), n1 + 5000);
    HEQ(fs_read(fd, b, 100), 0);                                         /* past the end: nothing */
    HEQ(fs_write(fd, b, 10), E_IO_BADFD);                                /* read-only handle */
    HEQ(fs_ioctl(fd, 0x01234567, 0, 0, 0, 0), 0);                        /* ES files accept any ioctl */
    HEQ(fs_remove("ms0:/ISO/game1.iso"), E_EACCES);                      /* open */
    fs_close(fd);
    HEQ(fs_open("ms0:/ISO/game1.iso", PSP_O_WRONLY, 0), E_ENOENT);      /* writes only see the filesystem */
    fd = fs_open("ms0:/ISO/dup.iso", PSP_O_RDONLY | PSP_O_WRONLY, 0);   /* RDWR: the filesystem copy */
    HCHECK(fd >= 0);
    HEQ(fs_read(fd, b, (int)ndf), (int)ndf);
    HCHECK(!memcmp(b, dupfs, (size_t)ndf));
    fs_close(fd);

    section("es listing");
    HEQ(listed("ms0:/ISO", "game1.iso", 0, 1), 1);
    HEQ(listed("ms0:/ISO", "dup.iso", 0, 1), 1);                         /* the ES copy ... */
    HEQ(listed("ms0:/ISO", "dup.iso", 0, 0), 1);                         /* ... and the filesystem's */
    HEQ(listed("ms0:/ISO", "real.iso", 0, 0), 1);
    HEQ(listed("ms0:/ISO", "Genre", 1, 1), 1);                           /* only ES has it */
    HEQ(listed("ms0:/ISO", "gone.iso", 0, 1), 0);
    HEQ(listed("ms0:/ISO/Genre", "game2.iso", 0, 1), 1);                 /* an ES-only directory */
    HEQ(listed("ms0:/ISO/Genre", "Sub", 1, 1), 1);
    HEQ(listed("ms0:/ISO/Genre/Sub", "game3.iso", 0, 1), 1);
    HEQ(listed("ms0:/", "ROOTFILE.ISO", 0, 1), 1);
    HEQ(listed("ms0:/", "ISO", 1, 0), 1);                                /* the real one, once */
    HEQ(listed("ms0:/", "ISO", 1, 1), 0);
    HEQ(fs_getstat("ms0:/ISO/Genre", &st), 0);
    HEQ(st.st_mode, 0x11ff);
    HEQ(fs_dopen("ms0:/ISO/Nope"), E_ENOENT);
    SceIoDirent d;
    const char *pat = "*.iso";
    int n = 0;
    fd = fs_dopen("ms0:/ISO");
    HEQ(fs_ioctl(fd, 0x02415050, &pat, 4, 0, 0), 0);
    while (memset(&d, 0, sizeof d), fs_dread(fd, &d) > 0) n++;
    fs_dclose(fd);
    HEQ(n, 4);                                                           /* real, dup (fs), game1, dup (es) */

    section("es remove");
    HEQ(fs_remove("ms0:/ISO/dup.iso"), 0);                               /* the filesystem copy first */
    HEQ(same_file("ms0:/ISO/dup.iso", dupes, nde), 0);
    HEQ(fs_remove("ms0:/ISO/dup.iso"), 0);                               /* then the ES entry */
    HEQ(fs_getstat("ms0:/ISO/dup.iso", &st), E_ENOENT);
    HEQ(host_eject(), 0);
    HEQ(host_insert(), 0);                                               /* the table on the card agrees */
    HEQ(fs_getstat("ms0:/ISO/dup.iso", &st), E_ENOENT);
    HEQ(same_file("ms0:/ISO/game1.iso", g1, n1), 0);

    section("es devctl");
    struct { u32 init, start, total, count, data, next; } info;
    memset(&info, 0, sizeof info);
    HEQ(fs_devctl("fatms0:", 0x04100003, 0, 0, &info, sizeof info), 0);
    HEQ(info.init, 1);
    HEQ(info.count, 6);
    HEQ(info.data, 256);
    HCHECK(info.start > host_part_lba && info.next > info.data && info.next <= info.total);
    static u8 sec[1024] __attribute__((aligned(64)));
    struct { u32 sector, count; void *buf; } a = { info.start, 1, sec };
    HEQ(fs_devctl("fatms0:", 0x04100001, &a, sizeof a, 0, 0), 0);
    HEQ(sec[0] | sec[1] << 8 | sec[2] << 16 | (u32)sec[3] << 24, 0x44505345u);   /* the superblock */
    a.buf = sec + 3;                                                      /* unaligned buffer */
    HEQ(fs_devctl("fatms0:", 0x04100001, &a, sizeof a, 0, 0), 0);
    HEQ(sec[3], 0x45);
    memset(sec, 0x5a, 512);
    a.sector = info.start + 201; a.buf = sec;                             /* the reserved area */
    HEQ(fs_devctl("fatms0:", 0x04100002, &a, sizeof a, 0, 0), 0);
    memset(sec, 0, 512);
    HEQ(fs_devctl("fatms0:", 0x04100001, &a, sizeof a, 0, 0), 0);
    HEQ(sec[100], 0x5a);
    host_k1 = 0x80000000u;                                                /* a user caller ... */
    a.buf = (void *)0x88000000u;                                          /* ... passing kernel memory */
    HEQ(fs_devctl("fatms0:", 0x04100001, &a, sizeof a, 0, 0), E_EINVAL);
    host_k1 = 0;
    u32 ver = 0;
    HEQ(fs_devctl("fatms0:", 0x04100007, 0, 0, &ver, 4), 0);
    HEQ(ver, 0x0200);
    HEQ(fs_devctl("fatms0:", 0x04100004, 0, 0, 0, 0), 0);                 /* rescan */
    HEQ(same_file("ms0:/ISO/Genre/game2.iso", g2, n2), 0);
    HEQ(fs_devctl("fatms0:", 0x04100005, 0, 0, 0, 0), E_EINVAL);          /* format needs its argument */
}

/* Phase 5: formatting. On the "es" card (FAT32 + ES) or a superfloppy card: the ES app's format to
   exFAT and to FAT32 (0x04100005), XMB's format (0x0241D813: keeps the card's type), each followed by
   Sony's remount. The ES ISOs must stay readable and the new filesystem must work (a write + the
   scenario suite). The images are saved for fsck / fuse-exfat / mtools and an ES-area checksum. */
static int fmt(int how)                                     /* 0/1: ES app FAT32/exFAT, -1: XMB */
{
    u32 m = host_mounts;
    int r;
    if (how >= 0) r = fs_devctl("fatms0:", 0x04100005, &how, 4, 0, 0);
    else {
        static const char zero = 0;
        const char *p = &zero;                              /* XMB: a pointer to a 0 byte (sysconf_plugin) */
        host_kcall = 1;                                     /* vshIoDevctl: a kernel-mode call */
        r = fs_devctl("fatms0:", 0x0241D813, &p, 4, 0, 0);
        host_kcall = 0;
    }
    if (r < 0) return r;
    return host_remounted(m) < 0 ? -1 : (cluster_bytes() ? 0 : -2);   /* remounted + identified */
}
static void format_tests(const char *dir, const char *out)
{
    static u8 g1[4 << 20], g2[4 << 20];
    long n1 = load(dir, "game1.bin", g1), n2 = load(dir, "game2.bin", g2);
    struct { u32 init, start, total, count, data, next; } info = { 0, 0, 0, 0, 0, 0 };
    fs_devctl("fatms0:", 0x04100003, 0, 0, &info, sizeof info);
    int es = info.init;
    static const struct { const char *name; int how, exfat; } steps[] = {
        { "format exfat (ES app)", 1, 1 }, { "format xmb (stays exfat)", -1, 1 },
        { "format fat32 (ES app)", 0, 0 }, { "format xmb (stays fat32)", -1, 0 },
    };
    char path[512];
    SceIoStat st;
    for (int i = 0; i < 4; i++) {
        section(steps[i].name);
        HEQ(fmt(steps[i].how), 0);
        HEQ(g_ctx.exfat, steps[i].exfat);
        if (!steps[i].exfat) HEQ(g_ctx.fat_type, 32);
        HEQ(fs_getstat("ms0:/FMT.BIN", &st), E_ENOENT);      /* the previous filesystem is gone */
        HEQ(fs_getstat("ms0:/FATMSTEST", &st), E_ENOENT);
        u32 o[5] = { 0 }, *p = o;
        fs_devctl("ms0:", 0x02425818, &p, 4, 0, 0);
        if (steps[i].exfat) HCHECK(o[1] < o[0] && o[0] - o[1] <= 64);   /* bitmap, upcase table, root */
        else HEQ(o[1], o[0] - 1);                                         /* only the root cluster */
        if (es) {
            HEQ(same_file("ms0:/ISO/game1.iso", g1, n1), 0);
            HEQ(same_file("ms0:/ISO/Genre/game2.iso", g2, n2), 0);
        }
        HEQ(fst_write_file("ms0:/FMT.BIN", 700 + (u32)i, 3 * cluster_bytes() + 77), 0);
        HEQ(fst_verify_file("ms0:/FMT.BIN", 700 + (u32)i, 3 * cluster_bytes() + 77), 0);
        if (i == 0 || i == 2) {                              /* the new filesystem passes the whole suite */
            fst_run("ms0:/FATMSTEST", FST_DETERMINISTIC, &R);
        }
        HEQ(host_eject(), 0);
        snprintf(path, sizeof path, "%s.%d.img", out, i);
        HEQ(host_disk_save(path), 0);
        HEQ(host_insert(), 0);
    }
}

/* walk: XMB's view of a real card (research/cardscan/dump_card.ps1): list every directory, stat
   every entry, read every file (the first 64 KB of big ones). Read-only. */
static void walk(const char *path, int depth)
{
    static u8 buf[0x10000];
    SceIoDirent d;
    int fd = fs_dopen(path), n = 0, seen = 0, r = 0;
    char (*kids)[256] = malloc(1024 * 256), c[512];
    HCHECK(fd >= 0 && depth < 32);
    if (fd < 0 || depth >= 32) { if (fd >= 0) fs_dclose(fd); free(kids); return; }
    while (memset(&d, 0, sizeof d), (r = fs_dread(fd, &d)) > 0) {
        if (++seen > 100000) { HCHECK(!"directory never ends"); break; }   /* a cluster chain cycle */
        if (!strcmp(d.d_name, ".") || !strcmp(d.d_name, "..")) continue;
        snprintf(c, sizeof c, "%s/%s", path, d.d_name);
        SceIoStat st;
        memset(&st, 0, sizeof st);
        HEQ(fs_getstat(c, &st), 0);
        int dir = (d.d_stat.st_mode & 0xf000) == 0x1000;
        printf("  %s%s  %lld\n", c, dir ? "/" : "", (long long)d.d_stat.st_size);
        if (dir) { if (n < 1024) strcpy(kids[n++], d.d_name); continue; }
        int f = fs_open(c, PSP_O_RDONLY, 0), k = 0;
        long long got = 0, want = st.st_size > 0x800000 ? 0x10000 : st.st_size;   /* stat: ES copy first */
        HCHECK(f >= 0);
        if (f < 0) continue;
        while (got < want && (k = fs_read(f, buf, sizeof buf)) > 0) got += k;
        HCHECK(k >= 0 && got >= want);
        fs_close(f);
    }
    HEQ(r, 0);
    fs_dclose(fd);
    for (int i = 0; i < n; i++) { snprintf(c, sizeof c, "%s/%s", path, kids[i]); walk(c, depth + 1); }
    free(kids);
}

/* files left on the card for mtools to read back (tests/run_host.sh) */
static void survivors(const char *out)
{
    static const char *names[] = { "EMPTY.BIN", "ONE.BIN", "Sector Plus One.bin", "three clusters.dat",
                                   "Sub Dir/deep file name.bin", "BIG100K.BIN" };
    u32 cs = cluster_bytes(), sizes[] = { 0, 1, 513, 3 * cs + 17, 4321, 100000 };
    char path[640], dir[512];
    section("survivors");
    HEQ(fs_mkdir("ms0:/SURVIVE", 0777), 0);
    HEQ(fs_mkdir("ms0:/SURVIVE/Sub Dir", 0777), 0);
    snprintf(path, sizeof path, "%s.survive.txt", out);
    snprintf(dir, sizeof dir, "%s.survive", out);
    mkdir(dir, 0777);
    FILE *m = fopen(path, "w");
    u8 *buf = malloc(sizes[3] > sizes[5] ? sizes[3] : sizes[5]);         /* 3 clusters may pass 100000 */
    for (int i = 0; i < 6; i++) {
        snprintf(path, sizeof path, "ms0:/SURVIVE/%s", names[i]);
        HEQ(fst_write_file(path, 900 + (u32)i, sizes[i]), 0);
        fst_fill(buf, 900 + (u32)i, 0, sizes[i]);
        snprintf(path, sizeof path, "%s/%d.bin", dir, i);
        FILE *f = fopen(path, "wb");
        fwrite(buf, 1, sizes[i], f);
        fclose(f);
        fprintf(m, "::/SURVIVE/%s\t%s\n", names[i], path);
    }
    free(buf);
    fclose(m);
}

static void fill_tests(void)
{
    char path[64];
    int r = 0, n = 0;
    section("root full");
    HCHECK(cluster_bytes() != 0);
    if (g_ctx.fat_type != 32) {                              /* FAT12/16: fixed root directory */
        for (n = 0; n < 1000; n++) {
            snprintf(path, sizeof path, "ms0:/R%03d.TXT", n);
            if ((r = fst_write_file(path, (u32)n, 1)) < 0) break;
        }
        HEQ(r, E_EACCES);                                    /* 0x8001000D: no room in the root */
        HCHECK(n > 0 && n <= g_ctx.root_entries);
        for (int i = 0; i < n; i++) { snprintf(path, sizeof path, "ms0:/R%03d.TXT", i); HEQ(fs_remove(path), 0); }
    }
    section("card full");
    u32 f0 = free_clusters(), total = 0;
    int fd = fs_open("ms0:/FULL.BIN", PSP_O_WRONLY | PSP_O_CREAT | PSP_O_TRUNC, 0777);
    u8 *buf = malloc(65536);
    memset(buf, 0x3c, 65536);
    for (;;) {
        r = fs_write(fd, buf, 65536);
        if (r > 0) total += (u32)r;
        if (r != 65536) break;
    }
    HCHECK(r >= 0 && r < 65536);                             /* a short write, not an error */
    HEQ(fs_write(fd, buf, 65536), 0);                        /* nothing more fits */
    fs_close(fd);
    HEQ(free_clusters(), 0);
    SceIoStat st;
    HEQ(fs_getstat("ms0:/FULL.BIN", &st), 0);
    HEQ(st.st_size, total);
    HEQ(fs_remove("ms0:/FULL.BIN"), 0);
    HEQ(free_clusters(), f0);
    free(buf);
}

/* Phase 1: an SD adapter whose attribute capacity (host_attr_cap) ends before the MBR partition.
   variant "patch": the driver must raise msstor's size to the partition end, keep it across msstor
   card setups (the patched getter) and re-inserts, and the whole partition must be usable.
   "guard": msstor's code is not Sony 6.61's -> nothing patched, card usable up to the adapter's size.
   "nolower": a real Memory Stick (attribute size past the partition) -> msstor's size unchanged. */
static u32 mbr_end(void)
{
    u32 end = 0;
    for (int i = 0; i < 4; i++) {
        const u8 *e = host_disk + 0x1be + i * 16;
        u32 lba = (u32)(e[8] | e[9] << 8 | e[10] << 16 | (u32)e[11] << 24), n = (u32)(e[12] | e[13] << 8 | e[14] << 16 | (u32)e[15] << 24);
        if (e[4] && n && lba + n > end) end = lba + n;
    }
    return end;
}
static void bigcard_tests(const char *variant)
{
    static const u32 sony[4] = { 0x27bdfff0, 0xafbf0000 };
    u32 end = mbr_end(), attr = host_attr_cap, cs = cluster_bytes();
    section("large card");
    HEQ(host_ms0_opens, 0);                                  /* the MBR read closed its msstor0: handle */
    if (!strcmp(variant, "nolower")) { HEQ(host_ms_capacity(), attr); HEQ(host_ms_text[0x3334 / 4], sony[0]); return; }
    if (!strcmp(variant, "guard")) {
        HEQ(host_ms_capacity(), attr);                       /* untouched */
        HEQ(host_ms_text[0x3334 / 4], sony[0]);
        HEQ(fst_write_file("ms0:/SMALL.BIN", 7, 3 * cs + 5), 0);
        HEQ(fst_verify_file("ms0:/SMALL.BIN", 7, 3 * cs + 5), 0);
        HEQ(fs_remove("ms0:/SMALL.BIN"), 0);
        return;
    }
    HEQ(host_ms_capacity(), end);                            /* the patched getter */
    HCHECK(host_ms_text[0x3334 / 4] != sony[0] && host_ms_text[0x333c / 4] == 0x03e00008u);
    u32 f0 = free_clusters(), total = 0;
    int fd = fs_open("ms0:/FILL.BIN", PSP_O_WRONLY | PSP_O_CREAT | PSP_O_TRUNC, 0777), r;
    u8 *buf = malloc(65536);
    for (u32 off = 0;; off += (u32)r) {                      /* fill the card: the last clusters lie past the adapter's size */
        fst_fill(buf, 31, off, 65536);
        if ((r = fs_write(fd, buf, 65536)) > 0) total += (u32)r;
        if (r != 65536) break;
    }
    fs_close(fd);
    free(buf);
    HCHECK(r >= 0);
    HEQ(free_clusters(), 0);
    HCHECK(total > (u64)(attr - host_part_lba) * 512);       /* data went past the adapter's size */
    HEQ(fst_verify_file("ms0:/FILL.BIN", 31, total), 0);
    host_ms_setup();                                         /* msstor card setup resets its field ... */
    HEQ(host_ms_capacity(), end);                            /* ... the patched getter keeps the size */
    HEQ(fst_verify_file("ms0:/FILL.BIN", 31, total), 0);
    u32 w0 = host_ms_text[0x3334 / 4];
    HEQ(host_eject(), 0);
    HEQ(host_insert(), 0);                                   /* re-insert: same patch, size refreshed */
    HEQ(host_ms_text[0x3334 / 4], w0);
    HEQ(host_ms_capacity(), end);
    HEQ(host_ms0_opens, 0);
    HEQ(fst_verify_file("ms0:/FILL.BIN", 31, total), 0);
    HEQ(fs_remove("ms0:/FILL.BIN"), 0);
    HEQ(free_clusters(), f0);
}

#ifdef PSPGO
int module_start(SceSize args, void *argp);
/* ---------------- PSP Go (fatmsef.prx): ms0: + ef0: + eh0: ----------------
 * Expected bytes come from Sony's fatmsef (research/docs/PSPGO_STORAGE_ANALYSIS.md), not from
 * fatfs_ef.c, so a slip in the port shows up here. */
extern int sceFatfs_driver_C8537E11(void), sceFatfs_driver_5DEB8124(void), sceFatfs_driver_3991C1A9(void);
static const char *EFIMG[2];                                     /* the two eflash image files */
static int drv_is(const char *dev, const char *name) { const char *d = host_assigned_drv(dev); return d && !strcmp(d, name); }
static int dinfo(const char *dev, u32 *o) { u32 *p = o; memset(o, 0, 20); return fs_devctl(dev, 0x02425818, &p, 4, 0, 0); }
static void img_sector(int i, u32 lba, u8 *b)
{
    FILE *f = fopen(EFIMG[i], "rb");
    memset(b, 0xee, 512);
    if (f) { fseeko(f, (off_t)lba * 512, SEEK_SET); if (fread(b, 1, 512, f) != 512) {} fclose(f); }
}
static u32 g16(const u8 *p) { return (u32)(p[0] | p[1] << 8); }
static u32 g32(const u8 *p) { return g16(p) | g16(p + 2) << 16; }
static int all(const u8 *b, int from, int n, u8 v) { for (int i = from; i < from + n; i++) if (b[i] != v) return 0; return 1; }

static void go_roundtrip(const char *path, u32 seed, u32 size)
{
    HEQ(fst_write_file(path, seed, size), 0);
    HEQ(fst_verify_file(path, seed, size), 0);
}
static void go_volumes(void)
{
    u32 o[5];
    section("go: volumes");
    HCHECK(drv_is("ms0:", "fatms") && drv_is("ef0:", "fatef") && drv_is("eh0:", "fateh"));
    HCHECK(g_ctx.ops == &g_ms_ops && g_ctx_ef.ops == &g_ef_ops && g_ctx_eh.ops == &g_ef_ops);
    HCHECK(g_ctx.mutex != g_ctx_ef.mutex && g_ctx_ef.mutex != g_ctx_eh.mutex && g_ctx.mutex != g_ctx_eh.mutex);
    HEQ(host_cp.calls, 1);                                       /* codepage tables once, for the first volume */
    HEQ(g_ctx_ef.flags & (ST_OPEN | ST_ASSIGNED | ST_MOUNTED), ST_OPEN | ST_ASSIGNED | ST_MOUNTED);
    HEQ(g_ctx_eh.flags & (ST_OPEN | ST_ASSIGNED | ST_MOUNTED), ST_OPEN | ST_ASSIGNED | ST_MOUNTED);
    HEQ(dinfo("ef0:", o), 0); HCHECK(o[0] > 0 && o[3] == 512);
    HEQ(dinfo("eh0:", o), 0); HCHECK(o[0] > 0 && o[3] == 512);
    HEQ(g_ctx_ef.ms_sectors_per_cluster, 1);                     /* no MS attributes on internal storage */
    HEQ(g_ctx_ef.ms_unit, 0xffff);
    HEQ(host_ef_opens(0), 1); HEQ(host_ef_opens(1), 1);
}
static void go_isolation(void)
{
    SceIoStat st;
    section("go: isolation");
    go_roundtrip("ms0:/GO.BIN", 21, 70000);
    go_roundtrip("ef0:/GO.BIN", 22, 50000);
    go_roundtrip("eh0:/GO.BIN", 23, 3000);
    HEQ(fst_verify_file("ms0:/GO.BIN", 21, 70000), 0);           /* each volume kept its own */
    HEQ(fst_verify_file("ef0:/GO.BIN", 22, 50000), 0);
    HEQ(fst_verify_file("eh0:/GO.BIN", 23, 3000), 0);
    HEQ(fs_getstat("ms0:/ISO/game1.iso", &st), 0);               /* ES: the Memory Stick's, not internal storage's */
    HEQ(fs_getstat("ef0:/ISO/game1.iso", &st), E_ENOENT);
    HEQ(fs_remove("ef0:/GO.BIN"), 0);
    HEQ(fs_remove("eh0:/GO.BIN"), 0);
    HEQ(fs_remove("ms0:/GO.BIN"), 0);
}
static void go_devctls(void)
{
    u32 v = 7;
    section("go: devctls");
    HEQ(fs_devctl("fatef0:", 0x02425824, 0, 0, &v, 4), 0); HEQ(v, 0);   /* no write-protect switch */
    HEQ(fs_devctl("fatef0:", 0x02425825, 0, 0, &v, 4), 0); HEQ(v, 1);   /* always writable */
    HEQ(fs_devctl("fatef0:", 0x02425823, 0, 0, &v, 4), 0); HEQ(v, 1);   /* assigned */
    int cb = fs_cb_create();
    HEQ(fs_devctl("fateh0:", 0x02415821, &cb, 4, 0, 0), 0);
    HEQ(fs_cb_seen(cb), 1);                                          /* told "assigned" at once */
    HEQ(fs_devctl("fateh0:", 0x02415822, &cb, 4, 0, 0), 0);
    HEQ(fs_devctl("fateh0:", 0x02415822, &cb, 4, 0, 0), E_EINVAL);
    HEQ(fs_devctl("fatef0:", 0x04100007, 0, 0, &v, 4), E_UNSUP);    /* ES: Memory Stick only */
    HEQ(fs_devctl("fatms0:", 0x04100007, 0, 0, &v, 4), 0);
    HEQ(v, 0x0200);
    HEQ(fs_devctl("fatef0:", 0x02400000, 0, 0, 0, 0), E_UNSUP);
}
static void go_usb(void)
{
    u8 b[64];
    section("go: USB mode (vshbridge)");
    HEQ(sceFatfs_driver_3991C1A9(), 0);                              /* let go of internal storage */
    HEQ(g_ctx_ef.flags & ST_OPEN, 0); HEQ(g_ctx_eh.flags & ST_OPEN, 0);
    HEQ(host_ef_opens(0), 0); HEQ(host_ef_opens(1), 0);
    HEQ(g_ctx.flags & ST_OPEN, ST_OPEN);                             /* the Memory Stick stays */
    HEQ(sceFatfs_driver_5DEB8124(), 0);                              /* and take it back */
    HEQ(g_ctx_ef.flags & ST_OPEN, ST_OPEN); HEQ(g_ctx_eh.flags & ST_OPEN, ST_OPEN);
    HEQ(host_ef_opens(0), 1); HEQ(host_ef_opens(1), 1);
    go_roundtrip("ef0:/AFTER.BIN", 31, 9000);
    int fd = fs_open("ef0:/AFTER.BIN", PSP_O_RDONLY, 0);
    HCHECK(fd >= 0 && fs_read(fd, b, 64) == 64);
    if (fd >= 0) fs_close(fd);
    HEQ(fs_remove("ef0:/AFTER.BIN"), 0);
}
static void go_sysevents(void)
{
    int (*h)(int, char *, void *, int *) = host_sysev->handler;
    u32 rec[2] = { 0, 0x80000000u }, plain[2] = { 0, 0 };
    void *changed[2] = { 0, rec }, *same[2] = { 0, plain };
    section("go: sysevents");
    host_ms_status = 4;
    HEQ(h(0x400, 0, 0, 0), 0);                                       /* internal storage drops its assign */
    HEQ(g_ctx_ef.flags & ST_ASSIGNED, 0); HEQ(g_ctx_eh.flags & ST_ASSIGNED, 0);
    HEQ(g_ctx.flags & ST_ASSIGNED, ST_ASSIGNED);                     /* the Memory Stick ignores 0x400 */
    HEQ(h(0x100002, 0, same, 0), 0);                                 /* back */
    HEQ(g_ctx_ef.flags & ST_ASSIGNED, ST_ASSIGNED);
    HEQ(g_ctx_ef.media_flags & (MF_REMOVED | MF_INSERTED), 0);
    go_roundtrip("ef0:/SE.BIN", 41, 5000);
    HEQ(h(0x100002, 0, changed, 0), 0);                              /* "the storage changed": re-identify */
    HEQ(g_ctx_ef.media_flags & (MF_REMOVED | MF_INSERTED), MF_REMOVED | MF_INSERTED);
    HEQ(fst_verify_file("ef0:/SE.BIN", 41, 5000), 0);
    HEQ(fs_remove("ef0:/SE.BIN"), 0);
    HEQ(h(0x102, 0, 0, 0), 0);
    HCHECK((g_ctx_ef.flags & ST_SUSPEND) && (g_ctx_eh.flags & ST_SUSPEND) && (g_ctx.flags & ST_SUSPEND));
    HEQ(h(0x10000, 0, 0, 0), 0);
    HCHECK(!(g_ctx_ef.flags & ST_SUSPEND) && !(g_ctx_eh.flags & ST_SUSPEND) && !(g_ctx.flags & ST_SUSPEND));
    section("go: reboot phase");
    HEQ(module_reboot_phase(2, 0, 0, 0), 0);
    HCHECK(host_mutex_count(g_ctx.mutex) == 1 && host_mutex_count(g_ctx_ef.mutex) == 1 && host_mutex_count(g_ctx_eh.mutex) == 1);
    HEQ(module_reboot_phase(1, 0, 0, 0), 0);
    HCHECK(host_mutex_count(g_ctx.mutex) == 0 && host_mutex_count(g_ctx_ef.mutex) == 0 && host_mutex_count(g_ctx_eh.mutex) == 0);
}
static void go_tests(void)
{
    go_volumes();
    host_sdk = 0x06060110;
    fst_run("ef0:/FATMSTEST", FST_BIG, &R);                          /* the whole suite on internal storage */
    fst_run("eh0:/FATMSTEST", FST_NAMES_ONLY, &R);
    fst_run("ms0:/FATMSTEST", FST_NAMES_ONLY, &R);                   /* and the Memory Stick beside it */
    go_isolation();
    go_devctls();
    go_usb();
    go_sysevents();
}

/* ms0: is internal storage: a game started from it (boot medium 0x50, application type 0x200) */
static void goemu_tests(int msemu)
{
    SceIoStat st;
    section("go emulated: setup");
    if (!msemu) {                                                    /* msemu.prx missing: no aliases, no ms0: */
        HCHECK(!host_assigned_drv("ms0:"));
        HCHECK(drv_is("ef0:", "fatef"));
        HCHECK(g_ctx.flags & ST_ASSIGN_ERR);
        go_roundtrip("ef0:/X.BIN", 51, 1000);
        return;
    }
    HCHECK(drv_is("ms0:", "fatef"));                                 /* ms0: -> msstoremu0: + fatef0: */
    HCHECK(drv_is("ef0:", "fatef") && drv_is("eh0:", "fateh"));
    HCHECK(g_ctx.ops == &g_ef_ops);
    HCHECK(g_ctx.mutex == g_ctx_ef.mutex);                           /* one lock, one fd, one set of buffers */
    HCHECK(g_ctx.ctrl_fd == g_ctx_ef.ctrl_fd && g_ctx.fd[0] == g_ctx_ef.fd[0] && g_ctx.data_buf == g_ctx_ef.data_buf);
    HCHECK(fatfs_redirect() == &g_ctx);
    HEQ(g_ctx.flags & ST_MOUNTED, ST_MOUNTED);                       /* the alias assign mounted the fatms volume */
    HEQ(host_ms0_opens, 0);                                          /* the Memory Stick is never touched */
    HEQ(host_rd_calls, 0);
    HEQ(host_ef_opens(0), 1);
    section("go emulated: ms0: is ef0:");
    go_roundtrip("ms0:/EMU.BIN", 61, 40000);
    HEQ(fst_verify_file("ef0:/EMU.BIN", 61, 40000), 0);
    go_roundtrip("ef0:/EMU2.BIN", 62, 5000);
    HEQ(fst_verify_file("ms0:/EMU2.BIN", 62, 5000), 0);
    HEQ(fs_remove("ms0:/EMU2.BIN"), 0);
    HEQ(fs_getstat("ef0:/EMU2.BIN", &st), E_ENOENT);
    HEQ(fs_remove("ef0:/EMU.BIN"), 0);
    u32 v = 0;
    HEQ(fs_devctl("fatms0:", 0x04100007, 0, 0, &v, 4), E_UNSUP);    /* fatms0: is fatef0: now: no ES */
    host_sdk = 0x06060110;
    fst_run("ms0:/FATMSTEST", FST_BIG, &R);
    HEQ(host_rd_calls, 0);
    section("go emulated: unassign");
    HEQ(k_sceIoUnassign("ms0:"), 0);                                 /* umount goes to the fatms volume */
    HEQ(g_ctx.flags & ST_MOUNTED, 0);
    HEQ(g_ctx_ef.flags & ST_MOUNTED, ST_MOUNTED);
    HCHECK(drv_is("ef0:", "fatef"));
    go_roundtrip("ef0:/X.BIN", 52, 1000);
}

/* Sony's internal-storage format: layout bytes, then the volume works. which: 0 export, 1 devctl */
static void go_check_ef32(u32 total, u32 rsv, u32 fatsz, u32 zero_sectors)
{
    u8 b[512], k[512];
    img_sector(0, 0, b);
    HCHECK(b[0] == 0xeb && b[1] == 0x58 && b[2] == 0x90 && all(b, 3, 8, ' '));
    HEQ(g16(b + 0x0b), 512); HEQ(b[0x0d], 0x40); HEQ(g16(b + 0x0e), rsv); HEQ(b[0x10], 2);
    HEQ(g16(b + 0x11), 0); HEQ(g16(b + 0x13), 0); HEQ(b[0x15], 0xf8); HEQ(g16(b + 0x16), 0);
    HEQ(g16(b + 0x18), 0x3f); HEQ(g16(b + 0x1a), 0xff); HEQ(g32(b + 0x1c), 0x40); HEQ(g32(b + 0x20), total);
    HEQ(g32(b + 0x24), fatsz); HEQ(g32(b + 0x28), 0); HEQ(g32(b + 0x2c), 2); HEQ(g16(b + 0x30), 1); HEQ(g16(b + 0x32), 6);
    HEQ(b[0x40], 0); HEQ(b[0x42], 0x29); HEQ(g32(b + 0x43), 0);
    HCHECK(all(b, 0x47, 11, ' ') && !memcmp(b + 0x52, "FAT32   ", 8) && all(b, 0x5a, 0x1fe - 0x5a, 0));
    HCHECK(b[0x1fe] == 0x55 && b[0x1ff] == 0xaa);
    img_sector(0, 6, k); HCHECK(!memcmp(b, k, 512));              /* backup boot sector */
    for (u32 s = 1; s <= 7; s += 6) {
        img_sector(0, s, k);                                         /* FSInfo and its backup */
        HCHECK(g32(k) == 0x41615252 && g32(k + 0x1e4) == 0x61417272 && g32(k + 0x1e8) == 0xffffffff &&
               g32(k + 0x1ec) == 3 && k[0x1fe] == 0x55 && k[0x1ff] == 0xaa && all(k, 4, 0x1e0, 0));
    }
    for (u32 f = 0; f < 2; f++) {
        img_sector(0, rsv + f * fatsz, k);
        HCHECK(g32(k) == 0xfffffff8 && g32(k + 4) == 0x0fffffff && g32(k + 8) == 0x0fffffff && all(k, 12, 500, 0));
        img_sector(0, rsv + f * fatsz + 1, k); HCHECK(all(k, 0, 512, 0));
    }
    img_sector(0, rsv + 2 * fatsz, k); HCHECK(all(k, 0, 512, 0));   /* the root cluster */
    img_sector(0, zero_sectors - 1, k); HCHECK(all(k, 0, 512, 0));   /* the end of the cleared area */
}
static void go_check_eh16(void)
{
    u8 b[512], k[512];
    img_sector(1, 0, b);
    HCHECK(b[0] == 0xeb && b[1] == 0x58 && b[2] == 0x90 && all(b, 3, 8, ' '));
    HEQ(g16(b + 0x0b), 512); HEQ(b[0x0d], 0x40); HEQ(g16(b + 0x0e), 2); HEQ(b[0x10], 2);
    HEQ(g16(b + 0x11), 0x200); HEQ(g16(b + 0x13), 0); HEQ(b[0x15], 0xf8); HEQ(g16(b + 0x16), 15);
    HEQ(g16(b + 0x18), 0x20); HEQ(g16(b + 0x1a), 0x10); HEQ(g32(b + 0x1c), 0x40); HEQ(g32(b + 0x20), 0x1dfc0);
    HEQ(b[0x24], 0); HEQ(b[0x26], 0x29); HEQ(g32(b + 0x27), 0);
    HCHECK(all(b, 0x2b, 11, ' ') && !memcmp(b + 0x36, "FAT16   ", 8) && b[0x1fe] == 0x55 && b[0x1ff] == 0xaa);
    for (u32 f = 0; f < 2; f++) {
        img_sector(1, 2 + f * 15, k);
        HCHECK(k[0] == 0xf8 && k[1] == 0xff && k[2] == 0xff && k[3] == 0x7f && all(k, 4, 508, 0));
    }
    img_sector(1, 5 * 64 - 1, k); HCHECK(all(k, 0, 512, 0));
}
static void goformat_tests(int big)
{
    u8 b[512];
    section("go format: export (XMB)");
    HEQ(sceFatfs_driver_C8537E11(), 0);
    if (big) go_check_ef32(0x1d3dd80, 0x44, host_model == 5 ? 0xe9e : 0xe9f, 120 * 64);
    else go_check_ef32(0xe5ddc0, 0x22, 0x72f, 59 * 64);
    go_check_eh16();
    HCHECK(drv_is("ef0:", "fatef") && drv_is("eh0:", "fateh"));      /* the format's rescan mounted them */
    go_roundtrip("ef0:/F.BIN", 71, 100000);
    go_roundtrip("eh0:/F.BIN", 72, 2000);
    HEQ(g_ctx_ef.fat_type, 32);
    HEQ(g_ctx_eh.fat_type, 12);                                      /* Sony's eh0: has FAT12's cluster count */
    if (big) return;
    section("go format: devctl 0x241D813");
    char *pp = "x";
    host_kcall = 1;
    HEQ(fs_devctl("fatef0:", 0x0241D813, &pp, 4, 0, 0), 0);
    host_kcall = 0;
    SceIoStat st;
    HEQ(fs_getstat("ef0:/F.BIN", &st), E_ENOENT);                    /* empty again */
    HEQ(fst_verify_file("eh0:/F.BIN", 72, 2000), 0);                 /* eh0: untouched */
    go_roundtrip("ef0:/G.BIN", 73, 30000);
    section("go format: IdStorage layout");
    static const u8 salt[16] = { 0x58, 0xf4, 0xd3, 0xcc, 0x7f, 0x1c, 0x95, 0x01, 0x31, 0x75, 0xff, 0x1b, 0x6f, 0x3a, 0xeb, 0xb3 };
    static const u8 md5_abc[16] = { 0x90, 0x01, 0x50, 0x98, 0x3c, 0xd2, 0x4f, 0xb0, 0xd6, 0x96, 0x3f, 0x7d, 0x28, 0xe1, 0x7f, 0x72 };
    u8 d[16];
    sceKernelUtilsMd5Digest((u8 *)"abc", 3, d);
    HCHECK(!memcmp(d, md5_abc, 16));                                 /* the host's MD5 is MD5 */
    static const u32 ef_rec[8] = { 0xf8023b40, 0x00220200, 0x00ff003f, 0x00060001, 0x40, 0x00e00000, 0x72f, 2 };
    memset(host_idstorage, 0x5c, sizeof host_idstorage);
    static const u32 eh_rec[6] = { 0xf8020540, 0x00020200, 0x00100020, 0x000f0200, 0x40, 0x0001dfc0 };
    memcpy(host_idstorage + 0x104, ef_rec, 32);                      /* fatef: a smaller total than built in */
    memcpy(host_idstorage + 0x184, eh_rec, 24);
    memcpy(host_idstorage, salt, 16);
    sceKernelUtilsMd5Digest(host_idstorage, 0x200, d);
    memcpy(host_idstorage, d, 16);
    host_idstorage_ok = 1;
    HEQ(sceFatfs_driver_C8537E11(), 0);
    img_sector(0, 0, b);
    HEQ(g32(b + 0x20), 0x00e00000);                                  /* the record's */
    host_idstorage[0x20] ^= 1;                                       /* digest no longer matches: built-in layout */
    HEQ(sceFatfs_driver_C8537E11(), 0);
    img_sector(0, 0, b);
    HEQ(g32(b + 0x20), 0xe5ddc0);
    host_idstorage_ok = 0;
    go_roundtrip("ef0:/H.BIN", 74, 12345);
}
#endif

#ifdef PSPGO
/* fstest_host_go go|goformat|goformat16|goemu|goemu-nomsemu <ms image> <out> <ef image> <eh image> */
static int go_main(int argc, char **argv)
{
    const char *mode = argv[1];
    if (argc < 6) { fprintf(stderr, "usage: %s go... <ms image> <out> <ef image> <eh image>\n", argv[0]); return 2; }
    if (host_disk_load(argv[2])) { fprintf(stderr, "can't load %s\n", argv[2]); return 2; }
    EFIMG[0] = argv[4]; EFIMG[1] = argv[5];
    if (host_ef_load(0, argv[4]) || host_ef_load(1, argv[5])) { fprintf(stderr, "can't load the eflash images\n"); return 2; }
    section("boot");
    if (!strncmp(mode, "goemu", 5)) {
        host_bootfrom = 0x50; host_apptype = 0x200;                  /* a game started from internal storage */
        host_msemu = strcmp(mode, "goemu-nomsemu") != 0;
        HEQ(module_start(0, 0), 0);
        goemu_tests(host_msemu);
    } else {
        if (host_boot()) { printf("FAIL: the driver did not mount the Memory Stick\n"); return 1; }
        if (!strcmp(mode, "go")) go_tests();
        else if (!strcmp(mode, "goformat")) goformat_tests(0);
        else if (!strcmp(mode, "goformat16")) goformat_tests(1);
        else { fprintf(stderr, "unknown mode %s\n", mode); return 2; }
        HEQ(host_eject(), 0);
    }
    fs_devctl("ef0:", 0x100, 0, 0, 0, 0);                            /* flush internal storage for fsck */
    fs_devctl("eh0:", 0x100, 0, 0, 0, 0);
    printf("RESULT %d passed, %d failed, %d skipped\n", R.pass, R.fail, R.skip);
    return R.fail ? 1 : 0;
}
#endif

int main(int argc, char **argv)
{
#ifdef PSPGO
    if (argc > 1 && !strncmp(argv[1], "go", 2)) return go_main(argc, argv);
#endif
    if (argc < 4) { fprintf(stderr, "usage: %s golden|full|fill <image> <out> [seeds]\n", argv[0]); return 2; }
    const char *mode = argv[1], *out = argv[3];
    char path[512];
    setvbuf(stdout, 0, _IOLBF, 0);
    if ((!strcmp(mode, "bigcard") || !strcmp(mode, "es") || !strcmp(mode, "format")) && argc > 5) host_attr_cap = (u32)strtoul(argv[5], 0, 0);
    if (!strcmp(mode, "bigcard") && argc > 5) host_attr_cap = (u32)strtoul(argv[4], 0, 0);
    if (host_disk_load(argv[2])) { fprintf(stderr, "can't load %s\n", argv[2]); return 2; }
    if (!strcmp(mode, "bigcard") && argc > 5 && !strcmp(argv[5], "guard")) host_ms_text[0x3338 / 4] ^= 1;   /* not Sony's code */
    section("boot");
    if (host_boot()) { printf("FAIL: the driver did not mount the image\n"); return 1; }
    check_boot();
    if (!strcmp(mode, "golden")) {
        host_sdk = 0x06060110;
        fst_run("ms0:/FATMSTEST", FST_DETERMINISTIC | FST_BIG | FST_KEEP, &R);
        host_sdk = 0x03070110;
        fst_run("ms0:/FATMSTEST", FST_DETERMINISTIC | FST_NAMES_ONLY, &R);
        HEQ(host_eject(), 0);
        HEQ(host_disk_save(out), 0);
        printf("IO %u %u %u %u\n", host_rd_calls, host_rd_sectors, host_wr_calls, host_wr_sectors);
    } else if (!strcmp(mode, "full")) {
        check_kernel_hooks();
        if (argc > 4) check_seeds(argv[4]);
        host_sdk = 0x06060110;
        fst_run("ms0:/FATMSTEST", FST_BIG | FST_KEEP, &R);
        host_sdk = 0x03070110;
        fst_run("ms0:/FATMSTEST", FST_NAMES_ONLY | FST_KEEP, &R);
        host_sdk = 0x06060110;
        check_host_only();
        if (g_ctx.exfat && argc > 4) exfat_extras();             /* needs the fuse-exfat seeds */
        HEQ(host_eject(), 0);
        snprintf(path, sizeof path, "%s.keep.img", out);
        HEQ(host_disk_save(path), 0);
        HEQ(host_insert(), 0);
        section("cleanup");
        HEQ(fst_rm_tree("ms0:/FATMSTEST"), 0);
        survivors(out);
        HEQ(host_eject(), 0);
        snprintf(path, sizeof path, "%s.final.img", out);
        HEQ(host_disk_save(path), 0);
    } else if (!strcmp(mode, "fill")) {
        fill_tests();
        HEQ(host_eject(), 0);
        HEQ(host_disk_save(out), 0);
    } else if (!strcmp(mode, "format") && argc > 4) {
        format_tests(argv[4], out);
    } else if (!strcmp(mode, "es") && argc > 4) {
        es_tests(argv[4]);
        HEQ(host_eject(), 0);
        if (strcmp(out, "-")) HEQ(host_disk_save(out), 0);           /* "-": the card itself is the result */
    } else if (!strcmp(mode, "walk")) {                              /* a dumped real card: <out> unused */
        u32 o[5] = { 0 }, *p = o, es[6] = { 0 };
        HEQ(fs_devctl("ms0:", 0x02425818, &p, 4, 0, 0), 0);
        HEQ(fs_devctl("ms0:", 0x04100003, 0, 0, es, sizeof es), 0);   /* ES_DEVCTL_GET_INFO */
        printf("  devinfo: %u clusters, %u free, %u x %u bytes; ES on=%u lba=%u isos=%u\n", o[0], o[1], o[4], o[3], es[0], es[1], es[3]);
        walk("ms0:", 0);
        HEQ(host_eject(), 0);
    } else if (!strcmp(mode, "bigcard") && argc > 5) {
        bigcard_tests(argv[5]);
        HEQ(host_eject(), 0);
        HEQ(host_disk_save(out), 0);
    } else { fprintf(stderr, "unknown mode %s\n", mode); return 2; }
    printf("RESULT %d passed, %d failed, %d skipped\n", R.pass, R.fail, R.skip);
    return R.fail ? 1 : 0;
}
