/* scenarios.c - the fatms conformance suite (see fstest.h). Every expectation is Sony 6.61
 * fatms behaviour as ported in the baseline; "Sony quirk" marks results a generic FAT driver
 * would not give. Groups write their own files under the test root, so they are independent. */
#include "fstest.h"
#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>

static fst_result *R;
static const char *G = "init";
static char ROOT[64];
static char DEV[8], FSDEV[12];                     /* the root's device ("ms0:") and its filesystem ("fatms0:") */
static int FLAGS, NEWSDK;
static u32 CS;                                  /* cluster size in bytes */
#define BUFSZ (512 * 1024)
static u8 *WB, *VB, *MB;                        /* write, verify, model buffers (+64 slack) */

static void check(int ok, int line, const char *what, long long got, long long want, int show)
{
    if (ok) { R->pass++; return; }
    R->fail++;
    if (show) fs_print("  FAIL %s:%d  %s  (got 0x%x, want 0x%x)", G, line, what, (unsigned)got, (unsigned)want);
    else fs_print("  FAIL %s:%d  %s", G, line, what);
}
#define CHECK(c)  check(!!(c), __LINE__, #c, 0, 0, 0)
#define EQ(a, b)  do { long long _a = (long long)(a), _b = (long long)(b); \
                       check(_a == _b, __LINE__, #a " == " #b, _a, _b, 1); } while (0)
#define SKIP(why) do { R->skip++; fs_print("  SKIP %s: %s", G, why); } while (0)

static const char *P(const char *fmt, ...)      /* "<root>/<fmt...>", 8 rotating buffers */
{
    static char b[8][512];
    static int n;
    char *s = b[n++ & 7];
    int k = snprintf(s, 512, "%s/", ROOT);
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(s + k, 512 - k, fmt, ap);
    va_end(ap);
    return s;
}

/* ---------------- data ---------------- */
static u32 mix(u32 x) { x ^= x >> 16; x *= 0x7feb352du; x ^= x >> 15; x *= 0x846ca68bu; x ^= x >> 16; return x; }
void fst_fill(u8 *b, u32 seed, u32 off, u32 n)
{
    for (u32 i = 0; i < n; i++, off++) b[i] = (u8)(mix(seed * 0x9e3779b9u + (off >> 2)) >> ((off & 3) * 8));
}
static u32 rnd_state = 1;
static u32 rnd(void) { rnd_state = rnd_state * 1103515245u + 12345u; return mix(rnd_state); }

static int bufs(void)
{
    if (!WB) { WB = malloc(BUFSZ + 64); VB = malloc(BUFSZ + 64); MB = malloc(BUFSZ + 64); }
    return WB && VB && MB;
}

int fst_write_file(const char *path, u32 seed, u32 size)
{
    if (!bufs()) return -1;
    int fd = fs_open(path, PSP_O_WRONLY | PSP_O_CREAT | PSP_O_TRUNC, 0777);
    if (fd < 0) return fd;
    for (u32 off = 0; off < size; ) {
        u32 k = size - off < BUFSZ ? size - off : BUFSZ;
        fst_fill(WB, seed, off, k);
        int w = fs_write(fd, WB, (int)k);
        if (w != (int)k) { fs_close(fd); return w < 0 ? w : -2; }
        off += k;
    }
    return fs_close(fd);
}

int fst_verify_file(const char *path, u32 seed, u32 size)
{
    SceIoStat st;
    if (!bufs()) return -1;
    memset(&st, 0, sizeof st);
    int r = fs_getstat(path, &st);
    if (r < 0) return r;
    if (st.st_size != (SceOff)size) return 1;
    int fd = fs_open(path, PSP_O_RDONLY, 0);
    if (fd < 0) return fd;
    for (u32 off = 0; off < size; ) {
        u32 k = size - off < BUFSZ ? size - off : BUFSZ;
        if (fs_read(fd, VB, (int)k) != (int)k) { fs_close(fd); return 3; }
        fst_fill(WB, seed, off, k);
        if (memcmp(VB, WB, k)) { fs_close(fd); return 4; }
        off += k;
    }
    r = fs_read(fd, VB, 16);
    fs_close(fd);
    return r == 0 ? 0 : 5;
}

/* read [off, off+n) through an open fd (buffer offset `boff`) and compare with the seed */
static int read_cmp(int fd, u32 seed, u32 off, u32 n, u32 boff)
{
    if (fs_lseek(fd, off, PSP_SEEK_SET) != (s64)off) return 1;
    if (fs_read(fd, VB + boff, (int)n) != (int)n) return 2;
    fst_fill(WB, seed, off, n);
    return memcmp(VB + boff, WB, n) ? 3 : 0;
}

/* chunked whole-file verify with an arbitrary read size */
static int verify_chunks(const char *path, u32 seed, u32 size, u32 chunk)
{
    int fd = fs_open(path, PSP_O_RDONLY, 0);
    if (fd < 0) return fd;
    int bad = 0;
    for (u32 off = 0; off < size && !bad; ) {
        u32 k = size - off < chunk ? size - off : chunk;
        if (fs_read(fd, VB, (int)chunk) != (int)k) bad = 2;
        else { fst_fill(WB, seed, off, k); if (memcmp(VB, WB, k)) bad = 3; }
        off += k;
    }
    if (!bad && fs_read(fd, VB, (int)chunk) != 0) bad = 4;
    fs_close(fd);
    return bad;
}

static int devinfo(u32 *o)
{
    u32 *p = o;
    memset(o, 0, 5 * sizeof(u32));
    return fs_devctl(DEV, 0x02425818, &p, 4, 0, 0);
}
static u32 free_clusters(void) { u32 o[5]; devinfo(o); return o[1]; }

typedef struct { char name[256]; SceOff size; int dir; unsigned attr; } dent;
/* list a directory: returns the number of entries (excluding "." and "..") or an error */
static int list_dir(const char *path, dent *out, int max, int *dots)
{
    SceIoDirent d;
    int fd = fs_dopen(path), n = 0, r;
    if (fd < 0) return fd;
    if (dots) *dots = 0;
    for (;;) {
        memset(&d, 0, sizeof d);
        r = fs_dread(fd, &d);
        if (r <= 0) break;
        if (!strcmp(d.d_name, ".") || !strcmp(d.d_name, "..")) { if (dots) ++*dots; continue; }
        if (out && n < max) {
            strncpy(out[n].name, d.d_name, 255);
            out[n].name[255] = 0;
            out[n].size = d.d_stat.st_size;
            out[n].dir = (d.d_stat.st_mode & 0xf000) == 0x1000;
            out[n].attr = d.d_stat.st_attr;
        }
        n++;
    }
    fs_dclose(fd);
    return r < 0 ? r : n;
}

int fst_rm_tree(const char *path)
{
    int max = 512, n;
    dent *e = malloc(sizeof(dent) * max);
    if (!e) return -1;
    n = list_dir(path, e, max, 0);
    if (n < 0) { free(e); return n; }
    for (int i = 0; i < n && i < max; i++) {
        char child[512];
        snprintf(child, sizeof child, "%s/%s", path, e[i].name);
        if (e[i].attr & 1) { SceIoStat st; memset(&st, 0, sizeof st); st.st_mode = 0x1ff; fs_chstat(child, &st, 1); }
        if (e[i].dir) fst_rm_tree(child);
        else fs_remove(child);
    }
    free(e);
    return fs_rmdir(path);
}

/* ---------------- groups ---------------- */
static void g_basic(void)
{
    static const char msg[] = "Hello, world!";
    char b[100];
    SceIoStat st;
    const char *f = P("BASIC.TXT");
    int fd = fs_open(f, PSP_O_WRONLY | PSP_O_CREAT | PSP_O_TRUNC, 0777);
    CHECK(fd >= 0);
    EQ(fs_write(fd, msg, 13), 13);
    EQ(fs_write(fd, msg, 0), 0);
    EQ(fs_close(fd), 0);
    EQ(fs_close(fd), E_IO_BADFD);                       /* double close: iofilemgr rejects the fd */
    memset(&st, 0, sizeof st);
    EQ(fs_getstat(f, &st), 0);
    EQ(st.st_size, 13);
    EQ(st.st_mode, 0x21ff);                             /* regular file, rwx */
    EQ(st.st_attr, 0x20);                               /* archive */
    fd = fs_open(f, PSP_O_RDONLY, 0);
    CHECK(fd >= 0);
    EQ(fs_read(fd, b, 100), 13);
    CHECK(!memcmp(b, msg, 13));
    EQ(fs_read(fd, b, 100), 0);                         /* at EOF */
    EQ(fs_read(fd, b, 0), 0);
    EQ(fs_write(fd, b, 1), E_IO_BADFD);                 /* not opened for writing */
    EQ(fs_close(fd), 0);
    fd = fs_open(f, PSP_O_WRONLY, 0);
    EQ(fs_read(fd, b, 1), E_IO_BADFD);                  /* not opened for reading */
    fs_close(fd);
}

static void g_sizes(void)
{
    u32 sz[24], n = 0;
    static const u32 fixed[] = { 0, 1, 2, 511, 512, 513, 1023, 1024, 1025, 4095, 4096, 4097, 65539, 307201 };
    for (u32 i = 0; i < sizeof fixed / sizeof *fixed; i++) sz[n++] = fixed[i];
    sz[n++] = CS - 1; sz[n++] = CS; sz[n++] = CS + 1; sz[n++] = 2 * CS; sz[n++] = 3 * CS + 17;
    for (u32 i = 0; i < n; i++) {
        const char *f = P("SZ%02u.BIN", i);
        EQ(fst_write_file(f, 100 + i, sz[i]), 0);
        EQ(fst_verify_file(f, 100 + i, sz[i]), 0);
        if (sz[i] <= 2048) EQ(verify_chunks(f, 100 + i, sz[i], 1), 0);
        EQ(verify_chunks(f, 100 + i, sz[i], 7), 0);
        EQ(verify_chunks(f, 100 + i, sz[i], 509), 0);
        EQ(verify_chunks(f, 100 + i, sz[i], 4097), 0);
    }
}

static void g_unaligned(void)
{
    static const u32 boffs[] = { 0, 1, 2, 3, 4, 8, 16, 32, 60 };
    static const u32 foffs[] = { 0, 1, 37, 511, 512, 1023 };
    static const u32 lens[] = { 1, 100, 512, 1024, 2860 };
    const u32 size = 5 * 512 + 300;
    const char *f = P("UNALIGN.BIN");
    EQ(fst_write_file(f, 7, size), 0);
    int fd = fs_open(f, PSP_O_RDONLY, 0), bad = 0;
    CHECK(fd >= 0);
    for (u32 a = 0; a < 9; a++)
        for (u32 b = 0; b < 6; b++)
            for (u32 c = 0; c < 5; c++) {
                u32 n = lens[c] < size - foffs[b] ? lens[c] : size - foffs[b];
                if (fs_lseek(fd, foffs[b], PSP_SEEK_SET) != (s64)foffs[b] ||
                    fs_read(fd, VB + boffs[a], (int)lens[c]) != (int)n) { bad++; continue; }
                fst_fill(WB, 7, foffs[b], n);
                if (memcmp(VB + boffs[a], WB, n)) bad++;
            }
    EQ(bad, 0);
    fs_close(fd);
    /* writes from misaligned sources, checked against a model of the file */
    fst_fill(MB, 7, 0, size);
    fd = fs_open(f, PSP_O_RDWR, 0);
    CHECK(fd >= 0);
    static const u32 wb[] = { 1, 3, 8, 60 };
    for (u32 i = 0; i < 4; i++) {
        u32 off = wb[i] * 37, n = 700;
        fst_fill(WB + wb[i], 8 + i, 0, n);
        memcpy(MB + off, WB + wb[i], n);
        EQ(fs_lseek(fd, off, PSP_SEEK_SET), off);
        EQ(fs_write(fd, WB + wb[i], (int)n), (int)n);
    }
    EQ(fs_lseek(fd, 0, PSP_SEEK_SET), 0);
    EQ(fs_read(fd, VB, (int)size), (int)size);
    CHECK(!memcmp(VB, MB, size));
    fs_close(fd);
}

static void g_seek(void)
{
    const u32 size = 4 * CS + 100;
    const char *f = P("SEEK.BIN");
    char b[8];
    EQ(fst_write_file(f, 11, size), 0);
    int fd = fs_open(f, PSP_O_RDONLY, 0);
    CHECK(fd >= 0);
    EQ(fs_lseek(fd, 0, PSP_SEEK_END), size);
    EQ(fs_lseek(fd, 0, PSP_SEEK_CUR), size);
    EQ(fs_read(fd, b, 8), 0);
    EQ(fs_lseek(fd, 10, PSP_SEEK_SET), 10);
    EQ(fs_lseek(fd, -5, PSP_SEEK_CUR), 5);
    EQ(read_cmp(fd, 11, 5, 5, 0), 0);
    EQ(fs_lseek(fd, 0, PSP_SEEK_CUR), 10);
    s64 r = fs_lseek(fd, -1, PSP_SEEK_SET);
    EQ((int)r, E_EINVAL); CHECK(r < 0);
    r = fs_lseek(fd, -(s64)size - 1, PSP_SEEK_END);
    EQ((int)r, E_EINVAL); CHECK(r < 0);
    r = fs_lseek(fd, 0x100000000LL, PSP_SEEK_SET);   /* beyond 4 GB */
    EQ((int)r, E_EINVAL); CHECK(r < 0);
    r = fs_lseek(fd, 0, 3);                                                         /* iofilemgr checks whence */
    EQ((int)r, E_IO_INVAL); CHECK(r < 0);
    EQ(fs_lseek(fd, 0, PSP_SEEK_CUR), 10);           /* failed seeks leave the position */
    EQ(fs_lseek(fd, size + 100, PSP_SEEK_SET), size + 100);
    EQ(fs_read(fd, b, 8), 0);
    /* random access, and backwards across clusters */
    int bad = 0;
    for (int i = 0; i < 300; i++) {
        u32 off = rnd() % size, n = rnd() % 3000 + 1;
        if (n > size - off) n = size - off;
        if (read_cmp(fd, 11, off, n, i & 7)) bad++;
    }
    EQ(bad, 0);
    EQ(read_cmp(fd, 11, 3 * CS + 50, 50, 0), 0);
    EQ(read_cmp(fd, 11, 0, 100, 0), 0);
    EQ(read_cmp(fd, 11, 2 * CS - 3, 7, 0), 0);
    EQ(read_cmp(fd, 11, CS, CS, 0), 0);
    fs_close(fd);
}

static void g_overwrite(void)
{
    const u32 size = 3 * CS;
    const char *f = P("OVER.BIN");
    EQ(fst_write_file(f, 20, size), 0);
    fst_fill(MB, 20, 0, size);
    int fd = fs_open(f, PSP_O_RDWR, 0);
    CHECK(fd >= 0);
    static const u32 at[] = { 0, 1, 511, 512, 1000 };
    for (u32 i = 0; i < 6; i++) {
        u32 off = i < 5 ? at[i] : CS - 100, n = i < 5 ? 3 : 300;   /* last one crosses a cluster */
        fst_fill(WB, 21 + i, 0, n);
        memcpy(MB + off, WB, n);
        EQ(fs_lseek(fd, off, PSP_SEEK_SET), off);
        EQ(fs_write(fd, WB, (int)n), (int)n);
        EQ(fs_lseek(fd, 0, PSP_SEEK_CUR), off + n);
    }
    fst_fill(WB, 29, 0, 1);
    MB[size - 1] = WB[0];
    EQ(fs_lseek(fd, -1, PSP_SEEK_END), size - 1);
    EQ(fs_write(fd, WB, 1), 1);
    EQ(fs_lseek(fd, 0, PSP_SEEK_SET), 0);
    EQ(fs_read(fd, VB, (int)size), (int)size);
    CHECK(!memcmp(VB, MB, size));
    fs_close(fd);
    SceIoStat st;
    EQ(fs_getstat(f, &st), 0);
    EQ(st.st_size, size);                               /* overwrites never grow the file */
}

static void g_append(void)
{
    const char *f = P("APPEND.BIN");
    EQ(fst_write_file(f, 30, 1000), 0);
    fst_fill(MB, 30, 0, 1000);
    int fd = fs_open(f, PSP_O_WRONLY | PSP_O_APPEND, 0);
    CHECK(fd >= 0);
    EQ(fs_lseek(fd, 0, PSP_SEEK_CUR), 1000);           /* O_APPEND opens at EOF */
    EQ(fs_lseek(fd, 0, PSP_SEEK_SET), 0);
    fst_fill(WB, 31, 0, 500);
    memcpy(MB + 1000, WB, 500);
    EQ(fs_write(fd, WB, 500), 500);                     /* ...and every write goes to EOF */
    EQ(fs_lseek(fd, 0, PSP_SEEK_CUR), 1500);
    fs_close(fd);
    fd = fs_open(f, PSP_O_RDWR | PSP_O_APPEND, 0);
    char b[4];
    EQ(fs_read(fd, b, 4), 0);
    fs_close(fd);
    fd = fs_open(f, PSP_O_RDONLY, 0);
    EQ(fs_read(fd, VB, 4000), 1500);
    CHECK(!memcmp(VB, MB, 1500));
    fs_close(fd);
}

static void g_gap(void)
{
    const char *f = P("GAP.BIN");
    const u32 at = 100 + 2 * CS + 5;
    EQ(fst_write_file(f, 40, 100), 0);
    int fd = fs_open(f, PSP_O_RDWR, 0);
    EQ(fs_lseek(fd, at, PSP_SEEK_SET), at);
    fst_fill(WB, 41, 0, 10);
    EQ(fs_write(fd, WB, 10), 10);
    fs_close(fd);
    SceIoStat st;
    EQ(fs_getstat(f, &st), 0);
    EQ(st.st_size, at + 10);                            /* the gap is allocated, not zeroed */
    fd = fs_open(f, PSP_O_RDONLY, 0);
    EQ(fs_lseek(fd, at, PSP_SEEK_SET), at);
    EQ(fs_read(fd, VB, 64), 10);
    CHECK(!memcmp(VB, WB, 10));
    EQ(read_cmp(fd, 40, 0, 100, 0), 0);
    fs_close(fd);
}

static void g_trunc(void)
{
    EQ(fs_mkdir(P("TRUNC"), 0777), 0);                   /* own dir: no directory growth below */
    const char *f = P("TRUNC/T.BIN");
    u32 f0 = free_clusters();
    EQ(fst_write_file(f, 50, 5 * CS + 1), 0);
    EQ(free_clusters(), f0 - 6);
    int fd = fs_open(f, PSP_O_WRONLY | PSP_O_TRUNC, 0);
    CHECK(fd >= 0);
    fs_close(fd);
    SceIoStat st;
    EQ(fs_getstat(f, &st), 0);
    EQ(st.st_size, 0);
    EQ(free_clusters(), f0);                            /* truncate frees the whole chain */
    fd = fs_open(f, PSP_O_WRONLY, 0);
    EQ(fs_write(fd, "0123456789", 10), 10);
    fs_close(fd);
    EQ(free_clusters(), f0 - 1);
    int ro = fs_open(f, PSP_O_RDONLY, 0);
    EQ(fs_open(f, PSP_O_WRONLY | PSP_O_CREAT | PSP_O_TRUNC, 0777), E_EACCES);   /* create+trunc of an open file */
    fs_close(ro);
    EQ(fs_remove(f), 0);
    EQ(free_clusters(), f0);
}

static void g_errors(void)
{
    char name[301];
    EQ(fs_open(P("NOPE.TXT"), PSP_O_RDONLY, 0), E_ENOENT);
    EQ(fs_open(P("NODIR/X.TXT"), PSP_O_WRONLY | PSP_O_CREAT, 0777), E_ENOENT);
    EQ(fst_write_file(P("EXISTS.TXT"), 60, 10), 0);
    EQ(fs_open(P("EXISTS.TXT"), PSP_O_WRONLY | PSP_O_CREAT | PSP_O_EXCL, 0777), E_EEXIST);
    EQ(fs_open(P("EXISTS.TXT"), PSP_O_RDONLY | PSP_O_TRUNC, 0), E_EINVAL);        /* Sony quirk */
    EQ(fs_open(P("EXISTS.TXT"), PSP_O_WRONLY | PSP_O_APPEND | PSP_O_TRUNC, 0), E_EINVAL);
    EQ(fs_open(P("NEW_RO.TXT"), PSP_O_RDONLY | PSP_O_CREAT, 0777), E_EINVAL);     /* Sony quirk: create needs write */
    EQ(fs_mkdir(P("ADIR"), 0777), 0);
    EQ(fs_open(P("ADIR"), PSP_O_RDONLY, 0), E_EACCES);                             /* a directory */
    EQ(fs_open(P("ADIR"), PSP_O_WRONLY | PSP_O_CREAT, 0777), E_EACCES);
    EQ(fs_open(P("a?b.txt"), PSP_O_WRONLY | PSP_O_CREAT, 0777), E_EINVAL);
    EQ(fs_open(P("a*b.txt"), PSP_O_WRONLY | PSP_O_CREAT, 0777), E_EINVAL);
    EQ(fs_open(P("a<b.txt"), PSP_O_WRONLY | PSP_O_CREAT, 0777), E_EINVAL);
    EQ(fs_open(P("a\"b.txt"), PSP_O_WRONLY | PSP_O_CREAT, 0777), E_EINVAL);
    EQ(fs_open(P("%s", ""), PSP_O_WRONLY | PSP_O_CREAT, 0777), E_EINVAL);                /* no leaf */
    memset(name, 'a', 300); name[300] = 0;
    EQ(fs_open(P("%s", name), PSP_O_WRONLY | PSP_O_CREAT, 0777), E_EINVAL);        /* path > 259 chars */
    name[200] = 0;                                                                  /* 200-char LFN: 16 entries */
    EQ(fst_write_file(P("%s", name), 61, 77), 0);
    EQ(fst_verify_file(P("%s", name), 61, 77), 0);
    dent e[4];
    EQ(fs_mkdir(P("LONGNM"), 0777), 0);
    EQ(fst_write_file(P("LONGNM/%s", name), 62, 5), 0);
    EQ(list_dir(P("LONGNM"), e, 4, 0), 1);
    EQ(strcmp(e[0].name, name), 0);
    EQ(fst_write_file(P("TRAIL."), 63, 3), 0);                                      /* trailing dots dropped */
    EQ(fst_verify_file(P("TRAIL"), 63, 3), 0);
    EQ(fst_verify_file(P("TRAIL..."), 63, 3), 0);
    EQ(fst_write_file(P(".hidden"), 64, 4), 0);
    EQ(fst_verify_file(P(".hidden"), 64, 4), 0);
}

static void g_fdlimit(void)
{
    const char *f = P("FDS.TXT");
    int fds[16], n = 0, last = 0, bad = 0;
    EQ(fst_write_file(f, 70, 100), 0);
    for (int i = 0; i < 16; i++) {
        int fd = fs_open(f, PSP_O_RDONLY, 0);
        if (fd < 0) { last = fd; break; }
        fds[n++] = fd;
    }
    CHECK(n >= 1 && n <= 10);
    EQ(last, n == 10 ? E_IO_MFILE : E_EMFILE);      /* iofilemgr's 10 user fds, or fatms's 10 slots if the kernel holds some */
    for (int i = 0; i < n; i++) EQ(fs_close(fds[i]), 0);
    for (int i = 0; i < 300; i++) {                                                 /* slots come back */
        int fd = fs_open(f, PSP_O_RDONLY, 0);
        if (fd < 0 || fs_read(fd, VB, 200) != 100) bad++;
        fs_close(fd);
    }
    EQ(bad, 0);
}

static void g_handles(void)
{
    const u32 size = 3 * CS + 17;
    const char *f = P("TWO.BIN");
    EQ(fst_write_file(f, 80, size), 0);
    int a = fs_open(f, PSP_O_RDONLY, 0), b = fs_open(f, PSP_O_RDONLY, 0);
    CHECK(a >= 0 && b >= 0);
    EQ(read_cmp(a, 80, CS + 5, 100, 0), 0);
    EQ(read_cmp(b, 80, 10, 100, 0), 0);
    EQ(fs_read(a, VB, 50), 50);                                                     /* positions are per fd */
    fst_fill(WB, 80, CS + 105, 50);
    CHECK(!memcmp(VB, WB, 50));
    fs_close(a); fs_close(b);
    /* a writer and readers on one file, before the writer closes */
    const char *w = P("WR.BIN");
    int fw = fs_open(w, PSP_O_RDWR | PSP_O_CREAT | PSP_O_TRUNC, 0777);
    fst_fill(WB, 81, 0, 1500);
    EQ(fs_write(fw, WB, 1000), 1000);
    int r1 = fs_open(w, PSP_O_RDONLY, 0);
    EQ(fs_read(r1, VB, 2000), 1000);                                                /* sees the written size */
    CHECK(!memcmp(VB, WB, 1000));
    SceIoStat st;
    EQ(fs_getstat(w, &st), 0);
    EQ(st.st_size, 1000);
    EQ(fs_write(fw, WB + 1000, 500), 500);
    EQ(fs_read(r1, VB, 2000), 0);                                                   /* Sony: size snapshot at open */
    EQ(fs_getstat(w, &st), 0);
    EQ(st.st_size, 1500);
    int r2 = fs_open(w, PSP_O_RDONLY, 0);
    EQ(fs_read(r2, VB, 2000), 1500);
    CHECK(!memcmp(VB, WB, 1500));
    fs_close(r2); fs_close(r1); fs_close(fw);
    EQ(fst_verify_file(w, 81, 1500), 0);
}

static void g_dirs(void)
{
    char path[256] = "D1";
    SceIoStat st;
    EQ(fs_mkdir(P("D1"), 0777), 0);
    EQ(fs_mkdir(P("D1"), 0777), E_EEXIST);
    for (int i = 2; i <= 8; i++) {
        snprintf(path + strlen(path), sizeof path - strlen(path), "/D%d", i);
        EQ(fs_mkdir(P("%s", path), 0777), 0);
    }
    EQ(fst_write_file(P("%s/F.TXT", path), 90, 3000), 0);
    EQ(fst_verify_file(P("D1/D2/../D2/./D3/D4/D5/D6/D7/D8/F.TXT"), 90, 3000), 0);
    EQ(fs_getstat(P("D1"), &st), 0);
    EQ(st.st_mode, 0x11ff);
    EQ(st.st_attr & 0x10, 0x10);
    EQ(fs_rmdir(P("D1")), E_EACCES);                                                /* not empty */
    EQ(fs_rmdir(P("%s/F.TXT", path)), E_EACCES);                                    /* not a directory */
    EQ(fs_remove(P("D1")), E_EACCES);                                               /* a directory */
    EQ(fs_remove(P("NOPE")), E_ENOENT);
    EQ(fs_rmdir(P("NOPE")), E_ENOENT);
    EQ(fs_mkdir(P("NOPE/X"), 0777), E_ENOENT);
    EQ(fs_mkdir(P("D1/../D1X"), 0777), 0);
    EQ(fs_getstat(P("D1X"), &st), 0);
    EQ(fs_mkdir(P("EMPTY"), 0777), 0);
    EQ(fs_rmdir(P("EMPTY")), 0);
    EQ(fs_getstat(P("EMPTY"), &st), E_ENOENT);
    EQ(fs_mkdir(P("Long Directory Name"), 0777), 0);
    EQ(fst_write_file(P("Long Directory Name/inner file.bin"), 91, 1234), 0);
    EQ(fst_verify_file(P("long directory name/INNER FILE.BIN"), 91, 1234), 0);    /* lookups ignore case */
    EQ(fs_dopen(P("NOPE")), E_ENOENT);
    EQ(fs_dopen(P("%s/F.TXT", path)), E_ENOTDIR);
    /* the first cluster of a new directory holds "." and ".." */
    int dots = 0;
    EQ(list_dir(P("D1X"), 0, 0, &dots), 0);
    EQ(dots, 2);
}

/* names whose 8.3 alias Sony generates; alias "" = name is a plain 8.3 already */
static const struct { const char *name, *alias; } NAMES[] = {
    { "UPPER.TXT", "UPPER.TXT" },
    { "lower.txt", "LOWER~1.TXT" },                       /* Sony quirk: always ~1 before the extension */
    { "MiXeD.TxT", "MIXED~1.TXT" },
    { "a very long file name with spaces.dat", "AVERYL~1.DAT" },
    { "a very long file name 2.dat", "AVERYL~2.DAT" },
    { "multi.dot.name.bin", "MULTID~1.BIN" },
    { "NOEXT", "NOEXT" },
    { ".hidden", "HIDDEN~1" },
    { "abcdefghijklm", "ABCDEF~1" },
    { "abcdefghijklmnopqrstuvwxyz", "ABCDEF~2" },
    { "8CHARSXX.EXT", "8CHARSXX.EXT" },
};
#define NNAMES (int)(sizeof NAMES / sizeof *NAMES)

static void g_listing(void)
{
    char dir[128];
    strcpy(dir, P(NEWSDK ? "LIST_U8" : "LIST_SJ"));
    EQ(fs_mkdir(dir, 0777), 0);
    for (int i = 0; i < NNAMES; i++) EQ(fst_write_file(P("%s/%s", NEWSDK ? "LIST_U8" : "LIST_SJ", NAMES[i].name), 400 + i, 10 + 3 * i), 0);
    SceIoDirent d;
    struct { u32 size; char s[16]; char l[1024]; } np;          /* SDK >= 3.80 d_private */
    char op[0x10d + 3];                                          /* older: {sfn[13], lfn[256]} */
    int fd = fs_dopen(dir), seen[NNAMES] = { 0 }, dots = 0, other = 0, r;
    CHECK(fd >= 0);
    if (NEWSDK) {
        memset(&d, 0, sizeof d);
        np.size = 0;
        d.d_private = (void *)&np;
        EQ(fs_dread(fd, &d), E_EINVAL);                         /* the struct must say 0x414 */
    }
    for (;;) {
        memset(&d, 0, sizeof d);
        memset(&np, 0, sizeof np);
        memset(op, 0, sizeof op);
        np.size = 0x414;
        d.d_private = NEWSDK ? (void *)&np : (void *)op;
        r = fs_dread(fd, &d);
        if (r <= 0) break;
        const char *s = NEWSDK ? np.s : op, *l = NEWSDK ? np.l : op + 13;
        if (!strcmp(d.d_name, ".") || !strcmp(d.d_name, "..")) { dots++; EQ(d.d_stat.st_mode, 0x11ff); continue; }
        int k = 0;
        while (k < NNAMES && strcmp(d.d_name, NAMES[k].name)) k++;
        if (k == NNAMES) { other++; fs_print("  unexpected entry '%s'", d.d_name); continue; }
        seen[k]++;
        EQ(d.d_stat.st_size, 10 + 3 * k);
        EQ(d.d_stat.st_mode, 0x21ff);
        if (strcmp(s, NAMES[k].alias)) { fs_print("  %s: alias '%s', want '%s'", NAMES[k].name, s, NAMES[k].alias); CHECK(0); }
        if (strcmp(l, NAMES[k].name)) { fs_print("  %s: long name '%s'", NAMES[k].name, l); CHECK(0); }
    }
    EQ(r, 0);
    EQ(fs_dread(fd, &d), 0);                                     /* stays at the end */
    fs_dclose(fd);
    EQ(dots, 2);
    EQ(other, 0);
    for (int i = 0; i < NNAMES; i++) EQ(seen[i], 1);
    /* lookups by any case and by the 8.3 alias */
    SceIoStat st;
    EQ(fs_getstat(P("%s/LOWER.TXT", NEWSDK ? "LIST_U8" : "LIST_SJ"), &st), 0);
    EQ(fs_getstat(P("%s/AVERYL~1.DAT", NEWSDK ? "LIST_U8" : "LIST_SJ"), &st), 0);
    EQ(st.st_size, 10 + 3 * 3);
    EQ(fs_open(P("%s/LOWER.TXT", NEWSDK ? "LIST_U8" : "LIST_SJ"), PSP_O_WRONLY | PSP_O_CREAT | PSP_O_EXCL, 0777), E_EEXIST);
}

/* kana in each encoding: SDK >= 3.80 callers pass UTF-8, older ones Shift-JIS */
#define U8_SOFUTO "\xE3\x82\xBD\xE3\x83\x95\xE3\x83\x88"      /* katakana "sofuto"; SJIS so = 83 5C (trail byte '\') */
#define SJ_SOFUTO "\x83\x5C\x83\x74\x83\x67"
#define U8_AIU    "\xE3\x81\x82\xE3\x81\x84\xE3\x81\x86.txt"   /* hiragana "aiu.txt" */
#define SJ_AIU    "\x82\xA0\x82\xA2\x82\xA4.txt"

static void g_names(void)
{
    const char *kdir = NEWSDK ? U8_SOFUTO "U" : SJ_SOFUTO "S", *kfile = NEWSDK ? U8_AIU : SJ_AIU;
    dent e[4];
    SceIoStat st;
    EQ(fs_mkdir(P("%s", kdir), 0777), 0);
    EQ(fst_write_file(P("%s/%s", kdir, kfile), 95, 4321), 0);
    EQ(fst_verify_file(P("%s/%s", kdir, kfile), 95, 4321), 0);
    EQ(fs_getstat(P("%s", kdir), &st), 0);
    EQ(st.st_mode, 0x11ff);
    EQ(list_dir(P("%s", kdir), e, 4, 0), 1);
    EQ(strcmp(e[0].name, kfile), 0);
    EQ(fs_rename(P("%s/%s", kdir, kfile), "renamed.txt"), 0);
    EQ(fst_verify_file(P("%s/renamed.txt", kdir), 95, 4321), 0);
    EQ(fs_rename(P("%s/renamed.txt", kdir), kfile), 0);
    /* the same name written in the other SDK mode (a previous pass) reads back in this encoding */
    const char *mine = NEWSDK ? "XMODEU" : "XMODES", *theirs = NEWSDK ? "XMODES" : "XMODEU";
    EQ(fs_mkdir(P("%s", mine), 0777), 0);
    EQ(fst_write_file(P("%s/%s", mine, kfile), 96, 10), 0);
    if (fs_getstat(P("%s", theirs), &st) == 0) {
        EQ(list_dir(P("%s", theirs), e, 4, 0), 1);
        EQ(strcmp(e[0].name, kfile), 0);
        EQ(fst_verify_file(P("%s/%s", theirs, kfile), 96, 10), 0);
    }
}

static void g_rename(void)
{
    SceIoStat st;
    EQ(fst_write_file(P("R1.TXT"), 100, 500), 0);
    EQ(fs_rename(P("R1.TXT"), "R2.TXT"), 0);                                        /* new name: no device */
    EQ(fs_getstat(P("R1.TXT"), &st), E_ENOENT);
    EQ(fst_verify_file(P("R2.TXT"), 100, 500), 0);
    EQ(fs_rename(P("R2.TXT"), "/FATMSTEST/Renamed Long Name.txt"), 0);
    EQ(fst_verify_file(P("Renamed Long Name.txt"), 100, 500), 0);
    EQ(fst_write_file(P("R3.TXT"), 101, 5), 0);
    EQ(fs_rename(P("R3.TXT"), "Renamed Long Name.txt"), E_EEXIST);
    EQ(fs_rename(P("R3.TXT"), "r3.txt"), E_EEXIST);                                 /* Sony quirk: case-only rename */
    EQ(fs_rename(P("R3.TXT"), P("R4.TXT")), 0);                                     /* iofilemgr strips "ms0:" from the new name */
    EQ(fst_verify_file(P("R4.TXT"), 101, 5), 0);
    EQ(fs_rename(P("R4.TXT"), "R3.TXT"), 0);
    EQ(fs_rename(P("NOPE.TXT"), "X.TXT"), E_ENOENT);
    /* the new name's directory is ignored: renames stay in the source directory (Sony) */
    EQ(fs_mkdir(P("RD1"), 0777), 0);
    EQ(fs_mkdir(P("RD2"), 0777), 0);
    EQ(fst_write_file(P("RD1/X.TXT"), 102, 50), 0);
    EQ(fs_rename(P("RD1/X.TXT"), "/FATMSTEST/RD2/Y.TXT"), 0);
    EQ(fst_verify_file(P("RD1/Y.TXT"), 102, 50), 0);
    EQ(fs_getstat(P("RD2/Y.TXT"), &st), E_ENOENT);
    /* read-only files can't be renamed; directories can */
    memset(&st, 0, sizeof st);
    st.st_mode = 0x16d;
    EQ(fs_chstat(P("R3.TXT"), &st, 1), 0);
    EQ(fs_rename(P("R3.TXT"), "R5.TXT"), E_EACCES);
    st.st_mode = 0x1ff;
    EQ(fs_chstat(P("R3.TXT"), &st, 1), 0);
    EQ(fs_mkdir(P("RDIR"), 0777), 0);
    EQ(fst_write_file(P("RDIR/IN.BIN"), 103, 3000), 0);
    EQ(fs_rename(P("RDIR"), "Renamed Dir"), 0);
    EQ(fst_verify_file(P("Renamed Dir/IN.BIN"), 103, 3000), 0);
    EQ(fs_getstat(P("RDIR"), &st), E_ENOENT);
}

static void g_move(void)
{
    SceIoStat st;
    EQ(fs_mkdir(P("MV1"), 0777), 0);
    EQ(fs_mkdir(P("MV2"), 0777), 0);
    EQ(fst_write_file(P("MV1/M.BIN"), 110, 2000), 0);
    EQ(fst_write_file(P("MV2/OTHER.BIN"), 111, 10), 0);
    const char *pp[2] = { "/FATMSTEST/MV1/M.BIN", "/FATMSTEST/MV2/Moved File.bin" };
    EQ(fs_devctl(DEV, 0x02415830, pp, 8, 0, 0), 0);                              /* move across directories */
    EQ(fs_getstat(P("MV1/M.BIN"), &st), E_ENOENT);
    EQ(fst_verify_file(P("MV2/Moved File.bin"), 110, 2000), 0);
    const char *ex[2] = { "/FATMSTEST/MV2/Moved File.bin", "/FATMSTEST/MV2/OTHER.BIN" };
    EQ(fs_devctl(DEV, 0x02415830, ex, 8, 0, 0), E_EEXIST);
    const char *dev[2] = { P("MV2/OTHER.BIN"), P("MV1/OTHER.BIN") };
    EQ(fs_devctl(DEV, 0x02415830, dev, 8, 0, 0), E_EINVAL);                         /* Sony quirk: no "ms0:" */
}

static int dt_eq(const ScePspDateTime *a, const ScePspDateTime *b)
{
    return a->year == b->year && a->month == b->month && a->day == b->day &&
           a->hour == b->hour && a->minute == b->minute && a->second == b->second;
}

static void g_chstat(void)
{
    const char *f = P("CH.TXT");
    SceIoStat st, g;
    EQ(fst_write_file(f, 120, 100), 0);
    memset(&st, 0, sizeof st);
    st.st_mode = 0x16d;                                                             /* no write bits */
    EQ(fs_chstat(f, &st, 1), 0);
    EQ(fs_getstat(f, &g), 0);
    EQ(g.st_attr & 1, 1);
    EQ(g.st_mode, 0x216d);
    EQ(fs_open(f, PSP_O_WRONLY, 0), E_EACCES);
    EQ(fs_open(f, PSP_O_RDWR, 0), E_EACCES);
    EQ(fs_remove(f), E_EACCES);
    int fd = fs_open(f, PSP_O_RDONLY, 0);
    CHECK(fd >= 0);
    fs_close(fd);
    st.st_mode = 0x1ff;
    EQ(fs_chstat(f, &st, 1), 0);
    EQ(fs_getstat(f, &g), 0);
    EQ(g.st_attr & 1, 0);
    st.st_attr = 0x22;                                                              /* archive + hidden */
    EQ(fs_chstat(f, &st, 2), 0);
    EQ(fs_getstat(f, &g), 0);
    EQ(g.st_attr, 0x22);
    st.st_attr = 0x10;
    EQ(fs_chstat(f, &st, 2), E_EINVAL);                                             /* can't make a file a dir */
    st.st_attr = 0x28;
    EQ(fs_chstat(f, &st, 2), E_EINVAL);                                             /* nor a volume label */
    ScePspDateTime c = { 2010, 5, 6, 7, 8, 10, 0 }, m = { 2011, 6, 7, 8, 9, 12, 0 };
    st.sce_st_ctime = c;
    st.sce_st_mtime = m;
    EQ(fs_chstat(f, &st, 8 | 0x20), 0);
    EQ(fs_getstat(f, &g), 0);
    CHECK(dt_eq(&g.sce_st_ctime, &c));
    CHECK(dt_eq(&g.sce_st_mtime, &m));
    st.sce_st_ctime.month = 13;
    EQ(fs_chstat(f, &st, 8), E_EINVAL);
    fd = fs_open(f, PSP_O_RDONLY, 0);
    st.st_attr = 0x20;
    EQ(fs_chstat(f, &st, 2), E_EACCES);                                             /* busy */
    fs_close(fd);
    EQ(fs_chstat(P("NOPE.TXT"), &st, 2), E_ENOENT);
    /* new files are stamped with the clock */
    EQ(fs_getstat(P("BASIC.TXT"), &g), 0);
    CHECK(g.sce_st_mtime.year >= 2010 && g.sce_st_mtime.month >= 1 && g.sce_st_mtime.month <= 12);
    CHECK(dt_eq(&g.sce_st_ctime, &g.sce_st_mtime) || g.sce_st_mtime.year >= g.sce_st_ctime.year);
}

static void g_frag(void)
{
    int a = fs_open(P("FRAG_A.BIN"), PSP_O_WRONLY | PSP_O_CREAT | PSP_O_TRUNC, 0777);
    int b = fs_open(P("FRAG_B.BIN"), PSP_O_WRONLY | PSP_O_CREAT | PSP_O_TRUNC, 0777);
    CHECK(a >= 0 && b >= 0);
    for (u32 i = 0; i < 16; i++) {                                                  /* interleaved clusters */
        fst_fill(WB, 130, i * CS, CS);
        EQ(fs_write(a, WB, (int)CS), (int)CS);
        fst_fill(WB, 131, i * CS, CS);
        EQ(fs_write(b, WB, (int)CS), (int)CS);
    }
    fs_close(a); fs_close(b);
    EQ(fst_verify_file(P("FRAG_A.BIN"), 130, 16 * CS), 0);
    EQ(fst_verify_file(P("FRAG_B.BIN"), 131, 16 * CS), 0);
    EQ(fs_remove(P("FRAG_A.BIN")), 0);                                              /* leaves 16 one-cluster holes */
    const u32 size = 20 * CS + 123;
    EQ(fst_write_file(P("FRAG_C.BIN"), 132, size), 0);
    EQ(fst_verify_file(P("FRAG_C.BIN"), 132, size), 0);
    int fd = fs_open(P("FRAG_C.BIN"), PSP_O_RDONLY, 0), bad = 0;
    for (int i = 0; i < 200; i++) {
        u32 off = rnd() % size, n = rnd() % (3 * CS) + 1;
        if (n > size - off) n = size - off;
        if (n > BUFSZ) n = BUFSZ;
        if (read_cmp(fd, 132, off, n, 0)) bad++;
    }
    EQ(bad, 0);
    fs_close(fd);
    EQ(fst_verify_file(P("FRAG_B.BIN"), 131, 16 * CS), 0);
}

static void g_many(void)
{
    enum { N = 300 };
    EQ(fs_mkdir(P("MANY"), 0777), 0);
    u32 t0 = fs_now_ms();
    int bad = 0;
    for (int i = 0; i < N; i++)
        if (fst_write_file(P("MANY/file number %03d with a long name.bin", i), 200 + i, 100 + i)) bad++;
    EQ(bad, 0);
    u32 t1 = fs_now_ms();
    int dots = 0;
    EQ(list_dir(P("MANY"), 0, 0, &dots), N);
    EQ(dots, 2);
    u32 t2 = fs_now_ms();
    int order[N];
    for (int i = 0; i < N; i++) order[i] = i;
    for (int i = N - 1; i > 0; i--) { int j = (int)(rnd() % (u32)(i + 1)), t = order[i]; order[i] = order[j]; order[j] = t; }
    bad = 0;
    for (int i = 0; i < N; i++)
        if (fst_verify_file(P("MANY/file number %03d with a long name.bin", order[i]), 200 + order[i], 100 + order[i])) bad++;
    EQ(bad, 0);
    u32 t3 = fs_now_ms();
    bad = 0;
    for (int i = 0; i < N; i++)
        if (fs_remove(P("MANY/FILE NUMBER %03d WITH A LONG NAME.BIN", i))) bad++;
    EQ(bad, 0);
    EQ(list_dir(P("MANY"), 0, 0, 0), 0);
    EQ(fs_rmdir(P("MANY")), 0);
    if (!(FLAGS & FST_DETERMINISTIC))
        fs_print("  %d files: create %u ms, list %u ms, open+verify %u ms, delete %u ms",
                 N, (unsigned)(t1 - t0), (unsigned)(t2 - t1), (unsigned)(t3 - t2), (unsigned)(fs_now_ms() - t3));
}

static void g_savedata(void)
{
    static const char *d = "SAVEDATA/ULUS10000GAME00";
    EQ(fs_mkdir(P("SAVEDATA"), 0777), 0);
    EQ(fs_mkdir(P("%s", d), 0777), 0);
    u32 f1 = 0;
    for (u32 c = 0; c < 3; c++) {                                                   /* three saves over the same files */
        EQ(fst_write_file(P("%s/PARAM.SFO", d), 300 + c, 4096), 0);
        EQ(fst_write_file(P("%s/ICON0.PNG", d), 310 + c, 20000), 0);
        EQ(fst_write_file(P("%s/DATA.BIN", d), 320 + c, 200000), 0);
        EQ(fst_verify_file(P("%s/PARAM.SFO", d), 300 + c, 4096), 0);
        EQ(fst_verify_file(P("%s/ICON0.PNG", d), 310 + c, 20000), 0);
        EQ(fst_verify_file(P("%s/DATA.BIN", d), 320 + c, 200000), 0);
        if (c == 0) f1 = free_clusters();
    }
    EQ(free_clusters(), f1);                                                        /* rewrites leak nothing */
    dent e[8];
    EQ(list_dir(P("%s", d), e, 8, 0), 3);
}

static void g_async(void)
{
    const u32 size = 300000;
    EQ(fst_write_file(P("ASYNC.BIN"), 140, size), 0);
    int fd = fs_open(P("ASYNC.BIN"), PSP_O_RDONLY, 0), bad = 0;
    CHECK(fd >= 0);
    u32 off = 0;
    for (;;) {
        s64 res = -1;
        if (fs_read_async(fd, VB, 65536) < 0 || fs_wait_async(fd, &res) < 0) { bad++; break; }
        if (res <= 0) break;
        fst_fill(WB, 140, off, (u32)res);
        if (memcmp(VB, WB, (u32)res)) bad++;
        off += (u32)res;
    }
    EQ(bad, 0);
    EQ(off, size);
    fs_close(fd);
}

struct worker { int id; u32 seed, size; u8 *buf, *exp; };
static int worker(void *arg)
{
    struct worker *w = arg;
    char path[128];
    snprintf(path, sizeof path, "%s/THREAD%d.BIN", ROOT, w->id);
    if (w->id == 2) {                                                               /* writer */
        int fd = fs_open(path, PSP_O_WRONLY | PSP_O_CREAT | PSP_O_TRUNC, 0777);
        if (fd < 0) return 1;
        for (u32 off = 0; off < w->size; off += 8192) {
            fst_fill(w->buf, w->seed, off, 8192);
            if (fs_write(fd, w->buf, 8192) != 8192) { fs_close(fd); return 2; }
        }
        fs_close(fd);
    }
    for (int pass = 0; pass < 3; pass++) {
        int fd = fs_open(path, PSP_O_RDONLY, 0);
        if (fd < 0) return 3;
        for (u32 i = 0; i < 64; i++) {
            u32 off = w->id == 1 ? (u32)(mix(i * 7919u + pass) % (w->size - 4096)) : i * 4096 % w->size;
            if (fs_lseek(fd, off, PSP_SEEK_SET) != (s64)off || fs_read(fd, w->buf, 4096) != 4096) { fs_close(fd); return 4; }
            fst_fill(w->exp, w->seed, off, 4096);
            if (memcmp(w->buf, w->exp, 4096)) { fs_close(fd); return 5; }
        }
        fs_close(fd);
    }
    return 0;
}

static void g_threads(void)
{
    struct worker w[3];
    void *args[3];
    for (int i = 0; i < 3; i++) {
        w[i].id = i; w[i].seed = 150 + i; w[i].size = 256 * 1024;
        w[i].buf = malloc(8192); w[i].exp = malloc(8192);
        args[i] = &w[i];
        if (i != 2) EQ(fst_write_file(P("THREAD%d.BIN", i), w[i].seed, w[i].size), 0);
    }
    EQ(fs_threads(3, worker, args), 0);
    for (int i = 0; i < 3; i++) { free(w[i].buf); free(w[i].exp); }
    EQ(fst_verify_file(P("THREAD2.BIN"), 152, 256 * 1024), 0);
}

static void g_devctl(void)
{
    u32 o[5], v = 0;
    EQ(devinfo(o), 0);
    CHECK(o[0] > 0 && o[1] <= o[0]);
    EQ(o[3], 512);
    CHECK(o[4] && !(o[4] & (o[4] - 1)));                                            /* sectors per cluster: 2^n */
    EQ(fs_devctl(FSDEV, 0x02425823, 0, 0, &v, 4), 0);                               /* ms0: assigned */
    EQ(v, 1);
    EQ(fs_devctl(DEV, 0x02400000, 0, 0, 0, 0), E_UNSUP);
    u32 one = 1;                                    /* write-through mode: kernel-only (cmd bit 15); host_main.c tests it */
    EQ(fs_devctl(DEV, 0x0241D814, &one, 4, 0, 0), E_IO_PERM);
    int cb = fs_cb_create();                                                        /* MS insert/eject callback */
    CHECK(cb > 0);
    EQ(fs_devctl(FSDEV, 0x02415821, &cb, 4, 0, 0), 0);
    EQ(fs_cb_seen(cb), 1);                                                          /* notified "inserted" at once */
    EQ(fs_devctl(FSDEV, 0x02415822, &cb, 4, 0, 0), 0);
    EQ(fs_devctl(FSDEV, 0x02415822, &cb, 4, 0, 0), E_EINVAL);
    fs_cb_delete(cb);
}

static void g_root(void)
{
    SceIoDirent d;
    char top[12];
    snprintf(top, sizeof top, "%s/", DEV);
    int fd = fs_dopen(top), found = 0;
    CHECK(fd >= 0);
    for (;;) {
        memset(&d, 0, sizeof d);
        if (fs_dread(fd, &d) <= 0) break;
        if (!strcmp(d.d_name, ROOT + 5)) found = (d.d_stat.st_mode & 0xf000) == 0x1000;
    }
    fs_dclose(fd);
    CHECK(found);
}

static void g_big(void)
{
    u32 n = 1100, size;                                                             /* > 1024 clusters: checkpoints */
    if ((u64)n * CS > 40u * 1024 * 1024) n = 40u * 1024 * 1024 / CS;
    size = n * CS + 777;
    u32 f0 = free_clusters();
    if (f0 < n + 64) { SKIP("not enough free space for the big file"); return; }
    const char *f = P("BIG.BIN");
    u32 t0 = fs_now_ms();
    EQ(fst_write_file(f, 170, size), 0);
    u32 t1 = fs_now_ms();
    EQ(fst_verify_file(f, 170, size), 0);
    u32 t2 = fs_now_ms();
    int fd = fs_open(f, PSP_O_RDONLY, 0), bad = 0;
    CHECK(fd >= 0);
    for (int i = 0; i < 300; i++) if (read_cmp(fd, 170, rnd() % (size - 2048) & ~2047u, 2048, 0)) bad++;   /* ISO-style */
    EQ(bad, 0);
    EQ(read_cmp(fd, 170, (n > 1024 ? 1024 : n / 2) * CS - 10, 20, 0), 0);          /* a cluster boundary (the 1024th when the file has it) */
    EQ(read_cmp(fd, 170, 3, 100, 0), 0);
    EQ(read_cmp(fd, 170, (n - 2) * CS, CS + 777, 0), 0);
    EQ(read_cmp(fd, 170, 1030 * CS % size, 100, 0), 0);
    EQ(fs_lseek(fd, 0, PSP_SEEK_END), size);
    EQ(fs_read(fd, VB, 10), 0);
    fs_close(fd);
    if (!(FLAGS & FST_DETERMINISTIC) && t1 > t0 && t2 > t1)
        fs_print("  %u KB: write %u KB/s, read %u KB/s", (unsigned)(size / 1024),
                 (unsigned)((u64)size * 1000 / 1024 / (t1 - t0)), (unsigned)((u64)size * 1000 / 1024 / (t2 - t1)));
    EQ(fs_remove(f), 0);
    EQ(free_clusters(), f0);
}

typedef void (*group_fn)(void);
static void run(const char *name, group_fn fn)
{
    int f0 = R->fail;
    u32 t0 = fs_now_ms();
    G = name;
    fn();
    fs_after_group(name);
    if (FLAGS & FST_DETERMINISTIC) fs_print("%s %s", R->fail == f0 ? "[ ok ]" : "[FAIL]", name);
    else fs_print("%s %-10s %6u ms", R->fail == f0 ? "[ ok ]" : "[FAIL]", name, (unsigned)(fs_now_ms() - t0));
}

void fst_run(const char *root, int flags, fst_result *r)
{
    SceIoStat st;
    u32 o[5];
    R = r;
    FLAGS = flags;
    rnd_state = 1;
    strncpy(ROOT, root, sizeof ROOT - 1);
    snprintf(DEV, sizeof DEV, "%.*s", (int)(strchr(ROOT, ':') - ROOT + 1), ROOT);   /* "ef0:/X" -> "ef0:" */
    snprintf(FSDEV, sizeof FSDEV, "fat%s", DEV);
    NEWSDK = fs_get_sdk() >= 0x03080000;
    G = "setup";
    if (!bufs()) { CHECK(0); return; }
    if (!(flags & FST_NAMES_ONLY)) {
        if (fs_getstat(ROOT, &st) == 0) fst_rm_tree(ROOT);
        EQ(fs_mkdir(ROOT, 0777), 0);
    } else if (fs_getstat(ROOT, &st) < 0) EQ(fs_mkdir(ROOT, 0777), 0);
    EQ(devinfo(o), 0);
    CS = o[3] * o[4];
    if (!CS) { CHECK(0); return; }
    fs_print("cluster %u bytes, %u of %u clusters free, %s names", (unsigned)CS, (unsigned)o[1], (unsigned)o[0], NEWSDK ? "UTF-8" : "Shift-JIS");
    if (!(flags & FST_NAMES_ONLY)) {
        run("basic", g_basic);
        run("sizes", g_sizes);
        run("unaligned", g_unaligned);
        run("seek", g_seek);
        run("overwrite", g_overwrite);
        run("append", g_append);
        run("gap", g_gap);
        run("truncate", g_trunc);
        run("errors", g_errors);
        run("fd limit", g_fdlimit);
        run("handles", g_handles);
        run("dirs", g_dirs);
    }
    run("listing", g_listing);
    run("names", g_names);
    if (!(flags & FST_NAMES_ONLY)) {
        run("rename", g_rename);
        run("move", g_move);
        run("chstat", g_chstat);
        run("fragment", g_frag);
        run("many", g_many);
        run("savedata", g_savedata);
        if (!(flags & FST_DETERMINISTIC)) { run("async", g_async); run("threads", g_threads); }
        run("devctl", g_devctl);
        run("root", g_root);
        if (flags & FST_BIG) run("big", g_big);
    }
    if (!(flags & FST_KEEP)) {
        G = "cleanup";
        EQ(fst_rm_tree(ROOT), 0);
        EQ(fs_getstat(ROOT, &st), E_ENOENT);
    }
}
