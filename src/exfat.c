/* exfat.c - exFAT on the fatms baseline (DratiniFS; research/docs/FEATURE_PLAN.md phases 2-3).
 * It plugs in below Sony's handler layer: fatfs_identify sees an exFAT boot sector, and every FS
 * worker (fatfs_open, fatfs_read, ...) hands over to its exfat_* twin right after Sony's own entry
 * checks. Everything above stays Sony's -- locking, kernel stacks, path conversion by caller SDK,
 * open-flag rules, the ten fd slots, error codes -- so exFAT files behave like FAT ones to callers.
 * Each worker below mirrors the Sony FAT worker it replaces (same checks, same order).
 *
 * Open files keep Sony's 32-byte FAT-style entry (f->entry) as their in-memory record: attribute,
 * DOS timestamps (exFAT timestamps use the same bit layout), first cluster, size, and the stream's
 * flags in byte 0x0c. It goes back into the on-disk entry set (fresh checksum) whenever it changes.
 * Short (8.3) names are Sony's FAT aliases computed from the long name (alias_init, ~N counted in
 * directory order); they fill d_private and can be opened, as on FAT.
 * No caches beyond Sony's FAT window: directory and bitmap sectors are written before an operation
 * returns. All work runs under the volume mutex, so the work buffers are static (X).
 * Limits: 512-byte sectors, files < 4 GB (Sony's API), clusters up to 16 MB, one active FAT and
 * bitmap (TexFAT volumes mount write-protected). New files and directories use FAT chains;
 * contiguous (NoFatChain) ones written elsewhere are read as they are and chained when they grow. */
#include "fatms.h"

#define X_EOC     0xffffffffu
#define X_NOFAT   0x02                   /* stream GeneralSecondaryFlags bit 1 (kept in entry[0x0c]) */
#define X_END     (int)0x8001000d        /* walked past the directory (as Sony's D_END) */
#define X_INVAL   0xffffffffu
#define XDH_MAGIC 0x54414658u            /* "XFAT" */

typedef struct { u32 lba[3]; u32 off, n; } xloc;                    /* an entry set: <= 19 entries, 3 sectors */
typedef struct { u32 first, nofat, nclus, cl, cidx, off; } xpos;    /* a position in a directory */
typedef struct { u32 first, nofat, size, root; xloc loc; u8 ent[32]; } xdir;
typedef struct { u8 base[11]; u8 pad; u32 count; } xnum;            /* ~N numbering, by directory order */
typedef struct { u32 magic; xpos pos; u32 dots, end, ntab; u8 ent[32]; xnum tab[16]; } xdh;   /* dread state */
#define XDH(d) ((xdh *)((u8 *)(d) + 0x440))                         /* behind Sony's DirHandle, same block */
_Static_assert(sizeof(DirHandle) <= 0x440 && 0x440 + sizeof(xdh) <= 0x5a0, "dir handle room (es.c from 0x5a0)");

static struct {
    u32 fat_off, count;                          /* active FAT start, clusters in the heap */
    u32 bm_cl, bm_len, bm_contig;                /* allocation bitmap */
    u32 free, free_ok, hint;
    u32 nup, up[2048];                           /* upcase: from << 16 | to, for every char that changes */
    u8  sec[512] __attribute__((aligned(64)));   /* the directory sector at hand */
    u8  bm[512] __attribute__((aligned(64)));    /* the bitmap sector at hand */
    u32 sec_lba, bm_lba, bm_dirty;
    u8  set[19 * 32], tmp[19 * 32];              /* entry sets */
    u32 set_n;
    u16 name[256]; u32 nlen;                     /* the name of the set in X.set */
    u16 comp[0x104], pbuf[0x104], lbuf[0x100];
    char oem[0x200];
    fpath fp;
    xnum tab[32]; u32 ntab;                      /* alias numbering for lookups */
    xloc fdloc[FAT_NUM_FD];                      /* where each open file's set lives */
} X;

static u32 le16(const u8 *p) { return (u32)(p[0] | p[1] << 8); }
static u32 le32(const u8 *p) { return le16(p) | le16(p + 2) << 16; }
static void put16(u8 *p, u32 v) { p[0] = (u8)v; p[1] = (u8)(v >> 8); }
static void put32(u8 *p, u32 v) { put16(p, v); put16(p + 2, v >> 16); }
static u32 xcb(fatms_ctx *c) { return (u32)c->bytes_per_sector * c->sectors_per_cluster; }
static u32 wlen(const u16 *s) { u32 n = 0; while (s[n]) n++; return n; }
static int same11(const u8 *a, const u8 *b) { for (int i = 0; i < 11; i++) if (a[i] != b[i]) return 0; return 1; }

/* ---------------- names ---------------- */
static u16 xup(u16 ch)                                          /* the volume's upcase table */
{
    u32 lo = 0, hi = X.nup;
    while (lo < hi) {
        u32 m = (lo + hi) / 2, f = X.up[m] >> 16;
        if (f == ch) return (u16)X.up[m];
        if (f < ch) lo = m + 1; else hi = m;
    }
    return (X.nup == 0 && ch >= 'a' && ch <= 'z') ? (u16)(ch - 0x20) : ch;
}
static int xeq(const u16 *a, const u16 *b, u32 n)
{
    for (u32 i = 0; i < n; i++) if (xup(a[i]) != xup(b[i])) return 0;
    return 1;
}
static u16 xhash(const u16 *n, u32 len)                         /* NameHash */
{
    u16 h = 0;
    for (u32 i = 0; i < len; i++) {
        u16 u = xup(n[i]);
        h = (u16)(((h & 1) ? 0x8000 : 0) + (h >> 1) + (u & 0xff));
        h = (u16)(((h & 1) ? 0x8000 : 0) + (h >> 1) + (u >> 8));
    }
    return h;
}
static u16 xsum(const u8 *s, u32 n)                             /* SetChecksum */
{
    u16 h = 0;
    for (u32 i = 0; i < n * 32; i++)
        if (i != 2 && i != 3) h = (u16)(((h & 1) ? 0x8000 : 0) + (h >> 1) + s[i]);
    return h;
}
/* split "\name" with Sony's fp_split (8.3 or not, the OEM 8.3 string) */
static int xsplit(const u16 *name)
{
    u32 n = wlen(name);
    if (n > 0xff) return FAT_EINVAL;
    X.pbuf[0] = '\\';
    for (u32 i = 0; i <= n; i++) X.pbuf[1 + i] = name[i];
    return fp_split(&X.fp, X.pbuf, X.lbuf, X.oem);
}
/* the 8.3 name FAT would give X.name: the name itself when it is plain 8.3, else Sony's NAME~N
   alias with N counted in directory order through tab -> sfn[11]; returns the tab slot used (or 32
   for a plain name), < 0 when it has none */
static int xalias(xnum *tab, u32 *ntab, u32 cap, u8 *sfn)
{
    int r = xsplit(X.name);
    if (r < 0) return r;
    if (!X.fp.need_lfn) { str_to_sfn(sfn, X.fp.sfn); return 32; }
    fp_alias a;
    if ((r = alias_init(&a, sfn, X.fp.leaf, X.oem)) < 0) return r;
    u32 i = 0;
    while (i < *ntab && !same11(tab[i].base, sfn)) i++;
    if (i == *ntab) {
        if (*ntab == cap) return 32;                            /* too many bases: ~1 */
        for (int k = 0; k < 11; k++) tab[i].base[k] = sfn[k];
        tab[i].count = 0;
        (*ntab)++;
    }
    for (u32 k = tab[i].count++; k > 0; k--) if (alias_next(&a, sfn) < 0) break;
    return (int)i;
}

/* ---------------- sectors, positions, entry sets ---------------- */
static int xsec(fatms_ctx *c, u32 lba)
{
    if (X.sec_lba == lba) return 0;
    int r = c->blk->read_sectors(c, X.sec, lba, 1);
    X.sec_lba = r < 0 ? X_INVAL : lba;
    return r;
}
static void xpos_init(fatms_ctx *c, xpos *p, u32 first, u32 nofat, u32 size)
{
    u32 cb = xcb(c);
    p->first = p->cl = first; p->nofat = nofat; p->nclus = nofat ? (size + cb - 1) / cb : 0;
    p->cidx = p->off = 0;
}
static u32 xpos_lba(fatms_ctx *c, const xpos *p) { return fatfs_clus2lba(c, p->cl) + p->off / 512; }
static int xpos_next(fatms_ctx *c, xpos *p)                     /* one entry on; X_END leaves p as it was */
{
    if ((p->off += 32) < xcb(c)) return 0;
    u32 nx;
    if (p->nofat) nx = (p->cidx + 1 < p->nclus) ? p->cl + 1 : 0;
    else if (fat_get(&c->fe, p->cl, &nx) < 0 || nx >= c->fat_max_cluster) nx = 0;
    if (nx < 2) { p->off -= 32; return X_END; }
    p->cl = nx; p->cidx++; p->off = 0;
    return 0;
}
static int xent(fatms_ctx *c, const xpos *p, u8 **e)
{
    int r = xsec(c, xpos_lba(c, p));
    *e = X.sec + p->off % 512;
    return r;
}
/* the set whose file entry is at *p -> X.set / X.name, *loc; *p ends on its last entry.
   1 = not a usable set (skip it) */
static int xread_set(fatms_ctx *c, xpos *p, xloc *loc)
{
    u8 *e;
    int r = xent(c, p, &e);
    if (r < 0) return r;
    u32 n = 1u + e[1], ns = 1;
    if (e[0] != 0x85 || n < 3 || n > 19) return 1;
    loc->lba[0] = X.sec_lba; loc->off = p->off % 512; loc->n = n;
    memcpy(X.set, e, 32);
    for (u32 i = 1; i < n; i++) {
        if ((r = xpos_next(c, p)) < 0) return r == X_END ? 1 : r;
        if ((r = xent(c, p, &e)) < 0) return r;
        if (X.sec_lba != loc->lba[ns - 1]) loc->lba[ns++] = X.sec_lba;
        memcpy(X.set + i * 32, e, 32);
    }
    X.set_n = n;
    if (X.set[32] != 0xc0) return 1;
    u32 nl = X.set[35], k = 0;
    for (u32 i = 2; i < n && k < nl && X.set[i * 32] == 0xc1; i++)
        for (u32 j = 0; j < 15 && k < nl; j++) X.name[k++] = (u16)le16(X.set + i * 32 + 2 + j * 2);
    X.name[k] = 0; X.nlen = k;
    return (k == nl && k > 0) ? 0 : 1;
}
static int xset_io(fatms_ctx *c, const xloc *loc, u8 *s, int write)   /* a set at a known place */
{
    u32 done = 0, off = loc->off, total = loc->n * 32;
    if (write) put16(s + 2, xsum(s, loc->n));
    for (u32 k = 0; done < total; k++, off = 0) {
        int r = xsec(c, loc->lba[k]);
        if (r < 0) return r;
        u32 take = 512 - off < total - done ? 512 - off : total - done;
        if (!write) { memcpy(s + done, X.sec + off, take); done += take; continue; }
        memcpy(X.sec + off, s + done, take);
        if ((r = c->blk->write_sectors(c, X.sec, loc->lba[k], 1)) < 0) return r;
        done += take;
    }
    return 0;
}
static int xdelete(fatms_ctx *c, const xloc *loc)               /* clear InUse in every entry of a set */
{
    int r = xset_io(c, loc, X.tmp, 0);
    if (r < 0) return r;
    for (u32 i = 0; i < loc->n; i++) X.tmp[i * 32] &= 0x7f;
    return xset_io(c, loc, X.tmp, 1);
}

/* Sony's 32-byte entry from a set (exFAT timestamps share the DOS layout) */
static void xto_ent(const u8 *s, u8 *ent)
{
    memset(ent, ' ', 11); memset(ent + 11, 0, 21);
    ent[0x0b] = s[4] & 0x37;
    ent[0x0c] = s[33];
    ent[0x0d] = s[1];
    put32(ent + 0x0e, le32(s + 8));                             /* create time + date */
    put16(ent + 0x12, le32(s + 16) >> 16);                      /* access date */
    put32(ent + 0x16, le32(s + 12));                            /* modify time + date */
    u32 cl = le32(s + 52);
    put16(ent + 0x1a, cl); put16(ent + 0x14, cl >> 16);
    put32(ent + 0x1c, (ent[0x0b] & 0x10) ? 0 : le32(s + 60) ? 0xffffffffu : le32(s + 56));
}
/* ... and back (names and directory sizes stay as they are; a changed timestamp drops 10ms + UTC) */
static void xfrom_ent(u8 *s, const u8 *ent)
{
    put16(s + 4, (le16(s + 4) & ~0x37u) | (ent[0x0b] & 0x37));
    if (le32(s + 8) != le32(ent + 0x0e)) { put32(s + 8, le32(ent + 0x0e)); s[20] = 0; s[22] = 0; }
    if (le32(s + 12) != le32(ent + 0x16)) { put32(s + 12, le32(ent + 0x16)); s[21] = 0; s[23] = 0; }
    if (le32(s + 16) >> 16 != le16(ent + 0x12)) { put32(s + 16, le16(ent + 0x12) << 16); s[24] = 0; }
    s[33] = (u8)(1 | (ent[0x0c] & X_NOFAT));                    /* AllocationPossible */
    put32(s + 52, le16(ent + 0x1a) | le16(ent + 0x14) << 16);
    if (!(ent[0x0b] & 0x10)) {
        u32 size = le32(ent + 0x1c);
        put32(s + 40, size); put32(s + 44, 0); put32(s + 56, size); put32(s + 60, 0);
    }
}
/* a new set in X.set: file + stream + names (dirsize: a directory's DataLength) */
static void xbuild(const u16 *name, const u8 *ent, u32 dirsize)
{
    u32 nlen = wlen(name), nn = (nlen + 14) / 15, n = 2 + nn;
    memset(X.set, 0, n * 32);
    X.set[0] = 0x85; X.set[1] = (u8)(n - 1);
    X.set[32] = 0xc0; X.set[35] = (u8)nlen;
    put16(X.set + 36, xhash(name, nlen));
    for (u32 i = 0; i < nn; i++) {
        u8 *e = X.set + (2 + i) * 32;
        e[0] = 0xc1;
        for (u32 j = 0; j < 15 && i * 15 + j < nlen; j++) put16(e + 2 + j * 2, name[i * 15 + j]);
    }
    xfrom_ent(X.set, ent);
    if (ent[0x0b] & 0x10) { put32(X.set + 40, dirsize); put32(X.set + 56, dirsize); }
    X.set_n = n;
}

/* ---------------- allocation bitmap ---------------- */
static u32 xbm_lba(fatms_ctx *c, u32 sec)
{
    u32 spc = c->sectors_per_cluster, k = sec / spc, cl = X.bm_cl;
    if (X.bm_contig) cl += k;
    else while (k--) { u32 nx; if (fat_get(&c->fe, cl, &nx) < 0 || nx < 2 || nx >= c->fat_max_cluster) return 0; cl = nx; }
    return fatfs_clus2lba(c, cl) + sec % spc;
}
static int xbm_flush(fatms_ctx *c)
{
    if (!X.bm_dirty) return 0;
    int r = c->blk->write_sectors(c, X.bm, X.bm_lba, 1);
    if (r >= 0) X.bm_dirty = 0;
    return r;
}
static int xbm_load(fatms_ctx *c, u32 cl)                       /* the sector holding cl's bit */
{
    u32 lba = xbm_lba(c, (cl - 2) >> 12);
    if (lba == 0) return FAT_EBADBPB;
    if (lba == X.bm_lba) return 0;
    int r = xbm_flush(c);
    if (r >= 0) r = c->blk->read_sectors(c, X.bm, lba, 1);
    X.bm_lba = r < 0 ? X_INVAL : lba;
    return r;
}
static int xbm_clear(fatms_ctx *c, u32 cl)
{
    if (cl < 2 || cl >= X.count + 2) return 0;
    int r = xbm_load(c, cl);
    if (r < 0) return r;
    u32 b = (cl - 2) & 0xfff;
    if (X.bm[b >> 3] & (1 << (b & 7))) {
        X.bm[b >> 3] &= (u8)~(1 << (b & 7));
        X.bm_dirty = 1;
        if (X.free_ok) X.free++;
    }
    return 0;
}
/* a free cluster from `hint` on (wrapping once) -> *out, marked in use; *out = 0: card full */
static int xalloc(fatms_ctx *c, u32 hint, u32 *out)
{
    *out = 0;
    if (X.free_ok && X.free == 0) return 0;
    u32 max = X.count + 2, cl = (hint >= 2 && hint < max) ? hint : (X.hint >= 2 && X.hint < max) ? X.hint : 2;
    for (u32 n = 0; n < X.count; ) {
        int r = xbm_load(c, cl);
        if (r < 0) return r;
        u32 b = (cl - 2) & 0xfff;
        if ((b & 7) == 0 && X.bm[b >> 3] == 0xff && cl + 8 <= max) { cl += 8; n += 8; }    /* a full byte */
        else if (!(X.bm[b >> 3] & (1 << (b & 7)))) {
            X.bm[b >> 3] |= (u8)(1 << (b & 7));
            X.bm_dirty = 1;
            if (X.free_ok) X.free--;
            X.hint = cl + 1;
            *out = cl;
            return 0;
        } else { cl++; n++; }
        if (cl >= max) cl = 2;
    }
    return 0;
}
static int xcount_free(fatms_ctx *c)                            /* count once, then keep the count */
{
    if (X.free_ok) return 0;
    u32 n = 0;
    for (u32 cl = 2; cl < X.count + 2; cl += 4096) {
        int r = xbm_load(c, cl);
        if (r < 0) return r;
        u32 bits = X.count + 2 - cl < 4096 ? X.count + 2 - cl : 4096;
        for (u32 b = 0; b < bits; b++) n += !(X.bm[b >> 3] & (1 << (b & 7)));
    }
    X.free = n; X.free_ok = 1;
    return 0;
}

/* ---------------- cluster chains ---------------- */
static int xchain(fatms_ctx *c, u32 first, u32 n)              /* a contiguous run gets a FAT chain */
{
    for (u32 i = 0; i < n; i++) {
        int r = fat_set(&c->fe, first + i, i + 1 < n ? first + i + 1 : X_EOC);
        if (r < 0) return r;
    }
    return 0;
}
static int xextend(fatms_ctx *c, u32 last, u32 count, u32 *got)  /* link `count` new clusters after last */
{
    *got = 0;
    for (u32 i = 0; i < count; i++) {
        u32 nc;
        int r = xalloc(c, last + 1, &nc);
        if (r < 0) return r;
        if (nc == 0) return FAT_ENOSPC;
        if ((r = fat_set(&c->fe, nc, X_EOC)) < 0 || (r = fat_set(&c->fe, last, nc)) < 0) return r;
        last = nc;
        (*got)++;
    }
    return 0;
}
static int xfree(fatms_ctx *c, u32 cl, int nofat, u32 bytes)   /* release a chain or a contiguous run */
{
    if (cl < 2) return 0;
    u32 cb = xcb(c);
    if (nofat) {
        u32 n = (bytes + cb - 1) / cb;
        for (u32 i = 0; i < (n ? n : 1); i++) { int r = xbm_clear(c, cl + i); if (r < 0) return r; }
        return 0;
    }
    for (u32 n = 0; n < X.count && cl >= 2 && cl < c->fat_max_cluster; n++) {
        u32 nx;
        if (fat_get(&c->fe, cl, &nx) < 0) break;
        int r = fat_set(&c->fe, cl, 0);
        if (r >= 0) r = xbm_clear(c, cl);
        if (r < 0) return r;
        cl = nx;
    }
    return 0;
}
static int xflush(fatms_ctx *c)                                 /* bitmap, then FAT (Sony's flush) */
{
    int r = xbm_flush(c);
    if (r >= 0) r = fatfs_flush(c);
    return r > 0 ? 0 : r;
}
/* contiguous file data (NoFatChain): partial clusters via Sony's sector_io, whole ones in one go */
static int xcontig(fatms_ctx *c, int write, u8 *buf, u32 first, u32 pos, u32 len, u32 *done)
{
    u32 cb = xcb(c), total = 0;
    *done = 0;
    while (total < len) {
        u32 cl = first + pos / cb, off = pos % cb, got;
        int r;
        if (off == 0 && len - total >= cb) {
            u32 nc = (len - total) / cb, lba = fatfs_clus2lba(c, cl), ns = nc * c->sectors_per_cluster;
            r = write ? c->blk->write_sectors(c, buf, lba, ns) : c->blk->read_sectors(c, buf, lba, ns);
            got = nc * cb;
        } else r = fatfs_sector_io(c, write, off, len - total, buf, fatfs_clus2lba(c, cl), &got);
        if (r < 0) return r;
        buf += got; pos += got; total += got;
        *done = total;
    }
    return 0;
}

/* ---------------- directories ---------------- */
static void xroot(fatms_ctx *c, xdir *d)
{
    memset(d, 0, sizeof(*d));
    d->first = c->root_cluster; d->root = 1;
    d->ent[0x0b] = 0x10;
}
static void xenter(xdir *d, const xloc *loc)                    /* d = the directory whose set is in X.set */
{
    d->first = le32(X.set + 52); d->nofat = X.set[33] & X_NOFAT; d->size = le32(X.set + 56);
    d->root = 0; d->loc = *loc;
    xto_ent(X.set, d->ent);
}
/* scan d for `name` (any case), or with alias set, for the entry whose 8.3 alias is `name` */
static int xscan(fatms_ctx *c, const xdir *d, const u16 *name, int alias, xloc *loc)
{
    u8 want[11], sfn[11];
    u32 len = wlen(name);
    if (alias) {
        int r = xsplit(name);
        if (r < 0 || X.fp.need_lfn) return FAT_ENOENT;          /* not a plain 8.3 name */
        str_to_sfn(want, X.fp.sfn);
        X.ntab = 0;
    }
    xpos p;
    xpos_init(c, &p, d->first, d->nofat, d->size);
    for (;;) {
        u8 *e;
        int r = xent(c, &p, &e);
        if (r < 0) return r;
        if (e[0] == 0) return FAT_ENOENT;
        if (e[0] == 0x85) {
            if ((r = xread_set(c, &p, loc)) < 0) return r;
            if (r == 0) {
                if (!alias) { if (X.nlen == len && xeq(X.name, name, len)) return 0; }
                else if (xalias(X.tab, &X.ntab, 32, sfn) >= 0 && same11(sfn, want)) return 0;
            }
        }
        if ((r = xpos_next(c, &p)) == X_END) return FAT_ENOENT;
        if (r < 0) return r;
    }
}
static int xlookup(fatms_ctx *c, const xdir *d, const u16 *name, xloc *loc)
{
    int r = xscan(c, d, name, 0, loc);
    if (r != FAT_ENOENT) return r;
    for (const u16 *s = name; *s; s++) if (*s == '~') return xscan(c, d, name, 1, loc);
    return r;
}
/* the directory at a Sony parent path ("\a\b\"): every component must be a directory */
static int xwalk(fatms_ctx *c, const u16 *dir, xdir *d)
{
    xroot(c, d);
    for (int i = 0;;) {
        if (dir[i] == '\\') i++;
        if (dir[i] == 0) return 0;
        int n = 0;
        do X.comp[n++] = dir[i++]; while (dir[i] != 0 && dir[i] != '\\' && n < 0x103);
        X.comp[n] = 0;
        xloc loc;
        int r = xlookup(c, d, X.comp, &loc);
        if (r < 0) return r;
        if (!(X.set[4] & 0x10)) return FAT_ENOENT;              /* a file in the middle of the path */
        xenter(d, &loc);
    }
}
/* the directory ran out of entries: one more (zeroed) cluster; *p moves onto it */
static int xgrow(fatms_ctx *c, xdir *d, xpos *p)
{
    u32 cb = xcb(c), nc;
    int r = xalloc(c, p->cl + 1, &nc);
    if (r < 0) return r;
    if (nc == 0) return FAT_ENOSPC;
    if (p->nofat && nc != p->cl + 1) {                          /* no longer contiguous: chain it */
        if ((r = xchain(c, p->first, p->nclus)) < 0) return r;
        p->nofat = d->nofat = 0;
    }
    if (!p->nofat && ((r = fat_set(&c->fe, nc, X_EOC)) < 0 || (r = fat_set(&c->fe, p->cl, nc)) < 0)) return r;
    r = dir_zero_cluster(c, nc);
    X.sec_lba = X_INVAL;                                        /* written behind the sector buffer */
    if (r < 0) return r;
    d->size += cb;
    if (!d->root) {                                             /* its DataLength (and chain flag) */
        if ((r = xset_io(c, &d->loc, X.tmp, 0)) < 0) return r;
        put32(X.tmp + 40, d->size); put32(X.tmp + 56, d->size);
        X.tmp[33] = (u8)(1 | (d->nofat ? X_NOFAT : 0));
        if ((r = xset_io(c, &d->loc, X.tmp, 1)) < 0) return r;
    }
    p->cl = nc; p->cidx++; p->off = 0;
    if (p->nofat) p->nclus++;
    return 0;
}
/* a run of n unused entries in d for the set in X.set (growing d when needed) -> *loc */
static int xplace(fatms_ctx *c, xdir *d, u32 n, xloc *loc)
{
    xpos p, start;
    u32 run = 0;
    xpos_init(c, &p, d->first, d->nofat, d->size);
    start = p;
    for (;;) {
        u8 *e;
        int r = xent(c, &p, &e);
        if (r < 0) return r;
        if (e[0] & 0x80) run = 0;
        else if (run++ == 0) start = p;
        if (run == n) break;
        if ((r = xpos_next(c, &p)) == X_END) {
            if ((r = xgrow(c, d, &p)) < 0) return r;
            start.nofat = p.nofat; start.nclus = p.nclus;
        } else if (r < 0) return r;
    }
    loc->n = n; loc->off = start.off % 512; loc->lba[0] = xpos_lba(c, &start);
    for (u32 i = 1, ns = 1; i < n; i++) {
        xpos_next(c, &start);
        u32 lba = xpos_lba(c, &start);
        if (lba != loc->lba[ns - 1]) loc->lba[ns++] = lba;
    }
    return 0;
}
static int xempty(fatms_ctx *c, const xdir *d)
{
    xpos p;
    xpos_init(c, &p, d->first, d->nofat, d->size);
    for (;;) {
        u8 *e;
        int r = xent(c, &p, &e);
        if (r < 0) return r;
        if (e[0] == 0) return 0;
        if (e[0] & 0x80) return FAT_EACCES;                      /* not empty */
        if ((r = xpos_next(c, &p)) == X_END) return 0;
        if (r < 0) return r;
    }
}
static int xslot(fatms_ctx *c, OpenFile *f)
{
    for (int i = 0; i < FAT_NUM_FD; i++) if (c->fd[i] == (void *)f) return i;
    return -1;
}
/* an open file's entry -> its set on the card (unless the set is gone: renamed or removed) */
static int xput(fatms_ctx *c, OpenFile *f)
{
    int i = xslot(c, f);
    if (i < 0) return 0;
    xloc *loc = &X.fdloc[i];
    int r = xset_io(c, loc, X.tmp, 0);
    if (r < 0) return r;
    if (X.tmp[0] != 0x85 || X.tmp[32] != 0xc0 || 1u + X.tmp[1] != loc->n) return 0;
    xfrom_ent(X.tmp, f->entry);
    return xset_io(c, loc, X.tmp, 1);
}

/* ---------------- mount ---------------- */
int exfat_identify(fatms_ctx *c, const u8 *b)                   /* 1 = not exFAT */
{
    if (strncmp((const char *)b + 3, "EXFAT   ", 8) != 0) return 1;
    c->exfat = 1;
    u32 nf = b[110], active = (nf == 2 && (le16(b + 106) & 1)) ? 1 : 0;
    u32 fatoff = le32(b + 80), fatlen = le32(b + 84), heap = le32(b + 88), count = le32(b + 92);
    if (b[510] != 0x55 || b[511] != 0xaa || b[108] != 9 || b[109] > 15 || nf < 1 || nf > 2 ||
        le32(b + 76) != 0 || count < 1 || fatlen < (count + 2 + 127) / 128 || fatoff + nf * fatlen > heap ||
        heap + (count << b[109]) > le32(b + 72))
        return FAT_EBADBPB;
    c->bytes_per_sector = 0x200;
    c->sectors_per_cluster = (u16)(1 << b[109]);
    c->cluster_shift = b[109];
    c->reserved_sectors = 0; c->root_entries = 0; c->num_fats = 1; c->media_type = 0xf8;
    c->total_sectors = le32(b + 72);
    c->fat_size = fatlen;
    c->root_cluster = le32(b + 96);
    c->fsinfo_sector = 0; c->backup_boot = 12;
    c->root_dir_lba = 0; c->data_start_lba = heap;
    c->fat_max_cluster = count + 2;
    c->fat_type = 32;
    c->ms_sectors_per_cluster = 1; c->ms_unit = 0xffff; c->ms_max_cluster = 0xffffffffu;
    X.fat_off = fatoff + active * fatlen;
    X.count = count;
    int v[4] = { 0 };
    if ((k_sceIoIoctl(c->ctrl_fd, 0x2125008, 0, 0, v, 4) == 0 && v[0] == 0) || nf == 2) c->media_flags |= MF_WP;
    else c->media_flags &= ~MF_WP;                              /* TexFAT (two FATs): read only */
    return 0;
}

/* after Sony's FAT-window setup: the active FAT, then the root's bitmap / upcase entries */
int exfat_mount(fatms_ctx *c)
{
    c->fe.reserved = X.fat_off;
    X.sec_lba = X.bm_lba = X_INVAL; X.bm_dirty = 0;
    X.free_ok = 0; X.hint = 2; X.nup = 0; X.bm_cl = 0;
    if (c->root_cluster < 2 || c->root_cluster >= c->fat_max_cluster) return FAT_EBADBPB;
    u32 up_cl = 0, up_len = 0, cb = xcb(c);
    xpos p;
    xpos_init(c, &p, c->root_cluster, 0, 0);
    for (;;) {
        u8 *e;
        int r = xent(c, &p, &e);
        if (r < 0) return r;
        if (e[0] == 0) break;
        if (e[0] == 0x81 && !(e[1] & 1)) { X.bm_cl = le32(e + 20); X.bm_len = le32(e + 24); }
        else if (e[0] == 0x82) { up_cl = le32(e + 20); up_len = le32(e + 24); }
        if (xpos_next(c, &p) < 0) break;
    }
    if (X.bm_cl < 2 || X.bm_cl >= c->fat_max_cluster || X.bm_len < (X.count + 7) / 8) return FAT_EBADBPB;
    u32 nb = (X.bm_len + cb - 1) / cb, cl = X.bm_cl;            /* formatters lay the bitmap out in one run */
    X.bm_contig = 1;
    for (u32 i = 1; i < nb; i++) {
        u32 nx;
        if (fat_get(&c->fe, cl, &nx) < 0 || nx == 0) break;     /* no chain recorded: contiguous */
        if (nx != cl + 1) { X.bm_contig = 0; break; }
        cl = nx;
    }
    /* upcase table (compressed: FFFF n = n chars map to themselves) -> the chars that change */
    u32 idx = 0, skip = 0, got = 0;
    cl = up_cl;
    while (cl >= 2 && cl < c->fat_max_cluster && got < up_len) {
        for (u32 s = 0; s < c->sectors_per_cluster && got < up_len; s++) {
            int r = xsec(c, fatfs_clus2lba(c, cl) + s);
            if (r < 0) return r;
            for (u32 o = 0; o < 512 && got < up_len; o += 2, got += 2) {
                u32 v = le16(X.sec + o);
                if (skip) { idx += v; skip = 0; }
                else if (v == 0xffff) skip = 1;
                else { if (v != idx && idx < 0x10000 && X.nup < 2048) X.up[X.nup++] = idx << 16 | v; idx++; }
            }
        }
        u32 nx;
        if (fat_get(&c->fe, cl, &nx) < 0 || nx == 0) nx = cl + 1;
        cl = nx;
    }
    return 0;
}

/* ---------------- workers (each mirrors Sony's FAT one in fatfs_ops.c / read / write) ---------------- */
int exfat_open(fatms_ctx *c, fpath *p, unsigned flags, OpenFile **out)
{
    X.sec_lba = X_INVAL;
    xdir d;
    xloc loc;
    int r = xwalk(c, p->dir, &d);
    if (r < 0) return r;
    r = xlookup(c, &d, p->leaf, &loc);
    int exists = (r == 0);
    if (r < 0 && r != FAT_ENOENT) return r;
    if ((r = flagcheck(flags, exists)) < 0) return r;
    if ((flags & 0x600) == 0x600 && (c->media_flags & MF_WP)) return FAT_EACCES;
    int idx = 0;
    u32 st = slot_scan(c, p, &idx);
    if (st == 0 || st == 2) return FAT_EMFILE;
    if ((flags & 0x600) == 0x600 && (st & 2)) return FAT_EACCES;
    OpenFile *f = (OpenFile *)c->fd[idx];
    memset(f, 0, sizeof(*f));
    int trunc = 0;
    if ((flags & 0x200) && !exists) {
        if (c->media_flags & MF_WP) return FAT_EACCES;
        u8 ent[32];
        memset(ent, 0, 32);
        dirent_stamp(ent, 1);
        ent[0x0b] = 0x20;
        xbuild(p->leaf, ent, 0);
        if ((r = xplace(c, &d, X.set_n, &loc)) < 0 || (r = xset_io(c, &loc, X.set, 1)) < 0) { xflush(c); return r; }
    } else {
        if (!exists) return FAT_ENOENT;                         /* Sony: the second lookup fails */
        if (X.set[4] & 0x10) return FAT_EACCES;                 /* a directory */
        if ((flags & 2) && ((X.set[4] & 0x19) || (c->media_flags & MF_WP) || le32(X.set + 60))) return FAT_EACCES;
        trunc = (flags & 0x400) != 0;
    }
    memcpy(&f->path, p, sizeof(fpath));
    xto_ent(X.set, f->entry);
    f->first_cluster = le32(X.set + 52);
    X.fdloc[idx] = loc;
    if (trunc && f->first_cluster) {
        u32 cl = f->first_cluster, bytes = le32(X.set + 56);
        int nofat = f->entry[0x0c] & X_NOFAT;
        f->first_cluster = 0; F_SIZE(f) = 0;
        f->entry[0x0c] &= (u8)~X_NOFAT;
        put16(f->entry + 0x1a, 0); put16(f->entry + 0x14, 0);
        xfrom_ent(X.set, f->entry);
        if ((r = xset_io(c, &loc, X.set, 1)) < 0) return r;
        if ((r = xfree(c, cl, nofat, bytes)) < 0) return r;
    }
    if ((r = xflush(c)) < 0) return r;
    f->position = 0; f->cur_cluster = f->first_cluster; f->cur_cluster_index = 0; f->ckpt_count = 0;
    f->rsv[0] = f->rsv[1] = f->rsv[2] = 0;
    memset(f->ckpt, 0, sizeof(f->ckpt));
    f->inuse++;
    if (flags & 0x100) f->position = F_SIZE(f);
    f->oflags = flags;
    f->is_dir = 0;
    *out = f;
    return 0;
}

int exfat_close(fatms_ctx *c, OpenFile *f)
{
    X.sec_lba = X_INVAL;
    int r = (f->oflags & 2) ? xput(c, f) : 0;
    if (r >= 0) r = xflush(c);
    return r;
}

int exfat_read(fatms_ctx *c, OpenFile *f, void *buf, int len)
{
    if (!(f->oflags & 1)) return FAT_EACCES;                    /* not opened for reading */
    u32 pos = f->position, size = F_SIZE(f), n = (u32)len;
    if (pos >= size) return 0;
    if (size - pos < n) n = size - pos;
    if (n == 0) return 0;
    u32 cb = xcb(c), cl, done = 0;
    int r;
    if (f->entry[0x0c] & X_NOFAT) r = xcontig(c, 0, (u8 *)buf, f->first_cluster, pos, n, &done);
    else if ((r = fatfs_seek_cluster(c, f, pos / cb, &cl)) >= 0) r = fatfs_data_io(c, f, 0, (u8 *)buf, pos, n, cl, &done);
    if (r < 0) return r;
    f->position = pos + done;
    return (int)done;
}

int exfat_write(fatms_ctx *c, OpenFile *f, const void *buf, int len)
{
    X.sec_lba = X_INVAL;
    u32 n = (u32)len, cb = xcb(c), size = F_SIZE(f);
    if (f->oflags & 0x100) f->position = size;                  /* O_APPEND */
    u32 pos = f->position;
    if (~pos < n) n = ~pos;                                     /* stay below 4 GB */
    u32 end = pos + n, had = f->first_cluster;
    u32 clusters = size / cb + (size % cb != 0);
    u32 alloc = (clusters <= 0xffffffffu / cb) ? clusters * cb : 0xffffffffu;
    int nofat = f->entry[0x0c] & X_NOFAT, r;
    if (f->first_cluster == 0) {                                /* empty file: first cluster */
        u32 nc;
        if ((r = xalloc(c, 0, &nc)) < 0) return r;
        if (nc == 0) return FAT_ENOSPC;
        if ((r = fat_set(&c->fe, nc, X_EOC)) < 0) return r;
        f->first_cluster = nc;
        put16(f->entry + 0x1a, nc); put16(f->entry + 0x14, nc >> 16);
        f->entry[0x0c] &= (u8)~X_NOFAT;
        nofat = 0;
        f->cur_cluster = nc; f->cur_cluster_index = 0;
        alloc = cb;
    } else if (alloc == 0) alloc = cb;
    if (alloc < end) {                                          /* extend from the last cluster */
        u32 need = (end - alloc) / cb + ((end - alloc) % cb != 0), last, got = 0;
        if (nofat) {                                            /* growing a contiguous file: chain it */
            if ((r = xchain(c, f->first_cluster, alloc / cb)) < 0) return r;
            f->entry[0x0c] &= (u8)~X_NOFAT;
            nofat = 0;
            f->cur_cluster = f->first_cluster; f->cur_cluster_index = 0; f->ckpt_count = 0;
        }
        if (fatfs_seek_cluster(c, f, (alloc - 1) / cb, &last) < 0) return FAT_ENOSPC;
        r = xextend(c, last, need, &got);
        if (r < 0 && got == 0 && had != 0) { xflush(c); return 0; }   /* card full: nothing written */
        if (need != got) {                                      /* short allocation: trim the write */
            n -= (need - got) * cb;
            if (end % cb) n += cb - end % cb;
        }
    }
    u32 cl = 0, done = 0;
    if (!nofat && (r = fatfs_seek_cluster(c, f, pos / cb, &cl)) < 0) {
        if (had == 0) { xfree(c, f->first_cluster, 0, 0); f->first_cluster = 0; put16(f->entry + 0x1a, 0); put16(f->entry + 0x14, 0); }
        xflush(c);
        return r;
    }
    dirent_stamp(f->entry, 0);
    r = nofat ? xcontig(c, 1, (u8 *)buf, f->first_cluster, pos, n, &done)
              : fatfs_data_io(c, f, 1, (u8 *)buf, pos, n, cl, &done);
    if (r < 0) { xflush(c); return r; }
    pos += done;
    if (size < pos) F_SIZE(f) = pos;
    F_ATTR(f) |= 0x20;                                          /* archive */
    f->position = pos;
    /* exFAT has no DirentCache: allocation (bitmap, FAT) and then the entry reach the card now */
    if ((r = xflush(c)) < 0 || (r = xput(c, f)) < 0) return r;
    return (int)done;
}

int exfat_dopen(fatms_ctx *c, fpath *p, DirHandle **out)
{
    X.sec_lba = X_INVAL;
    *out = 0;
    DirHandle *d = (DirHandle *)fatfs_get_scratch(c);
    if (!d) return FAT_ENOMEM;
    memset(&d->it, 0, sizeof(d->it));
    int seps = 0, chars = 0, r = 0;
    for (const u16 *s = p->dir; seps < 2; ) {                   /* first component of the dir path */
        while (*s == ' ') s++;
        if (*s == '\\') { seps++; s++; continue; }
        if (*s++ == 0) break;
        chars++;
    }
    xdir t;
    if (chars == 0) {
        if (seps > 1) r = FAT_EINVAL;
        else { d->path.dir[0] = '\\'; d->path.dir[1] = 0; xroot(c, &t); }
    } else {
        fp_wcscpy(X.pbuf, p->dir);
        int n = fp_wcslen(X.pbuf);
        if (n && X.pbuf[n - 1] == '\\') X.pbuf[n - 1] = 0;
        xdir pd;
        xloc loc;
        r = fp_split(&d->path, X.pbuf, X.lbuf, X.oem);
        if (r >= 0) r = xwalk(c, d->path.dir, &pd);
        if (r >= 0) r = xlookup(c, &pd, d->path.leaf, &loc);
        if (r >= 0 && !(X.set[4] & 0x10)) r = FAT_ENOTDIR;
        if (r >= 0) xenter(&t, &loc);
    }
    if (r < 0) { fatfs_put_scratch(c, d); return r; }
    d->path.leaf[0] = '*'; d->path.leaf[1] = 0;                 /* default wildcard */
    xdh *h = XDH(d);
    memset(h, 0, sizeof(*h));
    h->magic = XDH_MAGIC;
    xpos_init(c, &h->pos, t.first, t.nofat, t.size);
    h->dots = t.root ? 2 : 0;                                   /* FAT keeps "." and ".." in subdirectories */
    memcpy(h->ent, t.ent, 32);
    d->it.lba = 1;
    d->is_dir = 1;
    d->ctx = c;
    *out = d;
    return 0;
}

/* Sony's dread output (FUN_00007610's tail): names by caller SDK, d_private (also es.c's) */
int dread_emit(fatms_ctx *c, SceIoDirent *de, void *priv, const u8 *ent, const u16 *leaf, const char *sfn)
{
    char *s = (char *)fatfs_get_scratch(c);
    if (!s) return FAT_ENOMEM;
    int r = 0;
    dirent_stat(ent, &de->d_stat);
    if (sceKernelGetCompiledSdkVersion() < 0x3080000) {
        if ((r = sceCodepage_driver_47BDF633(s, 0x800, leaf)) < 0) goto out;
        if (strlen(s) < 0x100) strncpy(de->d_name, s, 0x100);
        else strncpy(de->d_name, sfn, 0xd);
        if (priv) {
            strncpy((char *)priv, sfn, 0xd);
            if ((r = sceCodepage_driver_47BDF633((char *)priv + 0xd, 0x100, leaf)) < 0) goto out;
        }
    } else {
        sceCodepage_driver_907CBFD2(s, 0x800, leaf);
        if (strlen(s) < 0x100) strncpy(de->d_name, s, 0x100);
        else {
            if ((r = sceCodepage_driver_855C5C2E((u16 *)s, 0x400, sfn)) < 0) goto out;
            sceCodepage_driver_907CBFD2(de->d_name, 0x100, (u16 *)s);
        }
        if (priv) {
            if (*(u32 *)priv != 0x414) { r = FAT_EINVAL; goto out; }
            strncpy((char *)priv + 4, sfn, 0xd);
            sceCodepage_driver_907CBFD2((char *)priv + 0x14, 0x400, leaf);
        }
    }
    r = 0;
out:
    fatfs_put_scratch(c, s);
    return r;
}

int exfat_dread(fatms_ctx *c, DirHandle *d, SceIoDirent *de, void *priv)
{
    X.sec_lba = X_INVAL;
    xdh *h = XDH(d);
    if (h->magic != XDH_MAGIC) return FAT_EINVAL;
    u16 w[16];
    char sfn[16];
    for (;;) {
        if (h->end) return FAT_ENOENT;
        if (h->dots < 2) {                                      /* "." and ".." (sfn-mode match, as FAT) */
            w[0] = '.'; w[1] = h->dots ? '.' : 0; w[2] = 0;
            sfn[0] = '.'; sfn[1] = h->dots ? '.' : 0; sfn[2] = 0;
            if (fp_wildmatch(d->path.leaf, w, 1) == 0) {
                int r = dread_emit(c, de, priv, h->ent, w, sfn);
                if (r < 0) return r;
                h->dots++;
                return 0;
            }
            h->dots++;
            continue;
        }
        xpos save = h->pos;
        xloc loc;
        u8 *e;
        int r = xent(c, &h->pos, &e), shown = 0;
        if (r < 0) return r;
        if (e[0] == 0) { h->end = 1; return FAT_ENOENT; }
        if (e[0] == 0x85) {
            if ((r = xread_set(c, &h->pos, &loc)) < 0) return r;
            if (r == 0) {
                u8 a[11], ent[32];
                int slot = xalias(h->tab, &h->ntab, 16, a);     /* counted whether shown or not */
                if (slot >= 0) sfn_to_str(sfn, a); else sfn[0] = 0;
                w[0] = 0;
                if (sfn[0] && sceCodepage_driver_855C5C2E(w, 0x1a, sfn) < 0) w[0] = 0;
                if (fp_wildmatch(d->path.leaf, X.name, 0) == 0 || (w[0] && fp_wildmatch(d->path.leaf, w, 1) == 0)) {
                    xto_ent(X.set, ent);
                    if ((r = dread_emit(c, de, priv, ent, X.name, sfn)) < 0) {   /* not consumed: same entry next time */
                        h->pos = save;
                        if (slot >= 0 && slot < 16 && h->tab[slot].count-- == 1 && (u32)slot == h->ntab - 1) h->ntab--;
                        return r;
                    }
                    shown = 1;
                }
            }
        }
        if ((r = xpos_next(c, &h->pos)) == X_END) h->end = 1;
        else if (r < 0) return r;
        if (shown) return 0;
    }
}

int exfat_getstat(fatms_ctx *c, fpath *p, SceIoStat *st)
{
    X.sec_lba = X_INVAL;
    xdir d;
    xloc loc;
    int r = xwalk(c, p->dir, &d);
    if (r < 0 || (r = xlookup(c, &d, p->leaf, &loc)) < 0) return r;
    u8 ent[32];
    xto_ent(X.set, ent);
    dirent_stat(ent, st);
    return 0;
}

int exfat_chstat(fatms_ctx *c, fpath *p, const SceIoStat *st, int bits)
{
    X.sec_lba = X_INVAL;
    xdir d;
    xloc loc;
    int r = xwalk(c, p->dir, &d), idx;
    if (r < 0) return r;
    if (c->media_flags & MF_WP) return FAT_EACCES;
    if (slot_scan(c, p, &idx) & 2) return FAT_EACCES;           /* busy: the file is open */
    if ((r = xlookup(c, &d, p->leaf, &loc)) < 0) return r;
    u8 ent[32];
    xto_ent(X.set, ent);
    if (bits & 2) {
        if ((ent[0x0b] & 0x10) != (st->st_attr & 0x10)) return FAT_EINVAL;   /* dir bit is fixed */
        ent[0x0b] = (u8)st->st_attr;
    }
    if (bits & 1) {                                             /* any write bit clears read-only */
        if (st->st_mode & 0x92) ent[0x0b] &= ~1; else ent[0x0b] |= 1;
    }
    if (bits & 8) { u32 dos = 0; sceRtc_driver_A4A5BF1B(&st->sce_st_ctime, &dos); put32(ent + 0x0e, dos); }
    if (bits & 0x20) { u32 dos = 0; sceRtc_driver_A4A5BF1B(&st->sce_st_mtime, &dos); put32(ent + 0x16, dos); }
    xfrom_ent(X.set, ent);
    if ((r = xset_io(c, &loc, X.set, 1)) < 0) return r;
    return xflush(c);
}

int exfat_mkdir(fatms_ctx *c, fpath *p)
{
    X.sec_lba = X_INVAL;
    if (c->media_flags & MF_WP) return FAT_EACCES;
    xdir d;
    xloc loc;
    u32 cl, cb = xcb(c);
    int r = xwalk(c, p->dir, &d);
    if (r < 0) return r;
    r = xlookup(c, &d, p->leaf, &loc);
    if (r == 0) return FAT_EEXIST;
    if (r != FAT_ENOENT) return r;
    if ((r = xalloc(c, 0, &cl)) < 0) return r;
    if (cl == 0) return FAT_ENOSPC;
    if ((r = fat_set(&c->fe, cl, X_EOC)) >= 0) { r = dir_zero_cluster(c, cl); X.sec_lba = X_INVAL; }
    if (r < 0) { fat_set(&c->fe, cl, 0); xbm_clear(c, cl); xflush(c); return r; }
    u8 ent[32];
    memset(ent, 0, 32);
    dirent_stamp(ent, 1);
    ent[0x0b] = 0x10;
    put16(ent + 0x1a, cl); put16(ent + 0x14, cl >> 16);
    xbuild(p->leaf, ent, cb);
    if ((r = xplace(c, &d, X.set_n, &loc)) < 0 || (r = xset_io(c, &loc, X.set, 1)) < 0) {
        fat_set(&c->fe, cl, 0); xbm_clear(c, cl); xflush(c); return r;
    }
    return xflush(c);
}

int exfat_remove(fatms_ctx *c, fpath *p)
{
    X.sec_lba = X_INVAL;
    xdir d;
    xloc loc;
    int r = xwalk(c, p->dir, &d), idx;
    if (r < 0) return r;
    if (c->media_flags & MF_WP) return FAT_EACCES;
    if (slot_scan(c, p, &idx) & 2) return FAT_EACCES;           /* open files can't be removed */
    if ((r = xlookup(c, &d, p->leaf, &loc)) < 0) return r;
    if (X.set[4] & 0x19) return FAT_EACCES;                     /* read-only or directory */
    u32 cl = le32(X.set + 52), bytes = le32(X.set + 56);
    int nofat = X.set[33] & X_NOFAT;
    if ((r = xdelete(c, &loc)) >= 0 && (r = xfree(c, cl, nofat, bytes)) >= 0) r = xflush(c);
    return r;
}

int exfat_rmdir(fatms_ctx *c, fpath *p)
{
    X.sec_lba = X_INVAL;
    xdir d, t;
    xloc loc;
    int r = xwalk(c, p->dir, &d);
    if (r < 0) return r;
    if (c->media_flags & MF_WP) return FAT_EACCES;
    if ((r = xlookup(c, &d, p->leaf, &loc)) < 0) return r;
    if ((X.set[4] & 0x19) != 0x10) return FAT_EACCES;           /* must be a writable directory */
    xenter(&t, &loc);
    if ((r = xempty(c, &t)) >= 0 && (r = xdelete(c, &loc)) >= 0 && (r = xfree(c, t.first, t.nofat, t.size)) >= 0)
        r = xflush(c);
    return r;
}

/* Sony's move: a new set under dst's name (data, times and flags kept), then the old one goes */
int exfat_move(fatms_ctx *c, fpath *src, fpath *dst)
{
    X.sec_lba = X_INVAL;
    if (c->media_flags & MF_WP) return FAT_EACCES;
    xdir sd, dd;
    xloc sloc, dloc;
    int r = xwalk(c, src->dir, &sd);
    if (r < 0 || (r = xlookup(c, &sd, src->leaf, &sloc)) < 0) return r;
    if (X.set[4] & 1) return FAT_EACCES;
    u8 ent[32];
    xto_ent(X.set, ent);
    u32 dirsize = le32(X.set + 56);
    if ((r = xwalk(c, dst->dir, &dd)) < 0) return r;
    r = xlookup(c, &dd, dst->leaf, &dloc);
    if (r == 0) return FAT_EEXIST;
    if (r != FAT_ENOENT) return r;
    xbuild(dst->leaf, ent, dirsize);
    if ((r = xplace(c, &dd, X.set_n, &dloc)) >= 0 && (r = xset_io(c, &dloc, X.set, 1)) >= 0 &&
        (r = xdelete(c, &sloc)) >= 0)
        r = xflush(c);
    return r;
}

/* devctl 0x2425818: free clusters from the bitmap, published where Sony's devinfo reads them */
int exfat_sync_free(fatms_ctx *c)
{
    int r = xcount_free(c);
    if (r < 0) return r;
    c->fe.free_clusters = X.free | 0x80000000u;
    c->fe.free_contig = X.free | 0x80000000u;
    return 0;
}
/* devctl 0x242d81c: free root entries (the root grows on demand, as FAT32's: capped at 0x1000) */
int exfat_root_free(fatms_ctx *c, u32 *out)
{
    X.sec_lba = X_INVAL;
    int r = xcount_free(c);
    if (r < 0) return r;
    u32 per = xcb(c) / 32, n = 0;
    if ((u64)X.free * per > 0xfff) { *out = 0x1000; return 0; }
    xpos p;
    xpos_init(c, &p, c->root_cluster, 0, 0);
    for (;;) {
        u8 *e;
        if ((r = xent(c, &p, &e)) < 0) return r;
        if (!(e[0] & 0x80)) n++;
        if ((r = xpos_next(c, &p)) == X_END) break;
        if (r < 0) return r;
    }
    n += X.free * per;
    *out = n > 0x1000 ? 0x1000 : n;
    return 0;
}
