/* es.c - ExtremeSpeed (ES) partitions (DratiniFS; research/docs/FEATURE_PLAN.md phase 4).
 * An MBR partition of type 0xDA holds ISOs back to back (format v3: shared/extremespeed_format.h,
 * written by the ES app and the PC tool). They appear as read-only files under ms0: by their stored
 * names ("ISO/Genre/x.iso"), with DratiniFS's rules:
 *   - read-only opens and getstat look in ES first (so the fast copy is used when the original is
 *     still in ms0:/ISO); writes, creates and truncates only ever see the filesystem;
 *   - remove tries the filesystem first ("delete original after transfer" removes the FS copy) and
 *     only then deactivates the ES entry;
 *   - a directory listing shows the filesystem's entries, then the ES files of that directory and the
 *     subdirectories that exist only in ES names (which can be opened and stat'ed, for GCLite);
 *   - ES entries carry the date 1992-03-27 (the ES app recognises them by it);
 *   - unknown ioctls on an ES file succeed;
 *   - devctls 0x04100001..7 serve the ES app (raw sectors, info, rescan, format, version).
 * ES data is read through Sony's own block handle (msstor0p1:) at the ES partition's offset from the
 * FS partition, with Sony's bounce rules; msstor_cap.c lets those seeks pass the adapter's size.
 * The table and the used part of the name pool are loaded at mount (fatfs_prep) into one Fpl block. */
#include "fatms.h"
#include "../shared/extremespeed_format.h"

#define ES_TAG     0x45535344u                   /* OpenFile.rsv[1] of an ES file ("DSSE") */
#define ES_DHMAGIC 0x45534448u                   /* ES state in a dir handle */
#define ES_DATE    ((u32)(((1992 - 1980) << 9) | (3 << 5) | 27) << 16)   /* 1992-03-27 00:00:00 (DOS) */

typedef struct { u32 start, size, flags, name_off, name_len; } es_ent;
/* per dir handle, behind the exFAT state in the same 0x800 block */
typedef struct { u32 magic, phase, idx, virt; u16 dir[0x104]; } es_dh;
#define ESDH(d) ((es_dh *)((u8 *)(d) + 0x5a0))
_Static_assert(0x5a0 + sizeof(es_dh) <= 0x800, "dir handle room");

static struct {
    int on;
    fatms_ctx *c;                                /* the Memory Stick volume it belongs to */
    u32 lba, rel;                                /* ES partition: absolute LBA, and from msstor0p1:'s start */
    u32 n, data_start, total;                    /* table entries (iso_count), data start, partition size */
    es_ent *e;
    char *pool;
    u32 pool_len;
    SceUID fpl;
    u8 bounce[16 * 512] __attribute__((aligned(64)));   /* raw devctl transfers to unaligned buffers */
} E;

static u32 le32(const u8 *p) { return (u32)(p[0] | p[1] << 8 | p[2] << 16 | (u32)p[3] << 24); }
static u32 le16(const u8 *p) { return (u32)(p[0] | p[1] << 8); }
static u16 up(u16 c) { return (c >= 'a' && c <= 'z') ? (u16)(c - 0x20) : c; }
static int on(const fatms_ctx *c) { return E.on && E.c == c; }   /* ES files show only on that volume */

/* ---------------- loading ---------------- */
void es_load(fatms_ctx *c)
{
    if (c->ops != &g_ms_ops) return;                                     /* Memory Stick only (not the Go's storage) */
    E.on = 0;
    E.c = c;
    if (E.fpl > 0) { sceKernelDeleteFpl(E.fpl); E.fpl = 0; }
    u8 t, t0; u32 lba = 0, len = 0, p0, l0;
    if ((c->flags & ST_WHOLEDISK) || mbr_entry(0, &t0, &p0, &l0) < 0) return;
    for (int i = 0; i < 4 && !lba; i++) if (mbr_entry(i, &t, &lba, &len) < 0 || t != ES_PARTITION_TYPE) lba = 0;
    if (!lba || lba <= p0) return;                                       /* msstor0p1: can't reach it */
    u8 *b = fatfs_get_scratch(c);
    if (!b) return;
    E.lba = lba; E.rel = lba - p0;
    u32 n = 0, pool = 0, need = 0;
    int r = c->blk->read_sectors(c, b, E.rel, 1);
    if (r < 0 || le32(b) != ES_MAGIC || le32(b + 4) != ES_VERSION) goto out;
    n = le32(b + 8); pool = le32(b + 28);
    if (n > ES_MAX_ISOS) n = ES_MAX_ISOS;
    if (pool > ES_NAME_POOL_BYTES) pool = ES_NAME_POOL_BYTES;
    E.data_start = le32(b + 12); E.total = le32(b + 16);
    need = n * sizeof(es_ent) + pool + 1;
    u8 *m = (u8 *)fatfs_alloc_block(&E.fpl, (int)((need + 63) & ~63u), "SceFatfsES");
    if (!m) goto out;
    E.e = (es_ent *)m; E.pool = (char *)(m + n * sizeof(es_ent)); E.pool_len = pool;
    for (u32 s = 0; s * 8 < n; s++) {                                   /* the table: 8 entries per sector */
        if ((r = c->blk->read_sectors(c, b, E.rel + ES_TABLE_FIRST_SECTOR + s, 1)) < 0) goto out;
        for (u32 k = 0; k < 8 && s * 8 + k < n; k++) {
            const u8 *x = b + k * ES_ENTRY_SIZE;
            es_ent *e = &E.e[s * 8 + k];
            e->start = le32(x + 16); e->size = le32(x + 24); e->flags = le32(x + 28);
            e->name_off = le32(x + 32); e->name_len = le16(x + 36);
            if (e->name_len == 0 || e->name_off + e->name_len > pool || e->name_len >= ES_NAME_MAX_LEN) e->flags = 0;
        }
    }
    for (u32 off = 0; off < pool; off += 512) {                        /* the used part of the name pool */
        if ((r = c->blk->read_sectors(c, b, E.rel + ES_NAME_POOL_START_SECTOR + off / 512, 1)) < 0) goto out;
        memcpy(E.pool + off, b, pool - off < 512 ? pool - off : 512);
    }
    E.pool[pool] = 0;
    E.n = n;
    E.on = 1;
out:
    fatfs_put_scratch(c, b);
    if (!E.on && E.fpl > 0) { sceKernelDeleteFpl(E.fpl); E.fpl = 0; }
}

/* ---------------- names ---------------- */
/* entry i's stored name ("ISO/x.iso", UTF-8) as a Sony path ("\ISO\x.iso") -> w (0x104 chars) */
static void es_name(u32 i, u16 *w)
{
    char tmp[ES_NAME_MAX_LEN + 1];
    u32 n = E.e[i].name_len;
    memcpy(tmp, E.pool + E.e[i].name_off, n);
    tmp[n] = 0;
    w[0] = '\\';
    sceCodepage_driver_C899572E(w + 1, 0x206, tmp);
    for (u16 *s = w; *s; s++) if (*s == '/') *s = '\\';
}
/* case-insensitive compare of n chars (ASCII folding, as Sony's names) */
static int same(const u16 *a, const u16 *b, int n) { for (int i = 0; i < n; i++) if (up(a[i]) != up(b[i])) return 0; return 1; }
static int es_find(fatms_ctx *c, const fpath *p)                         /* the active entry at path p */
{
    u16 *w = (u16 *)fatfs_get_scratch(c);
    if (!w) return -1;
    int dl = fp_wcslen(p->dir), ll = fp_wcslen(p->leaf), hit = -1;
    for (u32 i = 0; i < E.n && hit < 0; i++) {
        if (!(E.e[i].flags & ES_FLAG_ACTIVE)) continue;
        es_name(i, w);
        if (fp_wcslen(w) == dl + ll && same(w, p->dir, dl) && same(w + dl, p->leaf, ll)) hit = (int)i;
    }
    fatfs_put_scratch(c, w);
    return hit;
}
/* does some active entry live under directory `dir` ("\ISO\Genre\")? */
static int es_under(fatms_ctx *c, const u16 *dir)
{
    u16 *w = (u16 *)fatfs_get_scratch(c);
    if (!w) return 0;
    int dl = fp_wcslen(dir), hit = 0;
    for (u32 i = 0; i < E.n && !hit; i++) {
        if (!(E.e[i].flags & ES_FLAG_ACTIVE)) continue;
        es_name(i, w);
        hit = fp_wcslen(w) > dl && same(w, dir, dl);
    }
    fatfs_put_scratch(c, w);
    return hit;
}
static void es_entry32(u8 *ent, u32 attr, u32 size)                    /* Sony's 32-byte entry for an ES item */
{
    memset(ent, ' ', 11); memset(ent + 11, 0, 21);
    ent[0x0b] = (u8)attr;
    u32 d = ES_DATE;
    memcpy(ent + 0x0e, &d, 4); memcpy(ent + 0x16, &d, 4);
    ent[0x12] = (u8)(d >> 16); ent[0x13] = (u8)(d >> 24);
    memcpy(ent + 0x1c, &size, 4);
}

/* ---------------- files ---------------- */
/* read-only open: ES first. FAT_ENOENT = not an ES file (the caller goes on to the filesystem);
   other errors are prep's, the same the filesystem open would return. */
int es_open(fatms_ctx *c, fpath *p, unsigned flags, OpenFile **out)
{
    int r = fatfs_prep(c);
    if (r >= 0) r = fatfs_ready(c);
    if (r < 0) return r;
    if (!on(c) || (flags & 0x602)) return FAT_ENOENT;
    int i = es_find(c, p), idx = 0;
    if (i < 0) return FAT_ENOENT;
    if ((r = flagcheck(flags, 1)) < 0) return r;
    u32 st = slot_scan(c, p, &idx);
    if (st == 0 || st == 2) return FAT_EMFILE;
    OpenFile *f = (OpenFile *)c->fd[idx];
    memset(f, 0, sizeof(*f));
    memcpy(&f->path, p, sizeof(fpath));
    es_entry32(f->entry, 0x20, E.e[i].size);
    f->rsv[1] = ES_TAG; f->rsv[2] = (u32)i;
    f->inuse++;
    if (flags & 0x100) f->position = F_SIZE(f);
    f->oflags = flags;
    *out = f;
    return 0;
}
int es_is_file(OpenFile *f) { return f && f->rsv[1] == ES_TAG; }

/* fatfs_read for an ES file: contiguous sectors at the partition's offset, partial ones bounced */
int es_read(fatms_ctx *c, OpenFile *f, void *buf, int len)
{
    if (!(f->oflags & 1)) return FAT_EACCES;
    u32 pos = f->position, size = F_SIZE(f), n = (u32)len, i = f->rsv[2], done = 0;
    if (pos >= size || i >= E.n) return 0;                               /* past the end: nothing */
    if (size - pos < n) n = size - pos;
    u32 base = E.rel + E.e[i].start;
    u8 *p = (u8 *)buf;
    while (done < n) {
        u32 lba = base + pos / 512, off = pos % 512, got;
        int r;
        if (off == 0 && n - done >= 512) {
            got = (n - done) & ~511u;
            r = c->blk->read_sectors(c, p, lba, got / 512);
        } else {
            u8 *sb = fatfs_get_scratch(c);
            if (!sb) return FAT_ENOMEM;
            got = 512 - off < n - done ? 512 - off : n - done;
            r = c->blk->read_sectors(c, sb, lba, 1);
            if (r >= 0) memcpy(p, sb + off, got);
            fatfs_put_scratch(c, sb);
        }
        if (r < 0) { if (done) break; return r; }
        p += got; pos += got; done += got;
    }
    f->position = pos;
    return (int)done;
}

int es_getstat(fatms_ctx *c, fpath *p, SceIoStat *st)                  /* ES first (DratiniFS) */
{
    int r = fatfs_prep(c);
    if (r >= 0) r = fatfs_ready(c);
    if (r < 0) return r;
    int i = on(c) ? es_find(c, p) : -1;
    if (i < 0) return FAT_ENOENT;
    u8 ent[32];
    es_entry32(ent, 0x20, E.e[i].size);
    dirent_stat(ent, st);
    return 0;
}
/* a directory that exists only in ES names ("ISO/Genre/..."), after the filesystem said ENOENT */
int es_vdir(fatms_ctx *c, fpath *p, SceIoStat *st)
{
    if (!on(c)) return FAT_ENOENT;
    u16 *w = (u16 *)fatfs_get_scratch(c);
    if (!w) return FAT_ENOMEM;
    fp_wcscpy(w, p->dir);
    int n = fp_wcslen(w), ll = fp_wcslen(p->leaf);
    if (n + ll + 1 >= 0x104) { fatfs_put_scratch(c, w); return FAT_ENOENT; }
    fp_wcscpy(w + n, p->leaf); w[n + ll] = '\\'; w[n + ll + 1] = 0;
    int hit = es_under(c, w);
    fatfs_put_scratch(c, w);
    if (!hit) return FAT_ENOENT;
    if (st) { u8 ent[32]; es_entry32(ent, 0x10, 0); dirent_stat(ent, st); }
    return 0;
}

int es_remove(fatms_ctx *c, fpath *p)                                  /* after the filesystem said ENOENT */
{
    int idx, i = on(c) ? es_find(c, p) : -1;
    if (i < 0) return FAT_ENOENT;
    if (c->media_flags & MF_WP) return FAT_EACCES;
    if (slot_scan(c, p, &idx) & 2) return FAT_EACCES;                    /* open */
    u8 *b = fatfs_get_scratch(c);
    if (!b) return FAT_ENOMEM;
    u32 lba = E.rel + ES_TABLE_FIRST_SECTOR + (u32)i / 8;
    int r = c->blk->read_sectors(c, b, lba, 1);
    if (r >= 0) {
        u8 *x = b + (i % 8) * ES_ENTRY_SIZE;
        u32 fl = le32(x + 28) & ~(u32)ES_FLAG_ACTIVE;
        memcpy(x + 28, &fl, 4);
        if ((r = c->blk->write_sectors(c, b, lba, 1)) >= 0) E.e[i].flags = fl;
    }
    fatfs_put_scratch(c, b);
    return r < 0 ? r : 0;
}

/* ---------------- directories ---------------- */
/* after dopen (real or virtual): remember the directory for the listing overlay */
void es_dir_init(fatms_ctx *c, DirHandle *d, const fpath *p, int virt)
{
    es_dh *h = ESDH(d);
    h->magic = on(c) ? ES_DHMAGIC : 0;
    h->phase = h->idx = 0;
    h->virt = (u32)virt;
    fp_wcsncpy(h->dir, p->dir, 0x104);
}
int es_dir_virtual(DirHandle *d) { return ESDH(d)->magic == ES_DHMAGIC && ESDH(d)->virt; }

/* a dir handle for a directory that exists only in ES names (the filesystem listing is empty) */
int es_dopen(fatms_ctx *c, fpath *p, DirHandle **out)
{
    fpath q;
    *out = 0;
    int n = fp_wcslen(p->dir);
    if (n < 2) return FAT_ENOENT;                                        /* "\" is never virtual */
    fp_wcsncpy(q.dir, p->dir, 0x104);
    q.dir[n - 1] = 0;                                                     /* "\ISO\Genre" -> parent + leaf */
    int k = n - 2;
    while (k >= 0 && q.dir[k] != '\\') k--;
    fp_wcsncpy(q.leaf, q.dir + k + 1, 0x100);
    q.dir[k + 1] = 0;
    if (es_vdir(c, &q, 0) < 0) return FAT_ENOENT;
    DirHandle *d = (DirHandle *)fatfs_get_scratch(c);
    if (!d) return FAT_ENOMEM;
    memset(d, 0, sizeof(*d));
    d->is_dir = 1; d->ctx = c;
    d->path.dir[0] = '\\'; d->path.leaf[0] = '*';
    es_dir_init(c, d, p, 1);
    *out = d;
    return 0;
}

/* the overlay: next ES file in the directory, then each subdirectory only ES names have (once,
   and not when the filesystem has it). 0 = an entry, FAT_ENOENT = the end. */
int es_dread(fatms_ctx *c, DirHandle *d, SceIoDirent *de, void *priv)
{
    es_dh *h = ESDH(d);
    if (h->magic != ES_DHMAGIC || !on(c)) return FAT_ENOENT;
    u16 *w = (u16 *)fatfs_get_scratch(c), *v = (u16 *)fatfs_get_scratch(c);
    if (!w || !v) { if (w) fatfs_put_scratch(c, w); if (v) fatfs_put_scratch(c, v); return FAT_ENOMEM; }
    int dl = fp_wcslen(h->dir), r = FAT_ENOENT;
    for (; h->phase < 2; h->phase++, h->idx = 0) {
        for (; h->idx < E.n; h->idx++) {
            u32 i = h->idx;
            if (!(E.e[i].flags & ES_FLAG_ACTIVE)) continue;
            es_name(i, w);
            if (fp_wcslen(w) <= dl || !same(w, h->dir, dl)) continue;
            u16 *rest = w + dl, *sl = rest;
            while (*sl && *sl != '\\') sl++;
            if (h->phase == 0 && *sl) continue;                          /* files directly in the directory */
            if (h->phase == 1) {                                         /* subdirectories only ES has */
                if (!*sl) continue;
                *sl = 0;
                int sn = (int)(sl - rest), dup = 0;
                for (u32 k = 0; k < i && !dup; k++) {                    /* emitted for an earlier entry? */
                    if (!(E.e[k].flags & ES_FLAG_ACTIVE)) continue;
                    es_name(k, v);
                    dup = fp_wcslen(v) > dl + sn && same(v, h->dir, dl) && same(v + dl, rest, sn) && v[dl + sn] == '\\';
                }
                if (dup) continue;
                fpath *q = (fpath *)v;                                   /* v is free again: reuse it */
                fp_wcsncpy(q->dir, h->dir, 0x104);
                fp_wcsncpy(q->leaf, rest, 0x100);
                SceIoStat st;
                if (fatfs_getstat(c, q, &st) == 0) continue;             /* the filesystem has it */
            }
            if (fp_wildmatch(d->path.leaf, rest, 0) != 0) continue;
            u8 ent[32];
            es_entry32(ent, h->phase ? 0x10 : 0x20, h->phase ? 0 : E.e[i].size);
            r = dread_emit(c, de, priv, ent, rest, "");
            if (r < 0) goto out;
            h->idx++;
            r = 0;
            goto out;
        }
    }
out:
    fatfs_put_scratch(c, v);
    fatfs_put_scratch(c, w);
    return r;
}

/* ---------------- devctls for the ES app ---------------- */
static int es_raw(fatms_ctx *c, int write, void *in, int inlen)
{
    if (!in || inlen < 12) return FAT_EINVAL;
    u32 sector = ((u32 *)in)[0], count = ((u32 *)in)[1];
    u8 *buf = ((u8 **)in)[2];
    if (!buf || count == 0 || count > 0x10000) return FAT_EINVAL;
    if ((int)(fat_k1() & ((u32)buf | ((u32)buf + count * 512 - 1))) < 0) return FAT_EINVAL;   /* user caller, kernel buffer */
    SceUID fd = k_sceIoOpen(c->name_msstor0, write ? 0x4000003 : 0x4000001, 0);
    if (fd < 0) return fd;
    SceOff s = k_sceIoLseek64k(fd, (SceOff)sector * 512, 0);             /* 64-bit: never cast the offset */
    int r = s < 0 ? (int)s : 0;
    for (u32 done = 0; r >= 0 && done < count; ) {
        u32 k = count - done, aligned = !((u32)(buf + done * 512) & 63);
        if (!aligned && k > 16) k = 16;
        u8 *p = aligned ? buf + done * 512 : E.bounce;
        if (write && !aligned) memcpy(p, buf + done * 512, k * 512);
        if (write) sceKernelDcacheWritebackInvalidateRange(p, k * 512);
        r = write ? k_sceIoWrite(fd, p, (int)(k * 512)) : k_sceIoRead(fd, p, (int)(k * 512));
        if (r >= 0 && !write) {
            sceKernelDcacheWritebackInvalidateRange(p, k * 512);
            if (!aligned) memcpy(buf + done * 512, p, k * 512);
        }
        done += k;
    }
    k_sceIoClose(fd);
    return r < 0 ? r : 0;
}

int es_devctl(fatms_ctx *c, unsigned int cmd, void *in, int inlen, void *out, int outlen)
{
    int r = fatfs_prep(c);
    if (r >= 0) r = fatfs_ready(c);
    switch (cmd) {
    case ES_DEVCTL_READ_SECTOR:  return es_raw(c, 0, in, inlen);
    case ES_DEVCTL_WRITE_SECTOR: return es_raw(c, 1, in, inlen);
    case ES_DEVCTL_GET_INFO: {
        if (!out || outlen < (int)sizeof(ExtremeSpeedInfo)) return FAT_EINVAL;
        ExtremeSpeedInfo *o = (ExtremeSpeedInfo *)out;
        u32 next = E.data_start;
        for (u32 i = 0; E.on && i < E.n; i++)
            if ((E.e[i].flags & ES_FLAG_ACTIVE) && E.e[i].start + (E.e[i].size + 511) / 512 > next)
                next = E.e[i].start + (E.e[i].size + 511) / 512;
        o->initialized = (u32)E.on;
        o->partition_start_sector = E.on ? E.lba : 0;
        o->total_sectors = E.on ? E.total : 0;
        o->iso_count = E.on ? E.n : 0;
        o->data_start_sector = E.on ? E.data_start : 0;
        o->free_sector = E.on ? next : 0;
        return 0;
    }
    case ES_DEVCTL_RESCAN:                                               /* the app changed the MBR / ES / VBR */
        if (r < 0) return r;
        if ((r = fatfs_flush(c)) < 0) return r;
        msstor_cap_insert(c);                                            /* MBR again: size, ES partition */
        c->media_flags |= MF_INSERTED;                                   /* re-identify: FS caches dropped */
        if ((r = fatfs_prep(c)) < 0) return r;                           /* ... and ES reloaded, now */
        return 0;
    case ES_DEVCTL_SYNC_FD:      return 0;
    case ES_DEVCTL_GET_VERSION:
        if (!out || outlen < 4) return FAT_EINVAL;
        *(u32 *)out = DRATINIFS_VERSION;
        return 0;
    case ES_DEVCTL_FORMAT:                                               /* 0 FAT32, 1 exFAT (format.c) */
        if (!in || inlen < 4) return FAT_EINVAL;
        r = dfs_format(c, *(int *)in == 1);
        return r > 0 ? 0 : r;
    }
    return FAT_EUNSUP;
}
