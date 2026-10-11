/* fatfs_dirent.c - Sony 6.61 fatms directory layer.
 *   iterator        FUN_00000a0c (init) / aa8+ae0+c84 (move)
 *   DirentCache     FUN_00000000 (get) / 0094 (put) / e68+10e8 (hit/load) / 1220 (flush) /
 *                   1278 (write slot) / 1314 (LRU) / 1354 (touch) / f18+12f4 (forget)
 *   scanner         FUN_00001af8 (+1a8c lookup, 20f4 walk)
 *   create/delete   FUN_000017a4 (free run + EEXIST), 0124 (write LFN+8.3), 0288 (delete LFN+8.3),
 *                   13d4 (grow dir), 0428 (zero cluster), a2cc (~N alias), 8664 (new file entry),
 *                   8d28 (is empty)
 *   stat/time       FUN_00000538 + 5a84, aac0/aa38/a988/a9b0 (sceRtc_driver)
 *   root helpers    FUN_00001f00 (volume label / free slot), 6210/6280/63a0 (free root entries)
 * One deliberate deviation: the FAT12/16 root region is walked contiguously. Sony's ae0 wraps the
 * root sector index at sectors-per-cluster and then fails (only reachable when the root is larger
 * than one cluster, which Sony-formatted MS never are). */
#include "fatms.h"

#define D_END   (int)0x8001000d       /* iterator moved past the directory */

static int fat32(fatms_ctx *c) { return c->fat_type == 32; }
static u32 le16(const u8 *p) { return (u32)(p[0] | p[1] << 8); }
static void put16(u8 *p, u32 v) { p[0] = (u8)v; p[1] = (u8)(v >> 8); }

/* ---------------- iterator ---------------- */
int dir_iter_init(fatms_ctx *c, diter *it, u32 cluster)                 /* FUN_00000a0c */
{
    it->start = cluster; it->cur = cluster;
    it->cidx = it->sidx = it->eidx = 0;
    if (cluster == 0 && !fat32(c)) { it->lba = c->root_dir_lba; return 0; }
    if (cluster == 0) it->cur = cluster = c->root_cluster;
    u32 lba = fatfs_clus2lba(c, cluster);
    if (lba > c->total_sectors) return D_END;
    it->lba = lba;
    return 0;
}

int dir_iter_move(fatms_ctx *c, diter *it, int n)                       /* FUN_00000aa8 */
{
    if (n == 0) return 0;
    if (it->start == 0 && !fat32(c)) {                                   /* FAT12/16 root region */
        int abs = (int)(it->sidx * 16 + it->eidx) + n;
        u32 nsec = c->data_start_lba - c->root_dir_lba;
        if (abs < 0 || (u32)abs >= nsec * 16) return D_END;
        it->sidx = (u32)abs / 16; it->eidx = (u32)abs % 16; it->lba = c->root_dir_lba + it->sidx;
        return 0;
    }
    u32 per = (u32)c->sectors_per_cluster * 16;
    int abs = (int)((it->cidx * c->sectors_per_cluster + it->sidx) * 16 + it->eidx) + n;
    if (abs < 0) return D_END;
    u32 cidx = (u32)abs / per, cl = it->cur;
    if (cidx != it->cidx) {                                              /* forward from cur, back from start */
        int r = (cidx > it->cidx)
              ? fatfs_walk(c, it->cur, cidx - it->cidx, &cl)
              : fatfs_walk(c, it->start ? it->start : c->root_cluster, cidx, &cl);
        if (r < 0) return r;
        if (cl >= c->fat_max_cluster) return D_END;
    }
    u32 rem = (u32)abs % per;
    u32 lba = fatfs_clus2lba(c, cl) + rem / 16;
    if (lba > c->total_sectors) return D_END;
    it->cur = cl; it->cidx = cidx; it->sidx = rem / 16; it->eidx = rem % 16; it->lba = lba;
    return 0;
}

/* ---------------- DirentCache (4 slots x 4 sectors, one window per directory) ---------------- */
static int dc_find(fatms_ctx *c, u32 tag)                               /* FUN_00000f18 */
{
    for (int i = 0; i < FAT_NUM_DIRENT; i++) if (c->dc[i].tag == tag) return i;
    return -1;
}
static void dc_touch(fatms_ctx *c, int i)                               /* FUN_00001354 */
{
    c->dc[i].stamp = c->dc_clock++;
    if (c->dc_clock != 0xffffffffu) return;
    c->dc_clock = 1;
    for (int k = 0; k < FAT_NUM_DIRENT; k++) c->dc[k].stamp = 0;
    c->dc[i].stamp = c->dc_clock++;
}
static int dc_write(fatms_ctx *c, int i)                                /* FUN_00001278 */
{
    dcache_slot *s = &c->dc[i];
    if (!s->dirty) return 0;
    int r = c->blk->write_sectors(c, s->buf, s->lba, s->nsec);
    if (r < 0) return r;
    s->dirty = 0;
    return 0;
}
int dirent_flush(fatms_ctx *c)                                          /* FUN_00001220 */
{
    for (int i = 0; i < FAT_NUM_DIRENT; i++) { int r = dc_write(c, i); if (r < 0) return r; }
    return 0;
}
void dirent_reset(fatms_ctx *c)                                         /* 573c: forget everything */
{
    for (int i = 0; i < FAT_NUM_DIRENT; i++) { c->dc[i].tag = 0xffffffffu; c->dc[i].dirty = 0; c->dc[i].stamp = 0; }
    c->dc_clock = 0;
    c->mode_c4 = 0;
}
void dirent_forget(fatms_ctx *c, u32 cluster)                           /* f18 + FUN_000012f4 */
{
    int i = dc_find(c, cluster);
    if (i >= 0) { c->dc[i].lba = 0; c->dc[i].dirty = 0; }
}
void dirent_drop_all(fatms_ctx *c)                                      /* fatmsef 0x12f4: 12f4 on every slot */
{
    for (int i = 0; i < FAT_NUM_DIRENT; i++) { c->dc[i].lba = 0; c->dc[i].dirty = 0; }
}
static int dc_hit(fatms_ctx *c, diter *it, int *slot, u32 *idx)         /* FUN_00000e68 */
{
    int i = dc_find(c, it->start);
    if (i < 0) return 0;
    dcache_slot *s = &c->dc[i];
    if (it->lba < s->lba || it->lba >= s->lba + s->nsec) return 0;
    *slot = i; *idx = (it->lba - s->lba) * 16 + it->eidx;
    return 1;
}
static int dc_load(fatms_ctx *c, diter *it, int *slot, u32 *idx)        /* FUN_000010e8 */
{
    int i = dc_find(c, it->start);
    if (i < 0) {                                                         /* FUN_00001314: LRU victim */
        i = 0;
        for (int k = 1; k < FAT_NUM_DIRENT; k++) if (c->dc[k].stamp < c->dc[i].stamp) i = k;
    }
    int r = dc_write(c, i);
    if (r < 0) return r;
    dcache_slot *s = &c->dc[i];
    u32 n = c->sectors_per_cluster < 4 ? c->sectors_per_cluster : 4;
    u32 start = it->lba - (it->sidx & 3);
    if (it->start == 0 && !fat32(c)) {                                   /* contiguous FAT12/16 root: */
        u32 left = c->data_start_lba - start;                            /* 4-sector window, clamped to it */
        n = left < 4 ? left : 4;
    }
    r = c->blk->read_sectors(c, s->buf, start, n);
    if (r < 0) return r;
    s->tag = it->start; s->cur = it->cur; s->lba = start; s->cidx = it->cidx;
    s->sbase = it->sidx - (it->sidx & 3); s->nsec = n;
    *slot = i; *idx = (it->lba - start) * 16 + it->eidx;
    return 0;
}
int dirent_get(fatms_ctx *c, u8 **ent, diter *it)                       /* FUN_00000000 */
{
    int i; u32 idx;
    *ent = 0;
    if (!dc_hit(c, it, &i, &idx)) { int r = dc_load(c, it, &i, &idx); if (r < 0) return r; }
    if (idx >= c->dc[i].nsec * 16) return FAT_ENOENT;                   /* FUN_00000f78 */
    dc_touch(c, i);
    *ent = c->dc[i].buf + idx * 32;
    return 0;
}
int dirent_put(fatms_ctx *c, const u8 *ent, diter *it)                  /* FUN_00000094 */
{
    int i; u32 idx;
    if (!dc_hit(c, it, &i, &idx)) { int r = dc_load(c, it, &i, &idx); if (r < 0) return r; }
    if (idx >= c->dc[i].nsec * 16) return FAT_ENOENT;
    dc_touch(c, i);
    memcpy(c->dc[i].buf + idx * 32, ent, 32);                            /* FUN_00001014 */
    if (c->mode_c4 == 1) return dc_write(c, i);                          /* write-through mode */
    c->dc[i].dirty = 1;
    return 0;
}

/* ---------------- scanner ---------------- */
static void scan_result(fatms_ctx *c, OpenFile *out, const u8 *e, diter *it, const u16 *name)
{
    memcpy(out->entry, e, 32);
    out->first_cluster = le16(e + 0x1a) | (fat32(c) ? le16(e + 0x14) << 16 : 0);
    out->ctx = c;
    out->dirpos = *it;
    out->entry[0x0c] = 0; out->entry[0x0d] = 0;                          /* Sony clears these two bytes */
    out->inuse = 0;
    sfn_to_str(out->path.sfn, out->entry);
    fp_wcsncpy(out->path.leaf, name, 0x100);
    out->path.dir[0] = 0;
}

/* FUN_00001af8: from `it` onward, find the first entry whose LFN or 8.3 name matches `name`
   (exact, case-insensitive; or wildcard when `wild`). LFN checksums are not verified (Sony).
   Exact lookups first scan the directory's window already in the DirentCache (wherever the last
   access left it) and only fall back to a scan from `it` when the name isn't there. */
int dir_scan(fatms_ctx *c, OpenFile *out, diter *it, const u16 *name, int wild)
{
    u16 *buf = (u16 *)fatfs_get_scratch(c);                             /* LFN assembly (0x100 chars) */
    if (!buf) return FAT_ENOMEM;
    u16 sname[16]; char sstr[16];
    int st = 0, ord = 0, r, slot = wild ? -1 : dc_find(c, it->start);
    u32 k = 0;
    diter base = { 0, 0, 0, 0, 0, 0 }, at;
    if (slot >= 0) {                                                     /* FUN_00000f18: the window's position */
        dcache_slot *s = &c->dc[slot];
        base.start = s->tag; base.cur = s->cur; base.lba = s->lba; base.cidx = s->cidx; base.sidx = s->sbase;
    }
    buf[0] = 0;
    for (;;) {
        u8 *e;
        if (slot >= 0) {                                                 /* FUN_00000f78 on the cached window */
            dcache_slot *s = &c->dc[slot];
            if (s->tag != it->start || k >= s->nsec * 16) {             /* window exhausted: full scan */
                slot = -1; st = 0; ord = 0; buf[0] = 0;
                continue;
            }
            dc_touch(c, slot);
            e = s->buf + k * 32;
            at = base; at.eidx = k & 0xf; at.sidx += k >> 4; at.lba += k >> 4;
        } else {
            r = dirent_get(c, &e, it);
            if (r < 0) break;
            at = *it;
        }
        if (e[0] == 0) {
            if (slot < 0) { r = FAT_ENOENT; break; }
            slot = -1; st = 0; ord = 0; buf[0] = 0;                               /* end inside the window: full scan */
            continue;
        }
        if (e[0] == 0xe5) { st = 0; buf[0] = 0; }
        else if (st == 2) {                                              /* entry after a matching LFN run */
            *it = at;
            if (out) scan_result(c, out, e, it, buf);
            r = 0; break;
        } else if (e[0x0b] == 0x08) { st = 0; buf[0] = 0; }             /* volume label */
        else if (e[0x0b] == 0x0f) {
            if (e[0] & 0x40) { ord = e[0] & ~0x40; buf[0] = 0; st = 1; }
            if (st != 1) st = 0;
            else {
                lfn_prepend(e, buf);
                if ((e[0] & 0xbf) == 1 && ord == 1) {
                    int m = wild ? fp_wildmatch(name, buf, 0) : fp_namecmp(name, buf);
                    st = (m == 0 && buf[0] != 0) ? 2 : 0;
                }
                ord--;
            }
        } else {
            sfn_to_str(sstr, e);
            r = sceCodepage_driver_855C5C2E(sname, 0x1a, sstr);
            if (r < 0) break;
            if ((wild ? fp_wildmatch(name, sname, 1) : fp_namecmp(name, sname)) == 0) {
                *it = at;
                if (out) scan_result(c, out, e, it, sname);
                r = 0; break;
            }
            st = 0; buf[0] = 0;
        }
        if (slot >= 0) { k++; continue; }
        r = dir_iter_move(c, it, 1);
        if (r < 0) { if (r == D_END) r = FAT_ENOENT; break; }
    }
    fatfs_put_scratch(c, buf);
    return r;
}

int dir_lookup(fatms_ctx *c, OpenFile *out, u32 cluster, const u16 *name)   /* FUN_00001a8c */
{
    diter it;
    int r = dir_iter_init(c, &it, cluster);
    if (r >= 0) { r = dir_scan(c, out, &it, name, 0); if (r > 0) r = 0; }
    return r;
}

/* FUN_000020f4: walk a parent path ("\a\b\"); every component must be a directory */
int dir_walk(fatms_ctx *c, const u16 *dir, u32 *cluster)
{
    OpenFile *tmp = (OpenFile *)fatfs_get_scratch(c);
    if (!tmp) return FAT_ENOMEM;
    u16 *comp = (u16 *)fatfs_get_scratch(c);
    if (!comp) { fatfs_put_scratch(c, tmp); return FAT_ENOMEM; }
    u32 cl = 0;
    int i = 0, r;
    do {
        if (dir[i] == '\\') i++;
        if (dir[i] == 0) { *cluster = cl; r = 0; break; }
        memset(comp, 0, 0x208);
        int n = 0;
        do comp[n++] = dir[i++]; while (dir[i] != 0 && dir[i] != '\\' && n < 0x103);
        r = dir_lookup(c, tmp, cl, comp);
        if (r < 0) break;
        cl = tmp->first_cluster;
        r = FAT_ENOENT;                                                  /* a file in the middle of the path */
    } while (tmp->entry[0x0b] & 0x10);
    fatfs_put_scratch(c, comp);
    fatfs_put_scratch(c, tmp);
    return r;
}

/* ---------------- growing / creating ---------------- */
int dir_zero_cluster(fatms_ctx *c, u32 cl)                              /* FUN_00000428 */
{
    SceUID fpl;
    u8 *z = (u8 *)fatfs_alloc_block(&fpl, 0x1000, "SceFatfsDirentClear");
    if (!z) return FAT_ENOMEM;
    memset(z, 0, 0x1000);
    u32 lba = fatfs_clus2lba(c, cl), spc = c->sectors_per_cluster, done = 0;
    int r = 0;
    while (done < spc) {                                                 /* 8 sectors per write */
        u32 n = spc - done < 8 ? spc - done : 8;
        r = c->blk->write_sectors(c, z, lba + done, n);
        if (r < 0) break;
        done += n;
    }
    sceKernelDeleteFpl(fpl);
    return r < 0 ? r : 0;
}

static int dir_extend(fatms_ctx *c, u32 cl)                             /* FUN_000013d4 */
{
    if (fat32(c) && cl == 0) cl = c->root_cluster;
    u32 eoc = fat32(c) ? 0x0ffffff8u : c->fat_type == 16 ? 0xfff8u : 0xff8u, last, nx, got, n;
    do {
        last = cl;
        int r = fatfs_walk(c, last, 1, &nx);
        if (r < 0) return r;
        cl = nx;
    } while (nx < eoc);
    int r = fat_extend(&c->fe, last, 1, &got, &n);
    if (r < 0) return r;
    r = dir_zero_cluster(c, got);
    if (r >= 0 && (r = fat_flush(&c->fe)) >= 0) return 0;
    fat_set(&c->fe, last, 0x0fffffffu);                                  /* undo: re-terminate, free the new one */
    fat_set(&c->fe, got, 0);
    fat_flush(&c->fe);
    return r;
}

/* FUN_000017a4: find `need` consecutive free entries in `cluster` (growing it when allowed) while
   checking that `name` does not exist; out->dirpos = the last entry of the run (the 8.3 slot). */
int dir_find_free(fatms_ctx *c, OpenFile *out, u32 cluster, const u16 *name, int need_lfn)
{
    u16 *buf = (u16 *)fatfs_get_scratch(c);
    if (!buf) return FAT_ENOMEM;
    u16 sname[16]; char sstr[16];
    int need = need_lfn ? lfn_entries(name) : 1, run = 0, st = 0, ord = 0, r;
    diter it;
    buf[0] = 0;
    r = dir_iter_init(c, &it, cluster);
    while (r >= 0) {
        u8 *e;
        r = dirent_get(c, &e, &it);
        if (r < 0) break;
        if (e[0] == 0xe5 || e[0] == 0) {
            st = 0;
            if (++run == need) {
                out->ctx = c; out->dirpos = it;
                out->entry[0x0c] = 0; out->entry[0x0d] = 0; out->inuse = 0;
                r = 0; break;
            }
            buf[0] = 0;
        } else {
            r = FAT_EEXIST;
            if (st == 2) break;                                          /* the LFN run before this matched */
            run = 0;
            if (e[0x0b] == 0x0f) {
                if (e[0] & 0x40) { ord = e[0] & ~0x40; buf[0] = 0; st = 1; }
                if (st == 1) {
                    lfn_prepend(e, buf);
                    if ((e[0] & 0xbf) == 1 && ord == 1) st = (fp_namecmp(name, buf) == 0 && buf[0] != 0) ? 2 : 0;
                    ord--;
                } else st = 0;
            } else {
                sfn_to_str(sstr, e);
                r = sceCodepage_driver_855C5C2E(sname, 0x1a, sstr);
                if (r < 0) break;
                r = FAT_EEXIST;
                if (fp_namecmp(name, sname) == 0) break;
                st = 0; buf[0] = 0;
            }
        }
        r = dir_iter_move(c, &it, 1);
        if (r < 0) {
            if ((it.start == 0 && !fat32(c)) || r != D_END) break;       /* FAT12/16 root cannot grow */
            r = dir_extend(c, it.cur);
            if (r >= 0) r = dir_iter_move(c, &it, 1);
        }
    }
    fatfs_put_scratch(c, buf);
    return r;
}

/* FUN_0000a2cc: pick the first free ~N 8.3 alias for p->leaf in `cluster` -> p->sfn */
int dir_make_alias(fatms_ctx *c, fpath *p, u32 cluster)
{
    char *oem = (char *)fatfs_get_scratch(c);
    if (!oem) return FAT_ENOMEM;
    fp_alias a; u8 sfn[12]; char s[16]; u16 w[16];
    int r = alias_init(&a, sfn, p->leaf, oem);
    fatfs_put_scratch(c, oem);
    while (r >= 0) {
        sfn_to_str(s, sfn);
        r = sceCodepage_driver_855C5C2E(w, 0x1a, s);
        if (r < 0) break;
        r = dir_lookup(c, 0, cluster, w);
        if (r < 0) {
            if (r != FAT_ENOENT) break;
            strncpy(p->sfn, s, 0xc);
            return 0;
        }
        r = alias_next(&a, sfn);
    }
    return r;
}

int dir_write_entries(fatms_ctx *c, OpenFile *f)                        /* FUN_00000124 */
{
    int r = dirent_put(c, f->entry, &f->dirpos);
    if (r < 0 || !f->path.need_lfn) return r < 0 ? r : 0;
    int n = lfn_entries(f->path.leaf) - 1;
    u8 chk = lfn_checksum(f->entry), le[32];
    const u16 *nm = f->path.leaf;
    for (int ord = 1; n > 0; ord++, n--) {
        nm += lfn_fragment(nm, le) / 2;
        le[0] = (u8)(ord | (n == 1 ? 0x40 : 0));
        le[0x0b] = 0x0f; le[0x0d] = chk;
        diter it = f->dirpos;
        r = dir_iter_move(c, &it, -ord);
        if (r < 0) return r;
        r = dirent_put(c, le, &it);
        if (r < 0) return r;
    }
    return 0;
}

int dir_delete_entries(fatms_ctx *c, OpenFile *f)                       /* FUN_00000288 */
{
    f->entry[0] = 0xe5;
    int r = dirent_put(c, f->entry, &f->dirpos);
    if (r < 0) return r;
    int n = lfn_entries(f->path.leaf) - 1;
    for (int ord = 1; n > 0; ord++, n--) {
        diter it = f->dirpos;
        u8 *e, tmp[32];
        r = dir_iter_move(c, &it, -ord);
        if (r < 0) break;
        r = dirent_get(c, &e, &it);
        if (r < 0) return r;
        if (e[0x0b] != 0x0f || (e[0] & 0xbf) != ord) return 0;          /* not part of this name's run */
        memcpy(tmp, e, 32); tmp[0] = 0xe5;
        r = dirent_put(c, tmp, &it);
        if (r < 0) return r;
    }
    return (r == D_END || r >= 0) ? 0 : r;
}

int dir_is_empty(fatms_ctx *c, u32 cluster)                             /* FUN_00008d28 */
{
    diter it;
    int r = dir_iter_init(c, &it, cluster);
    if (r >= 0) r = dir_iter_move(c, &it, 2);                            /* skip "." and ".." */
    while (r >= 0) {
        u8 *e;
        r = dirent_get(c, &e, &it);
        if (r < 0) break;
        if (e[0] == 0) return 0;
        if (e[0] != 0xe5 && e[0x0b] != 0x0f) return FAT_EACCES;          /* not empty */
        r = dir_iter_move(c, &it, 1);
    }
    return r == D_END ? 0 : r;
}

void dir_new_entry(fatms_ctx *c, OpenFile *f)                           /* FUN_00008664 (minus the write) */
{
    (void)c;
    f->first_cluster = 0;
    put16(f->entry + 0x1a, 0); put16(f->entry + 0x14, 0);
    dirent_stamp(f->entry, 1);
    f->entry[0x0b] = 0x20;
    F_SIZE(f) = 0;
}

/* ---------------- stat + time ---------------- */
static void default_time(ScePspDateTime *t)                             /* FUN_0000a988 */
{
    t->year = 0x7c1; t->month = 1; t->day = 1; t->hour = 0; t->minute = 0; t->second = 0; t->microsecond = 0;
}
int datetime_valid(const ScePspDateTime *t)                             /* FUN_0000a9b0 */
{
    if ((u16)(t->year - 1980) > 0x7f || (u16)(t->month - 1) >= 12 || (u16)(t->day - 1) > 30 ||
        t->hour >= 24 || t->minute > 59 || t->second > 59) return FAT_EINVAL;
    return 0;
}
static void now_dos(u32 *date, u32 *time)                               /* FUN_0000aa38 */
{
    ScePspDateTime t; u32 dos = 0;
    if (sceRtc_driver_9012B140(&t) != 0) default_time(&t);
    if (datetime_valid(&t) != 0) default_time(&t);
    sceRtc_driver_A4A5BF1B(&t, &dos);
    *date = dos >> 16; *time = dos & 0xffff;
}
void dirent_stamp(u8 *de, int created)                                  /* FUN_0000aac0 */
{
    u32 date, time;
    now_dos(&date, &time);
    if (created) { put16(de + 0x10, date); put16(de + 0x0e, time); }
    put16(de + 0x12, date); put16(de + 0x16, time); put16(de + 0x18, date);
}
void dirent_stat(const u8 *de, SceIoStat *st)                           /* FUN_00000538 + 5a84 */
{
    u8 a = de[0x0b];
    u32 kind = (a & 0x10) ? 0x1000 : (a & 0x08) ? 0 : 0x2000;
    st->st_attr = a;
    st->st_mode = kind | ((a & 1) ? 0x16d : 0x1ff);
    st->st_size = (SceOff)(le16(de + 0x1c) | le16(de + 0x1e) << 16);
    if (sceRtc_driver_74772CCC(&st->sce_st_ctime, le16(de + 0x0e) | le16(de + 0x10) << 16) < 0) default_time(&st->sce_st_ctime);
    if (sceRtc_driver_74772CCC(&st->sce_st_mtime, le16(de + 0x16) | le16(de + 0x18) << 16) < 0) default_time(&st->sce_st_mtime);
    if (sceRtc_driver_74772CCC(&st->sce_st_atime, le16(de + 0x12) << 16) < 0) default_time(&st->sce_st_atime);
}

/* ---------------- root helpers ---------------- */
/* FUN_00001f00: want_free=0 -> the volume-label entry (else ENOENT); want_free=1 -> the label or
   the first free root slot (growing a FAT32 root; full FAT12/16 root -> error) */
int vol_label_find(fatms_ctx *c, u8 *ent, diter *pos, int want_free)
{
    diter it;
    int r = dir_iter_init(c, &it, 0);
    while (r >= 0) {
        u8 *e;
        r = dirent_get(c, &e, &it);
        if (r < 0) return r;
        int hit = (e[0] == 0) ? (want_free ? 1 : -1)
                : (e[0] == 0xe5) ? want_free
                : ((e[0x0b] & 0x08) && e[0x0b] != 0x0f);
        if (hit < 0) return FAT_ENOENT;
        if (hit) { memcpy(ent, e, 32); if (pos) *pos = it; return 0; }
        r = dir_iter_move(c, &it, 1);
        if (r < 0) {
            if (!want_free) return r == D_END ? FAT_ENOENT : r;
            if ((it.start == 0 && !fat32(c)) || r != D_END) return r;
            r = dir_extend(c, it.cur);
            if (r >= 0) r = dir_iter_move(c, &it, 1);
        }
    }
    return r;
}

int root_free_entries(fatms_ctx *c, u32 *out)                           /* FUN_00006280 / 000063a0 */
{
    if (c->exfat) return exfat_root_free(c, out);
    u32 base = 0, used = 0, per = 0;
    if (fat32(c)) {
        if ((int)c->fe.free_clusters >= 0) { int r = fat_scan(&c->fe); if (r < 0) return r; }
        per = (u32)c->sectors_per_cluster * 16;
        base = (c->fe.free_clusters & 0x7fffffffu) * per;
        if (base > 0xfff) { *out = 0x1000; return 0; }
    }
    diter it;
    int r = dir_iter_init(c, &it, 0);
    while (r >= 0) {
        u8 *e;
        r = dirent_get(c, &e, &it);
        if (r < 0) break;
        if (e[0] == 0) { *out = fat32(c) ? base + (per - used) : c->root_entries - used; return 0; }
        if (e[0] != 0xe5) used++;
        r = dir_iter_move(c, &it, 1);
    }
    return r;
}
