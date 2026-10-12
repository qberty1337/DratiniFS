/* fatfs_ops.c - Sony 6.61 fatms file/directory workers, ported from the decompile.
 *   path      FUN_00009794 (convert by caller SDK + normalize + split), 9908 (free)
 *   open      FUN_00008158 (+8514 flag check, a200 slot scan, 8618 finalize), close 5e74,
 *             lseek 7c0c, eof 79c4
 *   dirs      dopen 736c, dread 7610, dclose 5f28, pattern ioctl 7a3c
 *   paths     getstat 78f0, chstat 0x5c18, mkdir 0x7dc0, remove 8a20, rmdir 8bc0,
 *             rename 8b68 -> move 6978 (devctl 0x2415830 -> 7108 -> 6978)
 * Every worker runs under the volume mutex on an extended kernel stack; big buffers come from
 * the 0x800 scratch pool, as Sony's do. */
#include "fatms.h"

static int wcs_eq(const u16 *a, const u16 *b)                          /* FUN_0000abd8 == 0 */
{
    while (*a && *a == *b) { a++; b++; }
    return *a == *b;
}

/* FUN_000099fc with its two scratch temporaries */
static int path_split(fatms_ctx *c, fpath *p, u16 *path)
{
    u16 *leaf = (u16 *)fatfs_get_scratch(c);
    if (!leaf) return FAT_ENOMEM;
    char *oem = (char *)fatfs_get_scratch(c);
    if (!oem) { fatfs_put_scratch(c, leaf); return FAT_ENOMEM; }
    int r = fp_split(p, path, leaf, oem);
    fatfs_put_scratch(c, oem);
    fatfs_put_scratch(c, leaf);
    return r;
}

/* FUN_00009794: caller path -> fpath (in a scratch block; release with fatfs_path_put).
   Callers built with SDK < 3.80 pass Shift-JIS, newer ones UTF-8. */
int fatfs_path_get(fatms_ctx *c, const char *in, fpath **out)
{
    *out = 0;
    if (!in) return FAT_EINVAL;
    u16 *w = (u16 *)fatfs_get_scratch(c);
    if (!w) return FAT_ENOMEM;
    int r;
    if (sceKernelGetCompiledSdkVersion() < 0x3080000) {
        sceCodepage_driver_B0AE63AA(0xd);
        r = sceCodepage_driver_855C5C2E(w, 0x800, in);
        if (r < 0) { fatfs_put_scratch(c, w); return r; }
    } else sceCodepage_driver_C899572E(w, 0x800, in);
    for (int i = 0; i < 0x103 && w[i]; i++) if (w[i] == '/') w[i] = '\\';
    u16 *tmp = (u16 *)fatfs_get_scratch(c);                              /* FUN_0000a120's buffer */
    r = tmp ? fp_normalize(w, tmp) : FAT_ENOMEM;
    if (tmp) fatfs_put_scratch(c, tmp);
    if (r >= 0) {
        fpath *p = (fpath *)fatfs_get_scratch(c);
        if (!p) r = FAT_ENOMEM;
        else if ((r = path_split(c, p, w)) >= 0) { *out = p; fatfs_put_scratch(c, w); return 0; }
        else fatfs_put_scratch(c, p);
    }
    fatfs_put_scratch(c, w);
    return r;
}
void fatfs_path_put(fatms_ctx *c, fpath *p) { if (p) fatfs_put_scratch(c, p); }   /* FUN_00009908 */

/* FUN_0000a200: bit0 = a free slot (index of the LAST free one), bit1 = this path is open */
u32 slot_scan(fatms_ctx *c, const fpath *p, int *idx)
{
    u32 st = 0;
    for (int i = 0; i < FAT_NUM_FD; i++) {
        OpenFile *f = (OpenFile *)c->fd[i];
        if (f->inuse == 0) { *idx = i; st |= 1; }
        else if (p && wcs_eq(p->dir, f->path.dir) && wcs_eq(p->leaf, f->path.leaf)) st |= 2;
    }
    return st;
}

int flagcheck(unsigned flags, int exists)                        /* FUN_00008514 */
{
    if ((flags & 0xa00) == 0xa00 && exists) return FAT_EEXIST;
    if ((flags & 0x500) == 0x500) return FAT_EINVAL;
    if ((flags & 0x202) == 0x200 && !exists) return FAT_EINVAL;
    unsigned acc = flags & 3;
    if (acc == 2) {
        if (!(flags & 0x400) && !(flags & 0x100)) { if (flags & 0x200) return 0; if (!exists) return FAT_ENOENT; }
        else if (!(flags & 0x200) && !exists) return FAT_ENOENT;
        return 0;
    }
    if (acc == 1) return (flags & 0x500) ? FAT_EINVAL : 0;
    if (acc == 3) {
        if (!(flags & 0x400) && !(flags & 0x100)) return 0;
        if (!(flags & 0x200) && !exists) return FAT_ENOENT;
        return 0;
    }
    return FAT_EINVAL;
}

static void put_cluster(u8 *de, u32 cl)
{
    de[0x1a] = (u8)cl; de[0x1b] = (u8)(cl >> 8); de[0x14] = (u8)(cl >> 16); de[0x15] = (u8)(cl >> 24);
}

#define ENTER(c) do { int _e = fatfs_prep(c); if (_e >= 0) _e = fatfs_ready(c); if (_e < 0) return _e; } while (0)

int fatfs_open(fatms_ctx *c, fpath *p, unsigned flags, OpenFile **out)  /* FUN_00008158 */
{
    *out = 0;
    ENTER(c);
    if (c->exfat) return exfat_open(c, p, flags, out);
    u32 dcl;
    int r = dir_walk(c, p->dir, &dcl);
    if (r < 0) return r;
    r = dir_lookup(c, 0, dcl, p->leaf);
    int exists = (r == 0);
    if (r < 0 && r != FAT_ENOENT) return r;
    if ((r = flagcheck(flags, exists)) < 0) return r;
    if ((flags & 0x600) == 0x600 && (c->media_flags & MF_WP)) return FAT_EACCES;
    int idx = 0;
    u32 st = slot_scan(c, p, &idx);
    if (st == 0 || st == 2) return FAT_EMFILE;
    if ((flags & 0x600) == 0x600 && (st & 2)) return FAT_EACCES;         /* create/truncate an open file */
    OpenFile *f = (OpenFile *)c->fd[idx];
    memset(f, 0, sizeof(*f));
    int created = 0, trunc = 0;
    if ((flags & 0x200) && !exists) {
        r = dir_find_free(c, f, dcl, p->leaf, p->need_lfn);
        if (r >= 0) created = 1;
        else if (r == FAT_EEXIST) { if ((r = dir_lookup(c, f, dcl, p->leaf)) < 0) return r; trunc = (flags & 0x400) != 0; }
        else return r;
    } else {
        if ((r = dir_lookup(c, f, dcl, p->leaf)) < 0) return r;
        trunc = (flags & 0x400) != 0;
    }
    memcpy(&f->path, p, sizeof(fpath));
    if (created) {
        if (f->path.need_lfn && (r = dir_make_alias(c, &f->path, dcl)) < 0) return r;
        str_to_sfn(f->entry, f->path.sfn);
        dir_new_entry(c, f);
        if ((r = dir_write_entries(c, f)) < 0) return r;
    } else {
        if (F_ATTR(f) & 0x10) return FAT_EACCES;                         /* a directory */
        if ((flags & 2) && ((F_ATTR(f) & 0x19) || (c->media_flags & MF_WP))) return FAT_EACCES;
        if (trunc && f->first_cluster) {
            u32 cl = f->first_cluster;
            f->first_cluster = 0; F_SIZE(f) = 0; put_cluster(f->entry, 0);
            if ((r = dirent_put(c, f->entry, &f->dirpos)) < 0) return r;
            if ((r = fatfs_free_chain(c, cl)) < 0) return r;
            if ((r = fatfs_flush(c)) < 0) return r;
        }
    }
    if ((r = fatfs_flush(c)) < 0) return r;
    /* FUN_00008618 */
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

int fatfs_close(fatms_ctx *c, OpenFile *f)                              /* FUN_00005e74 */
{
    int r = fatfs_fd_valid(c, f);
    if (r < 0) return r;
    r = fatfs_prep(c);
    if (r >= 0 && (r = fatfs_ready(c)) >= 0) {
        if (c->exfat) r = exfat_close(c, f);
        else if (!(f->oflags & 2) || (r = dirent_put(c, f->entry, &f->dirpos)) >= 0) r = fatfs_flush(c);
    }
    if ((int)f->inuse > 0) f->inuse--;
    F_SIZE(f) = 0;
    return r;
}

int fatfs_lseek(fatms_ctx *c, OpenFile *f, SceOff ofs, int whence, SceOff *out)   /* FUN_00007c0c */
{
    ENTER(c);
    int r = fatfs_fd_valid(c, f);
    if (r < 0) return r;
    if (c->exfat && !es_is_file(f)) return exfat_lseek(c, f, ofs, whence, out);   /* DratiniFS: 64-bit */
    u32 lo = (u32)ofs; int hi = (int)((u64)ofs >> 32);
    u32 span = (u32)(hi + 1) - (lo == 0);
    if (span > 1 || (span == 1 && lo == 0)) return FAT_EINVAL;           /* |ofs| must fit in 32 bits */
    u32 np;
    if (whence == 0) np = lo;
    else if (whence == 1) np = f->position + lo;
    else if (whence == 2) np = F_SIZE(f) + lo;
    else return FAT_EINVAL;
    if (hi + (int)(np < lo) != 0) return FAT_EINVAL;                     /* before 0 or past 4 GB */
    f->position = np;
    *out = np;
    return 0;
}

int fatfs_at_eof(fatms_ctx *c, OpenFile *f)                             /* FUN_000079c4 */
{
    ENTER(c);
    int r = fatfs_fd_valid(c, f);
    if (r < 0) return r;
    if (c->exfat && !es_is_file(f)) return exfat_at_eof(c, f);                     /* DratiniFS: 64-bit */
    return F_SIZE(f) <= f->position;
}

int fatfs_set_pattern(fatms_ctx *c, DirHandle *d, const char *pat)      /* FUN_00007a3c */
{
    ENTER(c);
    u16 *w = (u16 *)fatfs_get_scratch(c);
    if (!w) return FAT_ENOMEM;
    sceCodepage_driver_C899572E(w, 0x800, pat);                          /* always UTF-8 */
    int r = 0, n = 0;
    for (; n < 0x800 && w[n]; n++)
        if (w[n] != '.' && (r = fp_char_valid(w[n], 1)) < 0) break;
    if (r >= 0) {
        r = FAT_EINVAL;
        /* Sony checks dir+pattern <= 0x500 chars; the pattern buffer itself holds 0x100 */
        if (fp_wcslen(d->path.dir) + n < 0x501 && n < 0x100) { fp_wcscpy(d->path.leaf, w); r = 0; }
    }
    fatfs_put_scratch(c, w);
    return r;
}

int fatfs_dopen(fatms_ctx *c, fpath *p, DirHandle **out)                /* FUN_0000736c */
{
    *out = 0;
    DirHandle *d = (DirHandle *)fatfs_get_scratch(c);
    if (!d) return FAT_ENOMEM;
    memset(&d->it, 0, sizeof(d->it));
    int seps = 0, chars = 0;
    for (const u16 *s = p->dir; seps < 2; ) {                            /* first component of the dir path */
        while (*s == ' ') s++;
        if (*s == '\\') { seps++; s++; continue; }
        if (*s++ == 0) break;
        chars++;
    }
    u32 cl = 0;
    int r = fatfs_prep(c);
    if (r >= 0) r = fatfs_ready(c);
    if (r >= 0 && c->exfat) { fatfs_put_scratch(c, d); return exfat_dopen(c, p, out); }
    if (r >= 0 && chars == 0) {
        if (seps > 1) r = FAT_EINVAL;
        else { d->path.dir[0] = '\\'; d->path.dir[1] = 0; }
    } else if (r >= 0) {
        u16 *w = (u16 *)fatfs_get_scratch(c);
        if (!w) r = FAT_ENOMEM;
        else {
            fp_wcscpy(w, p->dir);
            int n = fp_wcslen(w);
            if (n && w[n - 1] == '\\') w[n - 1] = 0;
            r = path_split(c, &d->path, w);
            fatfs_put_scratch(c, w);
        }
        u32 pcl;
        if (r >= 0) r = dir_walk(c, d->path.dir, &pcl);
        if (r >= 0) {
            OpenFile *t = (OpenFile *)fatfs_get_scratch(c);
            if (!t) r = FAT_ENOMEM;
            else {
                r = dir_lookup(c, t, pcl, d->path.leaf);
                if (r >= 0 && !(t->entry[0x0b] & 0x10)) r = FAT_ENOTDIR;
                cl = t->first_cluster;
                fatfs_put_scratch(c, t);
            }
        }
    }
    if (r >= 0) {
        d->path.leaf[0] = '*'; d->path.leaf[1] = 0;                      /* default wildcard */
        r = dir_iter_init(c, &d->it, cl);
    }
    if (r < 0) { fatfs_put_scratch(c, d); return r; }
    d->is_dir = 1;
    d->ctx = c;
    *out = d;
    return 0;
}

/* FUN_00007610: next entry matching the handle's pattern. d_private is the caller's buffer:
   SDK < 3.80 gets {sfn[13], SJIS lfn[256]}; newer gets the 0x414 struct {size, sfn[16], lfn[1024]} */
int fatfs_dread(fatms_ctx *c, DirHandle *d, SceIoDirent *de, void *priv)
{
    ENTER(c);
    if (c->exfat) return exfat_dread(c, d, de, priv);
    if (d->it.lba == 0) return FAT_ENOENT;
    OpenFile *t = (OpenFile *)fatfs_get_scratch(c);
    if (!t) return FAT_ENOMEM;
    char *s = 0;
    int r = dir_scan(c, t, &d->it, d->path.leaf, 1);
    if (r < 0) goto out;
    dirent_stat(t->entry, &de->d_stat);
    if (!(s = (char *)fatfs_get_scratch(c))) { r = FAT_ENOMEM; goto out; }
    if (sceKernelGetCompiledSdkVersion() < 0x3080000) {
        if ((r = sceCodepage_driver_47BDF633(s, 0x800, t->path.leaf)) < 0) goto out;
        if (strlen(s) < 0x100) strncpy(de->d_name, s, 0x100);
        else strncpy(de->d_name, t->path.sfn, 0xd);
        if (priv) {
            strncpy((char *)priv, t->path.sfn, 0xd);
            if ((r = sceCodepage_driver_47BDF633((char *)priv + 0xd, 0x100, t->path.leaf)) < 0) goto out;
        }
    } else {
        sceCodepage_driver_907CBFD2(s, 0x800, t->path.leaf);
        if (strlen(s) < 0x100) strncpy(de->d_name, s, 0x100);
        else {
            if ((r = sceCodepage_driver_855C5C2E((u16 *)s, 0x400, t->path.sfn)) < 0) goto out;
            sceCodepage_driver_907CBFD2(de->d_name, 0x100, (u16 *)s);
        }
        if (priv) {
            if (*(u32 *)priv != 0x414) { r = FAT_EINVAL; goto out; }
            strncpy((char *)priv + 4, t->path.sfn, 0xd);
            sceCodepage_driver_907CBFD2((char *)priv + 0x14, 0x400, t->path.leaf);
        }
    }
    d->it = t->dirpos;
    r = dir_iter_move(c, &d->it, 1);
    if (r < 0) { if (r != (int)0x8001000d) goto out; d->it.lba = 0; }  /* last entry: mark the end */
    r = 0;
out:
    if (s) fatfs_put_scratch(c, s);
    fatfs_put_scratch(c, t);
    return r;
}

int fatfs_dclose(fatms_ctx *c, DirHandle *d)                            /* FUN_00005f28 */
{
    int r = fatfs_prep(c);
    if (r >= 0) fatfs_put_scratch(c, d);
    return r;
}

int fatfs_getstat(fatms_ctx *c, fpath *p, SceIoStat *st)                /* FUN_000078f0 */
{
    ENTER(c);
    if (c->exfat) return exfat_getstat(c, p, st);
    u32 cl;
    int r = dir_walk(c, p->dir, &cl);
    if (r < 0) return r;
    OpenFile *t = (OpenFile *)fatfs_get_scratch(c);
    if (!t) return FAT_ENOMEM;
    r = dir_lookup(c, t, cl, p->leaf);
    if (r >= 0) { r = 0; dirent_stat(t->entry, st); }
    fatfs_put_scratch(c, t);
    return r;
}

int fatfs_chstat(fatms_ctx *c, fpath *p, const SceIoStat *st, int bits) /* 0x5c18 */
{
    if ((bits & 2) && (st->st_attr & ~0x37u)) return FAT_EINVAL;
    if ((bits & 8) && datetime_valid(&st->sce_st_ctime)) return FAT_EINVAL;
    if ((bits & 0x20) && datetime_valid(&st->sce_st_mtime)) return FAT_EINVAL;
    ENTER(c);
    if (c->exfat) return exfat_chstat(c, p, st, bits);
    u32 cl;
    int r = dir_walk(c, p->dir, &cl), idx;
    if (r < 0) return r;
    if (c->media_flags & MF_WP) return FAT_EACCES;
    if (slot_scan(c, p, &idx) & 2) return FAT_EACCES;                    /* busy: the file is open */
    OpenFile *t = (OpenFile *)fatfs_get_scratch(c);
    if (!t) return FAT_ENOMEM;
    r = dir_lookup(c, t, cl, p->leaf);
    if (r < 0) goto out;
    if (bits & 2) {
        if ((t->entry[0x0b] & 0x10) != (st->st_attr & 0x10)) { r = FAT_EINVAL; goto out; }   /* dir bit is fixed */
        t->entry[0x0b] = (u8)st->st_attr;
    }
    if (bits & 1) {                                                       /* any write bit clears read-only */
        if (st->st_mode & 0x92) t->entry[0x0b] &= ~1; else t->entry[0x0b] |= 1;
    }
    if (bits & 8) {
        u32 dos = 0; sceRtc_driver_A4A5BF1B(&st->sce_st_ctime, &dos);
        t->entry[0x0e] = (u8)dos; t->entry[0x0f] = (u8)(dos >> 8); t->entry[0x10] = (u8)(dos >> 16); t->entry[0x11] = (u8)(dos >> 24);
    }
    if (bits & 0x20) {
        u32 dos = 0; sceRtc_driver_A4A5BF1B(&st->sce_st_mtime, &dos);
        t->entry[0x16] = (u8)dos; t->entry[0x17] = (u8)(dos >> 8); t->entry[0x18] = (u8)(dos >> 16); t->entry[0x19] = (u8)(dos >> 24);
    }
    r = dirent_put(c, t->entry, &t->dirpos);
    if (r >= 0) { r = fatfs_flush(c); if (r > 0) r = 0; }
out:
    fatfs_put_scratch(c, t);
    return r;
}

int fatfs_mkdir(fatms_ctx *c, fpath *p)                                 /* 0x7dc0 */
{
    ENTER(c);
    if (c->exfat) return exfat_mkdir(c, p);
    if (c->media_flags & MF_WP) return FAT_EACCES;
    OpenFile *f = (OpenFile *)fatfs_get_scratch(c);
    if (!f) return FAT_ENOMEM;
    u32 pcl, cl;
    u8 dot[32];
    diter it;
    int r = dir_walk(c, p->dir, &pcl);
    if (r < 0) goto out;
    r = dir_lookup(c, 0, pcl, p->leaf);
    if (r == 0) { r = FAT_EEXIST; goto out; }
    if (r != FAT_ENOENT) goto out;
    if ((r = dir_find_free(c, f, pcl, p->leaf, p->need_lfn)) < 0) goto out;
    memcpy(&f->path, p, sizeof(fpath));
    if (f->path.need_lfn && (r = dir_make_alias(c, &f->path, pcl)) < 0) goto out;
    str_to_sfn(f->entry, f->path.sfn);
    if ((r = fat_find_free(&c->fe, 0, &cl, 0)) < 0) goto out;
    if (cl == 0) { r = FAT_ENOSPC; goto out; }
    put_cluster(f->entry, cl);
    f->first_cluster = cl;
    if ((r = fat_set(&c->fe, cl, 0x0fffffffu)) < 0) goto out;
    c->fe.next_free = cl + 1;
    if ((r = dir_zero_cluster(c, cl)) < 0) { fat_set(&c->fe, cl, 0); fat_flush(&c->fe); goto out; }
    memset(dot, 0, sizeof(dot));
    memset(dot, ' ', 11); dot[0] = '.'; dot[0x0b] = 0x10;
    dirent_stamp(dot, 1);
    if ((r = dir_iter_init(c, &it, cl)) < 0) goto out;
    put_cluster(dot, cl);                                                /* "."  -> itself */
    if ((r = dirent_put(c, dot, &it)) < 0) goto out;
    dir_iter_move(c, &it, 1);
    dot[1] = '.';
    put_cluster(dot, (c->fat_type == 32 && f->first_cluster == 2) ? 0 : f->dirpos.start);   /* ".." -> parent */
    if ((r = dirent_put(c, dot, &it)) < 0) goto out;
    dirent_stamp(f->entry, 1);
    f->entry[0x0b] = 0x10;
    F_SIZE(f) = 0;
    if ((r = dir_write_entries(c, f)) < 0) goto out;
    r = fatfs_flush(c);
    if (r > 0) r = 0;
out:
    fatfs_put_scratch(c, f);
    return r;
}

int fatfs_remove(fatms_ctx *c, fpath *p)                                /* FUN_00008a20 */
{
    ENTER(c);
    if (c->exfat) return exfat_remove(c, p);
    u32 cl;
    int r = dir_walk(c, p->dir, &cl), idx;
    if (r < 0) return r;
    if (c->media_flags & MF_WP) return FAT_EACCES;
    if (slot_scan(c, p, &idx) & 2) return FAT_EACCES;                    /* open files can't be removed */
    OpenFile *t = (OpenFile *)fatfs_get_scratch(c);
    if (!t) return FAT_ENOMEM;
    r = dir_lookup(c, t, cl, p->leaf);
    if (r >= 0) {
        r = FAT_EACCES;                                                  /* read-only, label or directory */
        if (!(t->entry[0x0b] & 0x19) && (r = dir_delete_entries(c, t)) >= 0 &&
            (t->first_cluster == 0 || (r = fatfs_free_chain(c, t->first_cluster)) >= 0)) {
            r = fatfs_flush(c);
            if (r > 0) r = 0;
        }
    }
    fatfs_put_scratch(c, t);
    return r;
}

int fatfs_rmdir(fatms_ctx *c, fpath *p)                                 /* FUN_00008bc0 */
{
    ENTER(c);
    if (c->exfat) return exfat_rmdir(c, p);
    u32 cl;
    int r = dir_walk(c, p->dir, &cl);
    if (r < 0) return r;
    if (c->media_flags & MF_WP) return FAT_EACCES;
    OpenFile *t = (OpenFile *)fatfs_get_scratch(c);
    if (!t) return FAT_ENOMEM;
    r = dir_lookup(c, t, cl, p->leaf);
    if (r >= 0) {
        r = FAT_EACCES;                                                  /* must be a writable directory */
        if ((t->entry[0x0b] & 0x19) == 0x10 && (r = dir_is_empty(c, t->first_cluster)) >= 0 &&
            (r = dir_delete_entries(c, t)) >= 0 && (r = fatfs_free_chain(c, t->first_cluster)) >= 0 &&
            (r = fatfs_flush(c)) >= 0) {
            dirent_forget(c, t->first_cluster);
            r = 0;
        }
    }
    fatfs_put_scratch(c, t);
    return r;
}

/* FUN_00006978: relink src's entry under dst (data untouched; a moved dir's ".." is not rewritten) */
int fatfs_move(fatms_ctx *c, fpath *src, fpath *dst)
{
    ENTER(c);
    if (c->exfat) return exfat_move(c, src, dst);
    if (c->media_flags & MF_WP) return FAT_EACCES;
    u32 scl, dcl;
    int r = dir_walk(c, src->dir, &scl);
    if (r < 0) return r;
    OpenFile *t = (OpenFile *)fatfs_get_scratch(c), *n = 0;
    if (!t) return FAT_ENOMEM;
    if ((r = dir_lookup(c, t, scl, src->leaf)) < 0) goto out;
    if (t->entry[0x0b] & 1) { r = FAT_EACCES; goto out; }
    if ((r = dir_walk(c, dst->dir, &dcl)) < 0) goto out;
    r = dir_lookup(c, 0, dcl, dst->leaf);
    if (r == 0) { r = FAT_EEXIST; goto out; }
    if (r != FAT_ENOENT) goto out;
    if (!(n = (OpenFile *)fatfs_get_scratch(c))) { r = FAT_ENOMEM; goto out; }
    if ((r = dir_find_free(c, n, dcl, dst->leaf, dst->need_lfn)) < 0) goto out;
    memcpy(&n->path, dst, sizeof(fpath));
    memcpy(n->entry, t->entry, 32);
    n->first_cluster = t->first_cluster;
    if (n->path.need_lfn && (r = dir_make_alias(c, &n->path, dcl)) < 0) goto out;
    str_to_sfn(n->entry, n->path.sfn);
    if ((r = dir_write_entries(c, n)) >= 0 && (r = dir_delete_entries(c, t)) >= 0) {
        r = fatfs_flush(c);
        if (r > 0) r = 0;
    }
out:
    if (n) fatfs_put_scratch(c, n);
    fatfs_put_scratch(c, t);
    return r;
}
