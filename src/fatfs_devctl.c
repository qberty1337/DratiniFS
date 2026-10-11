/* fatfs_devctl.c - Sony 6.61 fatms IoDevctl worker: FUN_00006c18 (fs level) falling through to
 * vtable+0x2c fatfs_vt2c (device level). Argument conventions are Sony's: several commands take
 * a POINTER passed inside indata (devinfo, volume label get/set, free root entries, 0x241d813),
 * and user pointers are k1-checked. */
#include "fatms.h"

#define D_EINVAL  FAT_EINVAL

static int bad_ptr(const void *p) { return (int)(fat_k1() & (u32)p) < 0; }   /* user caller, kernel pointer */
static void *in_ptr(void *in, int inlen, int need_len)                 /* *(void **)indata, checked */
{
    if (!in || (need_len && inlen < 4)) return 0;
    void *p = *(void **)in;
    return (!p || bad_ptr(p)) ? 0 : p;
}

/* ---- insert/eject callback table (ctx+0x150) ---- */
int fatfs_cb_register(fatms_ctx *c, SceUID cb)                          /* FUN_0000c770 */
{
    int st = sceKernelSuspendDispatchThread(), found = 0;
    for (int i = 0; i < 32; i++) {
        if (c->tbl150[i] != 0) continue;
        c->tbl150[i] = (u32)cb;
        found = 1;
        int nr = 0;
        if (c->flags & ST_ASSIGNED) nr = sceKernelNotifyCallback(cb, 1);
        else {
            int sv[4] = { 0 };
            int rr = k_sceIoDevctl("mscmhc0:", 0x02025801, 0, 0, sv, 4);
            if (rr >= 0 && sv[0] != 4) break;                           /* not inserted: no notify */
            nr = sceKernelNotifyCallback(cb, c->assign_result);
        }
        if (nr == -0x7ffdfe5f) c->tbl150[i] = 0;                         /* stale callback uid */
        break;
    }
    sceKernelResumeDispatchThread(st);
    return found ? 0 : D_EINVAL;
}
int fatfs_cb_unregister(fatms_ctx *c, SceUID cb)                        /* FUN_0000b430 */
{
    int st = sceKernelSuspendDispatchThread(), found = 0;
    for (int i = 0; i < 32; i++)
        if (c->tbl150[i] == (u32)cb) { c->tbl150[i] = 0; found = 1; break; }
    sceKernelResumeDispatchThread(st);
    return found ? 0 : D_EINVAL;
}
/* FUN_0000c528/c5b4: the uid must be a callback a user caller may use */
int fatfs_cb_arg(void *in, int inlen, SceUID *cb)
{
    if (!in || inlen < 4) return D_EINVAL;
    SceUidControlBlock *blk = 0;
    if (sceKernelGetUIDcontrolBlock(*(SceUID *)in, &blk) != 0) return D_EINVAL;
    if ((int)fat_k1() < 0 && !(*(u16 *)((u8 *)blk + 0x16) & 2)) return D_EINVAL;   /* user-accessible uid */
    *cb = *(SceUID *)in;
    return 0;
}

/* FUN_00005fe8: {max clusters, free clusters, free erase-block clusters, bytes/sector, sectors/cluster} */
static int devinfo(fatms_ctx *c, u32 *o)
{
    int r = fatfs_prep(c);
    if (r >= 0) r = fatfs_ready(c);
    if (r < 0) return r;
    if (c->exfat) r = exfat_sync_free(c);                              /* DratiniFS: the exFAT bitmap */
    else if ((int)c->fe.free_clusters >= 0) r = fat_scan(&c->fe);
    if (r < 0) return r;
    u32 bps = c->bytes_per_sector, spc = c->sectors_per_cluster, csz = bps * spc;
    o[0] = c->fat_max_cluster - 2;
    o[1] = c->fe.free_clusters & 0x7fffffffu;
    o[2] = c->fe.free_contig & 0x7fffffffu;
    o[3] = bps; o[4] = spc;
    if (c->flags & 0x100) {                                              /* devctl 0x2425858: keep bytes < 2 GB */
        u32 lim = 0x7fffffffu / csz, freec = o[1];
        if ((u64)csz * o[0] > 0x7fffffffu) o[0] = lim;
        if ((u64)csz * freec > 0x7fffffffu) o[1] = lim;
        if ((u64)o[2] * bps * freec > 0x7fffffffu) o[2] = lim;          /* sic: Sony tests contig*bps*free */
    }
    return 0;
}

/* FUN_00006778: label text -> 11 OEM chars, uppercased, no splitting a double-byte char at 7 */
static int vol_name(u8 *out, const char *s)
{
    if (!s || !*s || *s == ' ') return D_EINVAL;
    static const char bad[] = "\\\"./:<>|*?";                            /* DAT_0000cedc (FUN_00009994) */
    memset(out, ' ', 11);
    int dbl = 0;
    for (int i = 0; i < 12; i++, s++) {
        unsigned char ch = (unsigned char)*s;
        if (ch == 0) {
            if (out[0] == 0xe5) out[0] = 0x05;
            out[11] = 0;
            return 0;
        }
        if (dbl == 0 && sceCodepage_driver_014E0C72(s)) { dbl = 2; if (i == 7) return D_EINVAL; }
        int invalid = (ch < 0x20 && ch != 5);
        for (const char *b = bad; !invalid && *b; b++) invalid = ((unsigned char)*b == ch);
        if (invalid && dbl == 0) return D_EINVAL;
        if (dbl) { out[i] = ch; dbl--; } else out[i] = (u8)sceCodepage_driver_DCD95405(ch);
    }
    return D_EINVAL;                                                     /* longer than 11 */
}
/* FUN_000068b0: write the label into the boot sector's BPB (and the FAT32 backup boot sector) */
static int vol_bpb(fatms_ctx *c, const u8 *name)
{
    u32 off = 0x2b, n = 1, step = 0;
    if (c->fat_type == 32) { off = 0x47; n = 2; step = c->backup_boot; }
    for (u32 i = 0; i < n; i++) {
        int r = fatfs_sector_io(c, 1, off, 0xb, (u8 *)name, i * step, 0);
        if (r < 0) return r;
    }
    return 0;
}
static int vol_set(fatms_ctx *c, const char *s)                         /* FUN_0000645c */
{
    u8 nm[12], e[32];
    diter pos;
    int r = vol_name(nm, s);
    if (r < 0) return r;
    if ((r = fatfs_prep(c)) < 0 || (r = fatfs_ready(c)) < 0) return r;
    if (c->media_flags & MF_WP) return FAT_EACCES;
    if (c->exfat) return FAT_EUNSUP;                                    /* DratiniFS: no FAT label on exFAT */
    r = vol_label_find(c, e, &pos, 0);
    if (r != FAT_ENOENT) return r;                                       /* a label exists: Sony leaves it */
    if ((r = vol_label_find(c, e, &pos, 1)) < 0) return r;
    memset(e, 0, 32);
    memcpy(e, nm, 11);
    e[0x0b] = 0x08;
    dirent_stamp(e, 1);
    if ((r = dirent_put(c, e, &pos)) < 0 || (r = dirent_flush(c)) < 0) return r;
    r = vol_bpb(c, nm);
    return r > 0 ? 0 : r;
}
static int vol_get(fatms_ctx *c, u8 *out)                               /* FUN_000065c8 */
{
    u8 e[32];
    int r = fatfs_prep(c);
    if (r >= 0) r = fatfs_ready(c);
    if (r >= 0) r = c->exfat ? FAT_ENOENT : vol_label_find(c, e, 0, 0);
    if (r < 0) return r;
    memcpy(out, e, 11);
    if (out[0] == 0x05) out[0] = 0xe5;
    out[0x0b] = e[0x0b];
    *(u32 *)(out + 0x0c) = (u32)(e[0x18] | e[0x19] << 8);               /* write date */
    *(u32 *)(out + 0x10) = (u32)(e[0x16] | e[0x17] << 8);               /* write time */
    return 0;
}
static int vol_del(fatms_ctx *c)                                        /* FUN_0000668c */
{
    u8 e[32], blank[12];
    diter pos;
    int r = fatfs_prep(c);
    if (r >= 0) r = fatfs_ready(c);
    if (r < 0) return r;
    if (c->media_flags & MF_WP) return FAT_EACCES;
    if (c->exfat) return FAT_ENOENT;                                    /* DratiniFS: no FAT label on exFAT */
    if ((r = vol_label_find(c, e, &pos, 0)) < 0) return r;
    e[0] = 0xe5;
    if ((r = dirent_put(c, e, &pos)) < 0 || (r = dirent_flush(c)) < 0) return r;
    memset(blank, ' ', 11); blank[11] = 0;
    r = vol_bpb(c, blank);
    return r > 0 ? 0 : r;
}

int fatfs_devctl(fatms_ctx *c, unsigned int cmd, void *in, int inlen, void *out, int outlen)
{
    void *p;
    int r;
    switch (cmd) {
    /* ---------- fs level (FUN_00006c18) ---------- */
    case 0x0241D814:                                /* 6f4c -> 5f80: DirentCache/FAT write-through mode */
        if (!in || inlen < 4) return D_EINVAL;
        if ((r = fatfs_prep(c)) < 0 || (r = fatfs_ready(c)) < 0) return r;
        if (*(u32 *)in > 1) return D_EINVAL;
        c->mode_c4 = *(u32 *)in;
        return 0;
    case 0x02415830: {                              /* 7108 -> 6978: move src -> dst (any directory) */
        if (!in || inlen < 8) return D_EINVAL;
        const char *s = ((const char **)in)[0], *d = ((const char **)in)[1];
        if (!s || !d || bad_ptr(s) || bad_ptr(d)) return D_EINVAL;
        fpath *ps, *pd;
        if ((r = fatfs_path_get(c, s, &ps)) < 0) return r;
        if ((r = fatfs_path_get(c, d, &pd)) >= 0) {
            r = fatfs_move(c, ps, pd);
            if (r > 0) r = 0;
            fatfs_path_put(c, pd);
        }
        fatfs_path_put(c, ps);
        return r;
    }
    case 0x00000100:                                /* 731c: flush */
        if ((r = fatfs_prep(c)) < 0 || (r = fatfs_ready(c)) < 0) return r;
        r = fatfs_flush(c);
        return r > 0 ? 0 : r;
    case 0x0240D81B:                                /* 668c: delete volume label */
        return vol_del(c);
    case 0x02415859:                                /* 72e8: get flag 0x100 */
        if (!out || outlen != 4) return D_EINVAL;
        *(u32 *)out = c->flags & 0x100;
        return 0;
    case 0x02415857:                                /* 723c: get codepage -> outdata */
        if (!out || outlen != 4) return D_EINVAL;
        r = sceCodepage_driver_0AA54D6D((int *)out);
        return r > 0 ? 0 : r;
    case 0x0241D813: {                              /* 6e80: XMB "Format Memory Stick" (Sony: the card */
        if (!in) return D_EINVAL;                   /* formats itself through msstor 0x211d814) */
        const char *b = *(const char **)in;
        if (!b || bad_ptr(b)) return D_EINVAL;
        if (c->ops == &g_ms_ops) {
            r = dfs_format(c, -1);                  /* DratiniFS: software format, ES kept (format.c) */
            return r > 0 ? 0 : r;
        }
        fatfs_set_flags(c, ST_NOMEDIA, 1);          /* Sony's, for the PSP Go's internal storage: its */
        r = c->ops->format(c, *b);                  /* own format (vtable+0x24), then Sony's MS rescan */
        fatfs_set_flags(c, ST_NOMEDIA, 0);          /* (a direct call in 6e80, not the volume's) */
        if (r < 0) { fatfs_rescan(c); return r; }
        r = fatfs_rescan(c);
        return r > 0 ? 0 : r;
    }
    case 0x02425858:                                /* 7284: set flag 0x100 (2 GB clamp) */
        if (!in || inlen != 4) return D_EINVAL;
        if (*(int *)in != 0 && *(int *)in != 1) return D_EINVAL;
        fatfs_set_flags(c, 0x100, *(int *)in);
        return 0;
    case 0x02425818:                                /* 6f84 -> 5fe8: device info */
        if (!(p = in_ptr(in, inlen, 0))) return D_EINVAL;
        r = devinfo(c, (u32 *)p);
        return r > 0 ? 0 : r;
    case 0x0241D819:                                /* 6fdc -> 645c: set volume label */
        if (!(p = in_ptr(in, inlen, 1))) return D_EINVAL;
        return vol_set(c, (const char *)p);
    case 0x02425856:                                /* 71f8: set codepage */
        if (!in || inlen != 4) return D_EINVAL;
        r = sceCodepage_driver_B0AE63AA(*(int *)in);
        return r > 0 ? 0 : r;
    case 0x0242D81A:                                /* 7040 -> 65c8: get volume label */
        if (!(p = in_ptr(in, inlen, 1))) return D_EINVAL;
        return vol_get(c, (u8 *)p);
    case 0x0242585A:                                /* set flag 0x400: keep fds on eject */
        fatfs_set_flags(c, 0x400, 1);
        return 0;
    case 0x0242D81C:                                /* 70a4 -> 6210: free root-directory entries */
        if (!(p = in_ptr(in, inlen, 1))) return D_EINVAL;
        if ((r = fatfs_prep(c)) < 0 || (r = fatfs_ready(c)) < 0) return r;
        return root_free_entries(c, (u32 *)p);
    }
    return c->ops->devctl(c, cmd, in, inlen, out, outlen);           /* device level: the volume's table */
}

/* the Memory Stick's device-level devctls (vtable+0x2c, fatfs_vt2c) */
int ms_devctl(fatms_ctx *c, unsigned int cmd, void *in, int inlen, void *out, int outlen)
{
    int r;
    switch (cmd) {
    case 0x02415822: {                              /* c5b4 -> b430: unregister callback */
        SceUID cb;
        if ((r = fatfs_cb_arg(in, inlen, &cb)) < 0) return r;
        return fatfs_cb_unregister(c, cb);
    }
    case 0x0240D81E:                                /* vt18: force eject+insert (remount) */
        return fatfs_rescan(c);
    case 0x02415821: {                              /* c528 -> c770: register callback */
        SceUID cb;
        if ((r = fatfs_cb_arg(in, inlen, &cb)) < 0) return r;
        return fatfs_cb_register(c, cb);
    }
    case 0x02425824: {                              /* c640: write-protect switch */
        if (!out) return D_EINVAL;
        if (!(c->flags & ST_OPEN)) return FAT_ENODEV;
        int sv = 0;
        if ((r = k_sceIoIoctl(c->ctrl_fd, 0x02125009, 0, 0, &sv, 4)) < 0) return r;
        *(u32 *)out = (sv == 1);
        return 0;
    }
    case 0x02425823:                                /* c740: is ms0: assigned */
        if (!out) return D_EINVAL;
        *(u32 *)out = (c->flags & ST_ASSIGNED) ? 1 : 0;
        return 0;
    case 0x02425825: {                              /* c6c4: block device writability */
        if (!out) return D_EINVAL;
        if (!(c->flags & ST_OPEN)) return FAT_ENODEV;
        int sv = 0;
        if ((r = k_sceIoIoctl(c->ctrl_fd, 0x02125008, 0, 0, &sv, 4)) != 0) return r;
        *(u32 *)out = (u32)sv;
        return 0;
    }
    case 0x04100001: case 0x04100002: case 0x04100003: case 0x04100004:   /* DratiniFS: the ES app */
    case 0x04100005: case 0x04100006: case 0x04100007:
        return es_devctl(c, cmd, in, inlen, out, outlen);
    }
    return FAT_EUNSUP;
}
