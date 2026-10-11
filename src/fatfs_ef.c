/* fatfs_ef.c - PSP Go only (built with PSPGO into fatmsef.prx): Sony 6.61 fatmsef's internal storage
 * (research/docs/PSPGO_STORAGE_ANALYSIS.md). fatmsef is Sony's fatms plus two more volumes that run
 * the same FAT engine through this table (fatmsef 0xf2d0) instead of the Memory Stick's:
 *   fatef  ef0:  eflash0a0f0p0:  "SceFatfsEf"  type 2   the user storage (FAT32)
 *   fateh  eh0:  eflash0a0f1p0:  "SceFatfsEh"  type 3   the hidden system partition (FAT16)
 * Internal storage is always there: no media thread (start = insert), no write-protect switch, no MS
 * attributes, and a software format with a per-unit layout from factory data.
 * When a game runs from internal storage (vol_emulated), ms0: is that storage too: module_start
 * aliases the Memory Stick's device names to msemu.prx's and to fatef0:, and the fatms volume runs on
 * this table sharing fatef's fd, buffers and mutex (S below; fatfs_ioinit shares the mutex).
 * Addresses are fatmsef's. */
#include "fatms.h"

static struct {                    /* fatef's, for the emulated ms0: volume (DAT_0000fa48..fa5c) */
    void *fdbuf, *fatcache, *dirent, *databuf;
    SceUID fd;
    int fd_mode;
} S;

static int emu_ms(const fatms_ctx *c) { return vol_emulated() && c->vol_type == 0; }
static void put16(u8 *p, u32 v) { p[0] = (u8)v; p[1] = (u8)(v >> 8); }
static void put32(u8 *p, u32 v) { put16(p, v); put16(p + 2, v >> 16); }

/* 0xe74c: booted from internal flash as a game, or over USB/WLAN with apitype 0x132/0x133 */
int vol_emulated(void)
{
    if (sceKernelBootFrom() == 0x50 && sceKernelApplicationType() == 0x200) return 1;
    if (sceKernelBootFrom() != 0x30) return 0;
    return sceKernelInitApitype() == 0x132 || sceKernelInitApitype() == 0x133;
}

/* 0xe7d8: the Memory Stick's names -> msemu's devices and fatef0: (the first failure stops it) */
int ef_alias_ms0(void)
{
    static const char *const a[6][2] = {
        { "mscmhc:", "mscmhcemu0:" }, { "mscmhc0:", "mscmhcemu0:" }, { "msstor0p:", "msstoremu0:" },
        { "msstor0p1:", "msstoremu0:" }, { "fatms:", "fatef0:" }, { "fatms0:", "fatef0:" },
    };
    int r = 0;
    for (int i = 0; i < 6; i++) if ((r = k_sceIoAddAlias(a[i][0], a[i][1])) < 0) return r;
    return r > 0 ? 0 : r;
}

/* ---------------- block device, assign ---------------- */
static int ef_open(fatms_ctx *c)                                        /* 0xc200 (MS: c0a4) */
{
    if (c->flags & ST_OPEN) return (int)0x80000020;
    int mode = S.fd_mode;
    if (!emu_ms(c)) {
        int r = sceEFlash_driver_8FB8591D(0);                             /* wait until the flash is up */
        if (r < 0) return r;
        mode = 0x4000003;
        SceUID fd = k_sceIoOpen(c->name_msstor0p1, mode, 0);
        if (fd < 0) {
            mode = 0x4000001;
            if ((fd = k_sceIoOpen(c->name_msstor0p1, mode, 0)) < 0) return fd;
        }
        c->ctrl_fd = fd;
        if (c->vol_type == 2) { S.fd = fd; S.fd_mode = mode; }
    } else {
        if (!S.fd) return FAT_ENODEV;
        c->ctrl_fd = S.fd;                                                /* the emulated ms0: uses fatef's */
    }
    fatfs_set_flags(c, ST_OPEN, 1);
    if (mode == 1) fatfs_set_flags(c, ST_RDONLY_SW, 1);                    /* never, as Sony's c0a4 */
    return 0;
}
static int ef_close(fatms_ctx *c)                                       /* 0xc3a8 (MS: c1b8) */
{
    if (!(c->flags & ST_OPEN)) return (int)0x80000020;
    k_sceIoClose(c->ctrl_fd);
    fatfs_set_flags(c, ST_OPEN, 0);
    c->ctrl_fd = 0;
    return 0;
}
/* 0xc610 (MS: c354). Real devices assign with 0x80000000 (no alias lookup); the emulated ms0:
   needs the aliases. The result is kept for callbacks; nobody is notified here. */
static int ef_assign(fatms_ctx *c)
{
    u32 f = c->flags;
    if (!(f & ST_OPEN)) return FAT_ENODEV;
    if (f & ST_ASSIGNED) return (int)0x80000020;
    u32 mode = (f & 0x3f) >> 5;
    if (c->vol_type != 0) mode |= 0x80000000u;
    int r = k_sceIoAssign(c->name_ms0, c->name_msstor0p1, c->name_fatms0, (int)mode, 0, 0);
    if (r == 0) {
        c->assign_count++;
        fatfs_set_flags(c, ST_ASSIGNED, 1);
        fatfs_set_flags(c, ST_ASSIGN_ERR, 0);
        c->assign_result = 1;
    } else {
        fatfs_set_flags(c, ST_ASSIGN_ERR, 1);
        c->assign_result = r;
    }
    return 0;
}
static int ef_unassign(fatms_ctx *c)                                    /* 0xc7e4: c430 without the notify */
{
    u32 f = c->flags;
    if ((f & ST_SUSPEND) && !(f & ST_ASSIGNED)) return -0x7ffdfcdf;
    int r = k_sceIoUnassign(c->name_ms0);
    if (r != 0 && r != -0x7ffdfcdf) return r;
    if (r == 0 && (int)--c->assign_count < 1) { fatfs_set_flags(c, ST_ASSIGNED, 0); c->assign_count = 0; }
    if (!(c->flags & 0x400)) r = k_sceIoTerminateFd(c->name_fatms);
    for (int i = 0; i < FAT_NUM_FD; i++) ((OpenFile *)c->fd[i])->inuse = 0;
    return r;
}

/* ---------------- the volume table ---------------- */
static int ef_init(fatms_ctx *c)                                        /* 0xcee8 (MS: b4bc, minus the thread) */
{
    char nm[32];
    int share = emu_ms(c);
    u8 *fdbuf;
    if (share) {
        if (!(fdbuf = S.fdbuf)) return FAT_ENOMEM;
    } else {
        snprintf(nm, sizeof(nm), "%sFdBuf", c->obj_name);
        if (!(fdbuf = (u8 *)fatfs_alloc_block(&c->fdbuf_fpl, FAT_NUM_FD * FAT_FD_SIZE, nm))) return FAT_ENOMEM;
        if (c->vol_type == 2) S.fdbuf = fdbuf;
    }
    for (int i = 0; i < FAT_NUM_FD; i++, fdbuf += FAT_FD_SIZE) { c->fd[i] = fdbuf; ((OpenFile *)fdbuf)->inuse = 0; }
    fatfs_clear_tbl150(c);

    void *fc;
    if (share) {
        if (!(fc = S.fatcache)) return FAT_ENOMEM;
    } else {
        snprintf(nm, sizeof(nm), "%sFatCache", c->obj_name);
        if (!(fc = fatfs_alloc_block(&c->fatcache_fpl, 0xc00, nm))) { sceKernelDeleteFpl(c->fdbuf_fpl); return FAT_ENOMEM; }
        if (c->vol_type == 2) S.fatcache = fc;
    }
    c->fat_cache_tag = 0xffffffff;
    c->fat_cache = fc;
    c->fat_cache_count = 6;

    u8 *dc;
    if (share) {
        if (!(dc = S.dirent)) return FAT_ENOMEM;
    } else {
        snprintf(nm, sizeof(nm), "%sDirentCache", c->obj_name);
        if (!(dc = (u8 *)fatfs_alloc_block(&c->dirent_fpl, FAT_NUM_DIRENT * 0x800, nm))) {
            sceKernelDeleteFpl(c->fatcache_fpl); sceKernelDeleteFpl(c->fdbuf_fpl);
            return FAT_ENOMEM;
        }
        if (c->vol_type == 2) S.dirent = dc;
    }
    for (int i = 0; i < FAT_NUM_DIRENT; i++) { c->dc[i].buf = dc; dc += 0x800; }
    dirent_reset(c);

    if (share) {
        if (!(c->data_buf = S.databuf)) return FAT_ENOMEM;
    } else {
        snprintf(nm, sizeof(nm), "%sDataBuf", c->obj_name);
        if (!(c->data_buf = fatfs_alloc_block(&c->databuf_fpl, 0x200, nm))) {
            sceKernelDeleteFpl(c->fatcache_fpl); sceKernelDeleteFpl(c->dirent_fpl); sceKernelDeleteFpl(c->fdbuf_fpl);
            return FAT_ENOMEM;
        }
        if (c->vol_type == 2) S.databuf = c->data_buf;
    }
    c->media_flags = 0;
    c->fe.next_free = 2;
    return 0;
}
static int ef_exit(fatms_ctx *c)                                        /* 0xd1c0 */
{
    if (emu_ms(c)) return 0;                                             /* fatef owns what it uses */
    if (c->flags & ST_OPEN) ef_close(c);
    sceKernelDeleteFpl(c->fatcache_fpl);
    sceKernelDeleteFpl(c->dirent_fpl);
    sceKernelDeleteFpl(c->databuf_fpl);
    sceKernelDeleteFpl(c->fdbuf_fpl);
    return 0;
}
static int ef_start(fatms_ctx *c)                                       /* 0xd258: insert, at once */
{
    c->media_flags |= MF_INSERTED;
    fatfs_set_flags(c, ST_PRESENT, 1);
    c->assign_result = FAT_EBADBPB;
    int r = ef_open(c);
    if (r >= 0) {
        if ((r = ef_assign(c)) < 0) ef_close(c);
        else r = 0;
    }
    if (!(c->flags & ST_SUSPEND)) fatfs_notify_all(c, c->assign_result);
    return r;
}
static int ef_mount(fatms_ctx *c, const char *fs, const char *blockdev, unsigned flags)   /* 0xd36c */
{
    (void)fs;
    int r;
    if (!(c->flags & ST_MOUNTED) && (r = c->ops->identify(c)) < 0) return r;
    const char *want = emu_ms(c) ? "msstoremu0:" : c->name_msstor0p1;
    if (strcmp(want, blockdev) != 0) return FAT_EMOUNTED;
    if ((c->flags & ST_RDONLY_SW) && !(flags & 1)) return -0x7ffeffff;
    c->mount_count++;
    fatfs_set_flags(c, ST_MOUNTED, 1);
    return 0;
}
static int ef_rescan(fatms_ctx *c)                                      /* 0xd4a0: eject + insert, in place */
{
    c->media_flags = (c->media_flags & 0x9000) | MF_REMOVED;
    fatfs_set_flags(c, ST_PRESENT, 0);
    ef_unassign(c);
    fatfs_set_flags(c, ST_PRESENT, 1);
    c->media_flags |= MF_INSERTED;
    ef_assign(c);
    if (!(c->flags & ST_SUSPEND)) fatfs_notify_all(c, c->assign_result);
    return 0;
}
static int ef_identify(fatms_ctx *c)                                    /* 0xd60c: ba94 without the MS ioctls */
{
    u8 *buf = fatfs_get_scratch(c);
    if (!buf) return FAT_ENOMEM;
    int r = FAT_ENODEV;
    if ((c->media_flags & MF_INSERTED) && !(c->flags & ST_OPEN)) goto out;
    if ((r = c->blk->read_sectors(c, buf, 0, 1)) < 0) goto out;
    c->exfat = 0;
    if ((r = fatfs_bpb_parse(c, buf)) < 0) goto out;
    c->ms_sectors_per_cluster = 1; c->ms_unit = 0xffff; c->ms_max_cluster = 0xffffffffu;
out:
    fatfs_put_scratch(c, buf);
    return r;
}

/* callbacks: notified at once with the assign state, no mscmhc0: probe (0xcdfc; MS: c770) */
static int ef_cb_register(fatms_ctx *c, SceUID cb)
{
    int st = sceKernelSuspendDispatchThread(), found = 0;
    for (int i = 0; i < 32; i++) {
        if (c->tbl150[i] != 0) continue;
        c->tbl150[i] = (u32)cb;
        found = 1;
        if (sceKernelNotifyCallback(cb, (c->flags & ST_ASSIGNED) ? 1 : c->assign_result) == -0x7ffdfe5f) c->tbl150[i] = 0;
        break;
    }
    sceKernelResumeDispatchThread(st);
    return found ? 0 : FAT_EINVAL;
}
/* device-level devctls (0xd848; MS: beb4): no write-protect switch, always writable */
static int ef_devctl(fatms_ctx *c, unsigned int cmd, void *in, int inlen, void *out, int outlen)
{
    (void)outlen;
    SceUID cb;
    int r;
    switch (cmd) {
    case 0x02415822:                                                     /* ca74 -> b478: unregister callback */
        if ((r = fatfs_cb_arg(in, inlen, &cb)) < 0) return r;
        return fatfs_cb_unregister(c, cb);
    case 0x0240D81E:                                                     /* rescan */
        return c->ops->rescan(c);
    case 0x02415821:                                                     /* c95c -> cdfc: register callback */
        if ((r = fatfs_cb_arg(in, inlen, &cb)) < 0) return r;
        return ef_cb_register(c, cb);
    case 0x02425823:                                                     /* cc94: assigned? */
        if (!out) return FAT_EINVAL;
        *(u32 *)out = (c->flags & ST_ASSIGNED) ? 1 : 0;
        return 0;
    case 0x02425824:                                                     /* cb84: write-protect switch: off */
        if (!out) return FAT_EINVAL;
        if (!(c->flags & ST_OPEN)) return FAT_ENODEV;
        *(u32 *)out = 0;
        return 0;
    case 0x02425825:                                                     /* cc30: writable */
        if (!out) return FAT_EINVAL;
        if (!(c->flags & ST_OPEN)) return FAT_ENODEV;
        *(u32 *)out = 1;
        return 0;
    }
    return FAT_EUNSUP;
}
/* sysevents (0xd93c; MS: bfa8). 0x400 drops the assign; 0x100002 brings it back, and when the
   event's record says the storage changed underneath (+4 -> +4 == 0x80000000) forgets the directory
   cache, kills open fds and forces a re-identify. */
static int ef_sysevent(fatms_ctx *c, int ev, void *param)
{
    if (ev == 0x400) { fatfs_set_flags(c, ST_ASSIGNED, 0); fatfs_notify_all(c, 2); return 0; }
    if (ev == 0x102 || ev == 0x10000) { fatfs_set_flags(c, ST_SUSPEND, ev == 0x102); return 0; }
    if (ev != 0x100002) return 0;
    const u32 *rec = param ? ((u32 *const *)param)[1] : 0;
    if (rec && rec[1] == 0x80000000u) {
        dirent_drop_all(c);
        if (!(c->flags & 0x400)) k_sceIoTerminateFd(c->name_fatms);
        c->media_flags |= MF_REMOVED | MF_INSERTED;
    }
    int arg = 1;
    if (!(c->flags & ST_ASSIGN_ERR)) fatfs_set_flags(c, ST_ASSIGNED, 1);
    else arg = c->assign_result;
    fatfs_notify_all(c, arg);
    return 0;
}

/* ---------------- format (0xd52c), XMB's "Format internal storage" ---------------- */
/* layout, as fatmsef keeps it and IdStorage key 0x48 stores it (fatef at +0x104, fateh at +0x184) */
typedef struct {
    u8  spc, zero_clusters, nfats, media;    /* zero_clusters: clusters cleared from sector 0 */
    u16 bps, reserved;
    u16 spt, heads;
    u16 rootent_or_fsinfo, fat16_or_backup;  /* FAT16: root entries, FAT size; FAT32: FSInfo, backup boot */
    u32 hidden, total;
    u32 fat32_size, root_cluster;            /* FAT32 only */
} ef_geom;
_Static_assert(sizeof(ef_geom) == 32, "fatmsef layout record");
static const u32 GEOM_EH[8]    = { 0xf8020540, 0x00020200, 0x00100020, 0x000f0200, 0x40, 0x0001dfc0, 0, 0 };
static const u32 GEOM_EF8[8]   = { 0xf8023b40, 0x00220200, 0x00ff003f, 0x00060001, 0x40, 0x00e5ddc0, 0x72f, 2 };
static const u32 GEOM_EF16[8]  = { 0xf8027840, 0x00440200, 0x00ff003f, 0x00060001, 0x40, 0x01d3dd80, 0xe9f, 2 };

static int ef_big(fatms_ctx *c)                                         /* 0xe65c: more than 8 GB of flash */
{
    u32 n[2] = { 0, 0 };
    return k_sceIoDevctl(c->name_msstor0p1, 0x602a102, 0, 0, n, 8) >= 0 && (n[1] != 0 || n[0] > 0x1000000);
}
/* 0xe26c: the unit's layout from IdStorage key 0x48 when its MD5 (over the record with its first 16
   bytes replaced by a fixed salt) matches those 16 bytes, else the built-in one */
static int ef_geometry(fatms_ctx *c, ef_geom *g)
{
    static const u8 salt[16] = { 0x58, 0xf4, 0xd3, 0xcc, 0x7f, 0x1c, 0x95, 0x01, 0x31, 0x75, 0xff, 0x1b, 0x6f, 0x3a, 0xeb, 0xb3 };
    u8 rec[0x200] __attribute__((aligned(4))), want[16], md5[16];
    memset(g, 0, sizeof(*g));
    if (sceIdStorageLookup(0x48, 0, rec, 0x200) >= 0) {
        memcpy(want, rec, 16);
        memcpy(rec, salt, 16);
        int same = sceKernelUtilsMd5Digest(rec, 0x200, md5) >= 0;
        for (int i = 0; same && i < 16; i++) same = md5[i] == want[i];
        if (same) {
            if (c->vol_type == 3) { memcpy(g, rec + 0x184, 24); return 0; }
            if (c->vol_type != 2) return FAT_EINVAL;
            memcpy(g, rec + 0x104, 32);
            if (g->total != 0x1d7de00) {
                if (g->total == 0x1d3ddc0 && g->reserved == 0x42) { g->reserved = 0x44; g->total = 0x1d3dd80; }
                goto model;
            }
        }
    }
    if (c->vol_type == 3) { memcpy(g, GEOM_EH, 32); return 0; }
    if (c->vol_type != 2) return FAT_EINVAL;
    if (!ef_big(c)) { memcpy(g, GEOM_EF8, 32); return 0; }
    memcpy(g, GEOM_EF16, 32);
model:
    if (sceKernelGetModel() == 5 && g->fat32_size == 0xe9f) g->fat32_size = 0xe9e;
    return 0;
}
static int ef_zero(fatms_ctx *c, const ef_geom *g)                      /* 0xdb50: the system area */
{
    SceUID fpl;
    u8 *z = (u8 *)fatfs_alloc_block(&fpl, g->spc << 9, "SceFatfsEfFormat");
    if (!z) return FAT_ENOMEM;
    memset(z, 0, (u32)g->spc << 9);
    int r = 0;
    for (u32 i = 0; i < g->zero_clusters && r == 0; i++) r = c->blk->write_sectors(c, z, i * g->spc, g->spc);
    int d = sceKernelDeleteFpl(fpl);
    return r != 0 ? r : (d > 0 ? 0 : d);
}
static u8 *boot_common(fatms_ctx *c, const ef_geom *g)                 /* dc44 / de00, the shared fields */
{
    u8 *b = (u8 *)c->data_buf;
    memset(b, 0, 0x200);
    b[0] = 0xeb; b[1] = 0x58; b[2] = 0x90;
    memset(b + 3, ' ', 8);
    put16(b + 0x0b, g->bps); b[0x0d] = g->spc; put16(b + 0x0e, g->reserved); b[0x10] = g->nfats;
    b[0x15] = g->media;
    put16(b + 0x18, g->spt); put16(b + 0x1a, g->heads); put32(b + 0x1c, g->hidden); put32(b + 0x20, g->total);
    b[0x1fe] = 0x55; b[0x1ff] = 0xaa;
    return b;
}
static int ef_boot16(fatms_ctx *c, const ef_geom *g)                    /* 0xdc44 */
{
    u8 *b = boot_common(c, g);
    put16(b + 0x11, g->rootent_or_fsinfo); put16(b + 0x16, g->fat16_or_backup);
    b[0x26] = 0x29;
    memset(b + 0x2b, ' ', 11);
    memcpy(b + 0x36, "FAT16   ", 8);
    return c->blk->write_sectors(c, b, 0, 1);
}
static int ef_boot32(fatms_ctx *c, const ef_geom *g)                    /* 0xde00: + the backup boot sector */
{
    u8 *b = boot_common(c, g);
    put32(b + 0x24, g->fat32_size); put32(b + 0x2c, g->root_cluster);
    put16(b + 0x30, g->rootent_or_fsinfo); put16(b + 0x32, g->fat16_or_backup);
    b[0x42] = 0x29;
    memset(b + 0x47, ' ', 11);
    memcpy(b + 0x52, "FAT32   ", 8);
    int r = c->blk->write_sectors(c, b, 0, 1);
    return r == 0 ? c->blk->write_sectors(c, b, g->fat16_or_backup, 1) : r;
}
static int ef_fsinfo(fatms_ctx *c, const ef_geom *g)                    /* 0xe034: free count unknown, next 3 */
{
    u8 *b = (u8 *)c->data_buf;
    memset(b, 0, 0x200);
    put32(b, 0x41615252); put32(b + 0x1e4, 0x61417272); put32(b + 0x1e8, 0xffffffff); put32(b + 0x1ec, 3);
    b[0x1fe] = 0x55; b[0x1ff] = 0xaa;
    int r = c->blk->write_sectors(c, b, g->rootent_or_fsinfo, 1);
    return r == 0 ? c->blk->write_sectors(c, b, (u32)g->rootent_or_fsinfo + g->fat16_or_backup, 1) : r;
}
static int ef_fat(fatms_ctx *c, const ef_geom *g)                       /* 0xe104 FAT16 / 0xe1b4 FAT32 */
{
    u8 *b = (u8 *)c->data_buf;
    u32 size;
    memset(b, 0, 0x200);
    if (c->vol_type == 3) { put16(b, 0xfff8); put16(b + 2, 0x7fff); size = g->fat16_or_backup; }
    else { put32(b, 0xfffffff8); put32(b + 4, 0x0fffffff); put32(b + 8, 0x0fffffff); size = g->fat32_size; }
    int r = c->blk->write_sectors(c, b, g->reserved, 1);
    return r == 0 ? c->blk->write_sectors(c, b, g->reserved + size, 1) : r;
}
static int ef_format(fatms_ctx *c, int arg)
{
    (void)arg;
    ef_geom g;
    int r = ef_geometry(c, &g);
    if (r < 0 || (r = ef_zero(c, &g)) != 0) return r;
    if ((r = c->vol_type == 3 ? ef_boot16(c, &g) : ef_boot32(c, &g)) != 0) return r;
    if (c->vol_type != 3 && (r = ef_fsinfo(c, &g)) != 0) return r;
    if ((r = ef_fat(c, &g)) == 0 && (r = c->ops->rescan(c)) > 0) r = 0;
    return r;
}

const vol_ops g_ef_ops = {
    ef_init, ef_exit, ef_start, ef_mount, fatfs_umount, ef_rescan,
    ef_format, ef_identify, ef_devctl, ef_sysevent,
};

fatms_ctx g_ctx_ef = {
    .name_ms0 = "ef0:", .name_fatms0 = "fatef0:", .name_msstor0p1 = "eflash0a0f0p0:",
    .name_fatms = "fatef", .obj_name = "SceFatfsEf", .vol_type = 2,
    .ops = &g_ef_ops, .blk = &fatfs_blk,
};
fatms_ctx g_ctx_eh = {
    .name_ms0 = "eh0:", .name_fatms0 = "fateh0:", .name_msstor0p1 = "eflash0a0f1p0:",
    .name_fatms = "fateh", .obj_name = "SceFatfsEh", .vol_type = 3,
    .ops = &g_ef_ops, .blk = &fatfs_blk,
};

/* ---------------- sceFatfs_driver: XMB (vshbridge) ---------------- */
int sceFatfs_driver_C8537E11(void)                                     /* format internal storage */
{
    fatms_ctx *c;
    if ((c = fatfs_find("fatef"))) ef_format(c, 0);
    if ((c = fatfs_find("fateh"))) ef_format(c, 0);
    return 0;
}
int sceFatfs_driver_5DEB8124(void)                                     /* take it back after USB mode */
{
    fatms_ctx *c;
    if ((c = fatfs_find("fatef"))) ef_open(c);
    if ((c = fatfs_find("fateh"))) ef_open(c);
    return 0;
}
int sceFatfs_driver_3991C1A9(void)                                     /* let go of it for USB mode */
{
    fatms_ctx *c;
    if ((c = fatfs_find("fatef"))) ef_close(c);
    if ((c = fatfs_find("fateh"))) ef_close(c);
    return 0;
}
