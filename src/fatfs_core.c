/* fatfs_core.c - Sony 6.61 fatms FATFS core: volume table, init, media thread, mount, block device,
 * per-op discipline. The Memory Stick's FATFS table (Sony 0xd040, g_ms_ops below):
 *   [0] init b4bc  [4] exit b690  [8] init2 b718  [0x10] mount b7d4  [0x14] umount b910
 *   [0x18] rescan b974  [0x1c] read aca8  [0x20] write b000  [0x24] blkdevctl ba10
 *   [0x28] identify ba94  [0x2c] device devctl beb4 (fatfs_devctl.c)  [0x30] sysevent bfa8
 * Sony keeps up to 4 volumes (one per driver); fatms has one, the PSP Go's fatmsef three. */
#include "fatms.h"
#include <pspsysevent.h>

static int  media_thread(SceSize arglen, void *argp);

/* module-wide state (Sony globals d2c0..d2f4) */
static int    g_init_done;    /* DAT_0000d2c0: volumes registered */
static fatms_ctx *g_redirect; /* DAT_0000d2c4: PSP Go, the emulated ms0: volume */
static SceUID g_ef_mutex;     /* DAT_0000d2c8: PSP Go, fatef's mutex (the emulated ms0: shares it) */
static fatms_ctx *g_vols[4];  /* DAT_0000d2cc */
static SceUID g_cp_fpl;       /* DAT_0000d2dc (SceFatfsCPTable) */
static SceUID g_heap;         /* DAT_0000d2f4 */

static u32 le16(const u8 *p) { return (u32)(p[0] | p[1] << 8); }
static u32 le32(const u8 *p) { return le16(p) | le16(p + 2) << 16; }
static u32 be16(const u8 *p) { return (u32)(p[0] << 8 | p[1]); }

/* scratch-sector pool (FUN_00004fcc/00005044): 0x800 blocks, 4 kept for reuse */
static int   g_scratch_n;       /* DAT_0000d2e0 */
static void *g_scratch[4];      /* DAT_0000d2e4 */
u8 *fatfs_get_scratch(fatms_ctx *c)
{
    (void)c; int st = sceKernelCpuSuspendIntr(); void *p;
    if (g_scratch_n < 1) p = sceKernelAllocHeapMemory(g_heap, 0x800);
    else p = g_scratch[--g_scratch_n];
    sceKernelCpuResumeIntr(st);
    return (u8 *)p;
}
void fatfs_put_scratch(fatms_ctx *c, void *b)
{
    (void)c; int st = sceKernelCpuSuspendIntr();
    if (g_scratch_n < 4) g_scratch[g_scratch_n++] = b;
    else sceKernelFreeHeapMemory(g_heap, b);
    sceKernelCpuResumeIntr(st);
}

/* FUN_0000b398: notify every registered insert/eject callback; drop stale uids */
void fatfs_notify_all(fatms_ctx *c, int arg)
{
    int st = sceKernelSuspendDispatchThread();
    for (int i = 0; i < 32; i++)
        if (c->tbl150[i] != 0 && sceKernelNotifyCallback((SceUID)c->tbl150[i], arg) == -0x7ffdfe5f)
            c->tbl150[i] = 0;
    sceKernelResumeDispatchThread(st);
}

/* FUN_000059a4: validate the parsed BPB */
static int fatfs_validate_bpb(fatms_ctx *c)
{
    u8 mt = c->media_type;
    if (c->bytes_per_sector != 0x200 || c->sectors_per_cluster == 0 || c->reserved_sectors == 0) return 0x8001000d;
    if (c->root_entries >= 0x201 || c->num_fats == 0) return 0x8001000d;
    if (!(mt == 0xf0 || mt >= 0xf8)) return 0x8001000d;
    if (c->total_sectors == 0 || c->fat_size == 0) return 0x8001000d;
    return 0;
}
/* FUN_00005618: data-region start, cluster shift, max cluster, FAT window (min(fat size, 6)) */
static void fatfs_calc_layout(fatms_ctx *c)
{
    u32 fatregion = (u32)c->num_fats * c->fat_size + c->reserved_sectors;
    if (c->root_entries == 0) { c->root_dir_lba = 0; c->data_start_lba = fatregion; }
    else { c->root_dir_lba = fatregion; c->data_start_lba = fatregion + c->root_entries / (c->bytes_per_sector >> 5); }
    u32 spc = c->sectors_per_cluster, sh = 0;
    while (spc && !(spc & 1)) { spc >>= 1; sh++; }
    c->cluster_shift = sh;
    c->fat_max_cluster = ((c->total_sectors - c->data_start_lba) >> sh) + 2;
    /* DratiniFS: never past the FAT's own end. The old ES PC tool's FAT32 format made a FAT too small
       for its data area; the free-space scan (XMB at boot) then looped at the FAT's end forever. */
    u32 fat_entries = c->fat_type == 32 ? c->fat_size * 128 : c->fat_type == 16 ? c->fat_size * 256 : c->fat_size * 1024 / 3;
    if (c->fat_max_cluster > fat_entries) c->fat_max_cluster = fat_entries;
    c->fat_window_sectors = c->fat_size < c->fat_cache_count ? c->fat_size : c->fat_cache_count;
}
/* FUN_0000586c: FAT12 iff the data region holds < 0xff6 clusters; erase block must be 2^n <= 0x80 */
static void fatfs_detect_type(fatms_ctx *c)
{
    if (c->root_entries == 0) c->fat_type = 32;
    else {
        u32 clusters = (c->total_sectors - c->reserved_sectors - c->fat_size * c->num_fats -
                        c->root_entries / (c->bytes_per_sector >> 5)) / c->sectors_per_cluster;
        if (clusters < 0xff6) { c->fat_type = 12; c->ms_sectors_per_cluster = 1; }
        else c->fat_type = 16;
    }
    u32 ok = 0;
    for (u32 b = 0x80; b; b >>= 1) if (c->ms_sectors_per_cluster == b) ok = 1;
    if (!ok) c->ms_sectors_per_cluster = 1;
}

/* vtable+0x1c (0xaca8): read sectors. Word-aligned buffers that are not 64-byte aligned bounce
   their first and last sector through DataBuf and DMA the middle; unaligned buffers bounce all. */
static int blk_read_sectors(fatms_ctx *c, void *buf, u32 lba, u32 nsec)
{
    if (!(c->flags & ST_OPEN)) return FAT_ENODEV;
    SceOff s = k_sceIoLseek64k(c->ctrl_fd, (SceOff)lba * 0x200, 0);
    if (s < 0) return (int)s;
    u8 *p = (u8 *)buf; int n = (int)nsec, r;
    if (((u32)p & 3) == 0) {
        if ((u32)p & 0x3f) {
            if ((r = k_sceIoRead(c->ctrl_fd, c->data_buf, 0x200)) < 0) return r;
            memcpy(p, c->data_buf, 0x200); sceKernelDcacheWritebackInvalidateRange(p, 0x200);
            p += 0x200; n -= 2;
        }
        while (n > 0) {
            int ch = n > 0x200 ? 0x200 : n;
            n -= ch;
            r = k_sceIoRead(c->ctrl_fd, p, ch * 0x200);
            p += ch * 0x200;
            if (r < 0) return r;
        }
        if (n == 0 && ((u32)p & 0x3f)) {
            if ((r = k_sceIoRead(c->ctrl_fd, c->data_buf, 0x200)) < 0) return r;
            memcpy(p, c->data_buf, 0x200); sceKernelDcacheWritebackInvalidateRange(p, 0x200);
        }
        return 0;
    }
    for (int i = 0; i < n; i++, p += 0x200) {
        if ((r = k_sceIoRead(c->ctrl_fd, c->data_buf, 0x200)) < 0) return r;
        memcpy(p, c->data_buf, 0x200); sceKernelDcacheWritebackInvalidateRange(p, 0x200);
    }
    return 0;
}
/* vtable+0x20 (0xb000): write sectors, same bounce rules */
static int blk_write_sectors(fatms_ctx *c, const void *buf, u32 lba, u32 nsec)
{
    if (!(c->flags & ST_OPEN)) return FAT_ENODEV;
    SceOff s = k_sceIoLseek64k(c->ctrl_fd, (SceOff)lba * 0x200, 0);
    if (s < 0) return (int)s;
    const u8 *p = (const u8 *)buf; int n = (int)nsec, r;
    if (((u32)p & 3) == 0) {
        if ((u32)p & 0x3f) {
            memcpy(c->data_buf, p, 0x200);
            if ((r = k_sceIoWrite(c->ctrl_fd, c->data_buf, 0x200)) < 0) return r;
            p += 0x200; n -= 2;
        }
        while (n > 0) {
            int ch = n > 0x200 ? 0x200 : n;
            n -= ch;
            r = k_sceIoWrite(c->ctrl_fd, p, ch * 0x200);
            p += ch * 0x200;
            if (r < 0) return r;
        }
        if (n != 0 || !((u32)p & 0x3f)) return 0;
        memcpy(c->data_buf, p, 0x200);
        return (r = k_sceIoWrite(c->ctrl_fd, c->data_buf, 0x200)) < 0 ? r : 0;
    }
    for (int i = 0; i < n; i++, p += 0x200) {
        memcpy(c->data_buf, p, 0x200);
        if ((r = k_sceIoWrite(c->ctrl_fd, c->data_buf, 0x200)) < 0) return r;
    }
    return 0;
}
fatms_blockdev fatfs_blk = { blk_read_sectors, blk_write_sectors };

/* FAT-cache engine block callbacks + geometry sync */
static int eng_read(void *owner, void *buf, u32 lba, u32 n) { return blk_read_sectors((fatms_ctx *)owner, buf, lba, n); }
static int eng_write(void *owner, const void *buf, u32 lba, u32 n) { return blk_write_sectors((fatms_ctx *)owner, buf, lba, n); }
static void fatfs_engine_sync(fatms_ctx *c)
{
    fat_engine *e = &c->fe;
    e->bps = c->bytes_per_sector;
    e->ms_block = c->ms_sectors_per_cluster ? c->ms_sectors_per_cluster : 1;
    e->reserved = c->reserved_sectors;
    e->num_fats = c->num_fats;
    e->fat_size = c->fat_size;
    e->max_cluster = c->fat_max_cluster;
    e->tflags = (c->fat_type == 32) ? TF_FAT32 : (c->fat_type == 16) ? TF_FAT16 : 0;
    e->cache = (u8 *)c->fat_cache;
    e->window_sectors = c->fat_window_sectors ? c->fat_window_sectors : 1;
    e->owner = c;
    e->read = eng_read;
    e->write = eng_write;
    e->is_reserved = 0;                 /* FUN_00002a28 is dead in 6.61: slot +0x638 is never set */
    fat_window_reset(e);
}

/* media insert: open the block device (FUN_0000c0a4). Sony never sets ST_RDONLY_SW here: its
   check compares the open mode (0x4000001/3) with 1, which never matches. */
static void media_on_insert(fatms_ctx *c)
{
    if (c->flags & ST_OPEN) return;
    int fd = k_sceIoOpen(c->name_msstor0p1, 0x4000003, 0);
    if (fd < 0) fd = k_sceIoOpen(c->name_msstor0p1, 0x4000001, 0);
    if (fd < 0) {
        fatfs_set_flags(c, ST_WHOLEDISK, 1);
        fd = k_sceIoOpen(c->name_msstor0, 0x4000003, 0);
        if (fd < 0) fd = k_sceIoOpen(c->name_msstor0, 0x4000001, 0);
        if (fd < 0) return;
    }
    c->ctrl_fd = fd;
    fatfs_set_flags(c, ST_OPEN, 1);
    if (!(c->flags & ST_WHOLEDISK)) msstor_cap_insert(c);              /* DratiniFS: cards up to 2 TB */
}
/* media insert step 2: assign ms0: -> block device (FUN_0000c354) -> triggers IoMount */
static void media_post_insert(fatms_ctx *c)
{
    u32 f = c->flags;
    if (!(f & ST_OPEN) || (f & ST_ASSIGNED)) return;
    const char *src = (f & ST_WHOLEDISK) ? c->name_msstor0 : c->name_msstor0p1;
    int r = k_sceIoAssign(c->name_ms0, src, c->name_fatms0, (int)(((f & 0x3f) >> 5) | 0x80000000), 0, 0);
    if (r == 0) { c->assign_count++; fatfs_set_flags(c, ST_ASSIGNED, 1); r = 1; }
    c->assign_result = r;
    if (!(c->flags & ST_SUSPEND)) fatfs_notify_all(c, c->assign_result);
}
/* media eject: unassign (FUN_0000c430) */
static void media_on_eject(fatms_ctx *c)
{
    u32 f = c->flags;
    if ((f & ST_SUSPEND) && !(f & ST_ASSIGNED)) return;
    int r = k_sceIoUnassign(c->name_ms0);
    if (r != 0 && r != -0x7ffdfcdf) return;
    if (r == 0 && (int)--c->assign_count < 1) { fatfs_set_flags(c, ST_ASSIGNED, 0); c->assign_count = 0; }
    if (!(c->flags & 0x400)) k_sceIoTerminateFd(c->name_fatms);
    for (int i = 0; i < FAT_NUM_FD; i++) ((OpenFile *)c->fd[i])->inuse = 0;
    fatfs_notify_all(c, 2);
}
/* media eject step 2: close the block device (FUN_0000c1b8) */
static void media_cleanup(fatms_ctx *c)
{
    if (c->flags & ST_OPEN) {
        k_sceIoClose(c->ctrl_fd);
        fatfs_set_flags(c, ST_OPEN, 0);
        fatfs_set_flags(c, ST_WHOLEDISK, 0);
        c->ctrl_fd = 0;
    }
}

/* ===== the static volume context (Sony ctx@0xd088) ===== */
fatms_ctx g_ctx = {
    .name_ms0 = "ms0:", .name_fatms0 = "fatms0:",
    .name_msstor0p1 = "msstor0p1:", .name_msstor0 = "msstor0:",
    .name_fatms = "fatms", .obj_name = "SceFatfsMs",
    .ops = &g_ms_ops, .blk = &fatfs_blk,
};

/* ===== sysevents: "SceFatfsSysEvent" (FUN_000045e0) calls every volume's vtable+0x30 =====
 * Memory Stick (fatfs_vt30): 0x102 (suspending) sets ST_SUSPEND, which mutes insert notifies and
 * skips unassign of an unassigned ms0: during the sleep power-down; 0x10000 (resumed) clears it.
 * 0x100002 is the "MS busy?" query: refuse (0x80000021) while mscmhc0 is mid-insert, then for 10
 * more polls. */
static int ms_sysevent(fatms_ctx *c, int ev_id, void *param)
{
    (void)param;
    if (ev_id == 0x102 || ev_id == 0x10000) { fatfs_set_flags(c, ST_SUSPEND, ev_id == 0x102); return 0; }
    if (ev_id == 0x100000) { c->unk1e4 = 0; return 0; }
    if (ev_id != 0x100002) return 0;
    int st[4] = { 0 };
    if (k_sceIoDevctl("mscmhc0:", 0x02025801, 0, 0, st, 4) < 0) return 0;
    if (st[0] == 3) { c->unk1e4 = 10; return (int)0x80000021; }
    if ((int)c->unk1e4 > 0) c->unk1e4--;
    return (c->in_insert == 0 && c->unk1e4 == 0) ? 0 : (int)0x80000021;
}
static int fatfs_sysevent(int ev_id, char *ev_name, void *param, int *result)      /* FUN_000045e0 */
{
    (void)ev_name; (void)result;
    for (int i = 0; i < 4; i++) {
        fatms_ctx *c = g_vols[i];
        int r;
        if (c && (r = c->ops->sysevent(c, ev_id, param)) < 0) return r;
    }
    return 0;
}
static PspSysEventHandler g_sysev = {
    .size = sizeof(PspSysEventHandler), .name = "SceFatfsSysEvent",
    .type_mask = 0x00FFFF00, .handler = fatfs_sysevent,
};

/* ===== flag helper (FUN_00005bb4): atomic set/clear of ctx.flags ===== */
void fatfs_set_flags(fatms_ctx *c, u32 mask, int set)
{
    int st = sceKernelCpuSuspendIntr();
    if (set) c->flags |= mask; else c->flags &= ~mask;
    sceKernelCpuResumeIntr(st);
}

/* FUN_00005978: identified + valid BPB */
int fatfs_ready(fatms_ctx *c)
{
    return ((c->media_flags & MF_OK) && c->bytes_per_sector != 0) ? 0 : (int)0x8001000d;
}

/* FUN_0000592c: validate an fd handle = one of the 10 slots and in use */
int fatfs_fd_valid(fatms_ctx *c, void *f)
{
    for (int i = 0; i < FAT_NUM_FD; i++)
        if (c->fd[i] == f) return ((OpenFile *)f)->inuse != 0 ? 0 : (int)0x80010009;
    return (int)0x80010009;
}

/* FUN_000056fc: DirentCache then FAT window */
int fatfs_flush(fatms_ctx *c)
{
    int r = dirent_flush(c);
    if (r >= 0 && (r = fat_flush(&c->fe)) > 0) r = 0;
    return r;
}

/* identify's boot-sector part, the same in ba94 and fatmsef d60c: 0, or FAT_EBADBPB */
int fatfs_bpb_parse(fatms_ctx *c, const u8 *buf)
{
    c->bytes_per_sector = (u16)le16(buf + 0x0b);
    if (c->bytes_per_sector != 0x200) return FAT_EBADBPB;
    c->sectors_per_cluster = buf[0x0d];
    if (!c->sectors_per_cluster) return FAT_EBADBPB;
    c->reserved_sectors = (u16)le16(buf + 0x0e);
    c->root_entries = (u16)le16(buf + 0x11);
    c->num_fats = buf[0x10];
    if (!c->num_fats) return FAT_EBADBPB;
    c->media_type = buf[0x15];
    c->total_sectors = le16(buf + 0x13) ? le16(buf + 0x13) : le32(buf + 0x20);
    if (!c->total_sectors) return FAT_EBADBPB;
    if (c->root_entries == 0) {
        c->fat_size = le32(buf + 0x24); c->root_cluster = le32(buf + 0x2c);
        c->fsinfo_sector = (u16)le16(buf + 0x30); c->backup_boot = (u16)le16(buf + 0x32);
    } else {
        c->fat_size = le16(buf + 0x16);
        c->root_cluster = 0; c->fsinfo_sector = 0; c->backup_boot = 0;
    }
    return 0;
}
/* vtable+0x28 (0xba94): parse the BPB from sector 0, then the MS attribute (erase block) and
   writability ioctls on the block device */
static int fatfs_identify(fatms_ctx *c)
{
    u8 *buf = fatfs_get_scratch(c);
    if (!buf) return FAT_ENOMEM;
    int r = FAT_ENODEV;
    if ((c->media_flags & MF_INSERTED) && !(c->flags & ST_OPEN)) goto out;
    if ((r = c->blk->read_sectors(c, buf, 0, 1)) < 0) goto out;
    c->exfat = 0;
    if ((r = exfat_identify(c, buf)) <= 0) goto out;                     /* DratiniFS: exFAT boot sector */
    if ((r = fatfs_bpb_parse(c, buf)) < 0) goto out;
    if (k_sceIoIoctl(c->ctrl_fd, 0x2125803, 0, 0, buf, 0x60) < 0) {     /* no MS attribute data */
        c->ms_sectors_per_cluster = 1; c->ms_unit = 0xffff; c->ms_max_cluster = 0xffffffffu;
        r = 0; goto out;
    }
    u32 ms = 1;
    if (buf[0] == 2) ms = (u16)((be16(buf + 2) * be16(buf + 0x2c) / c->bytes_per_sector) / c->sectors_per_cluster);
    c->ms_sectors_per_cluster = (u16)ms;
    if (buf[0] == 2 && ms) {
        u32 a = be16(buf + 0x2a), unit = a / c->sectors_per_cluster + (a % c->sectors_per_cluster != 0);
        c->ms_unit = (u16)((ms == unit || (ms == 1 && unit == 2)) ? unit : 0xffff);
        u32 ds = (u32)c->num_fats * c->fat_size + c->reserved_sectors;
        if (c->root_entries) ds += c->root_entries / (c->bytes_per_sector >> 5);
        u32 spc = c->sectors_per_cluster, sh = 0;
        while (spc && !(spc & 1)) { spc >>= 1; sh++; }
        c->ms_max_cluster = ((((c->total_sectors - ds) >> sh) + 2) / ms) * ms;
    } else { c->ms_unit = 0xffff; c->ms_max_cluster = 0xffffffffu; }
    int v[4] = { 0 };
    if (k_sceIoIoctl(c->ctrl_fd, 0x2125008, 0, 0, v, 4) == 0 && v[0] == 0) c->media_flags |= MF_WP;
    else c->media_flags &= ~MF_WP;
    r = 0;
out:
    fatfs_put_scratch(c, buf);
    return r;
}

/* FUN_0000573c: per-op prep. Media removed -> drop FAT state + fds; inserted -> forget the
   DirentCache and re-identify (Sony re-reads the BPB on the first op after every insert). */
int fatfs_prep(fatms_ctx *c)
{
    u32 mf = c->media_flags;
    if (mf & MF_REMOVED) {
        c->media_flags = mf & MF_INSERTED;
        fat_window_reset(&c->fe);                                        /* window, dirty, free counts, hint */
        for (int i = 0; i < FAT_NUM_FD; i++) ((OpenFile *)c->fd[i])->inuse = 0;
        mf = c->media_flags;
    }
    if (!(mf & MF_INSERTED)) return 0;
    dirent_reset(c);
    c->media_flags = 0;
    int r = c->ops->identify(c);
    if (r >= 0 && c->exfat) {                                            /* DratiniFS: exFAT */
        c->fat_window_sectors = c->fat_size < c->fat_cache_count ? c->fat_size : c->fat_cache_count;
        fatfs_engine_sync(c);
        if ((r = exfat_mount(c)) < 0) { c->media_flags |= MF_BAD; return r; }
        c->media_flags |= MF_OK;
        es_load(c);
        return 0;
    }
    if (r < 0 || (r = fatfs_validate_bpb(c)) < 0) { c->media_flags |= MF_BAD; return r; }
    c->media_flags |= MF_OK;
    fatfs_detect_type(c);
    fatfs_calc_layout(c);
    fatfs_engine_sync(c);                                                /* next-free hint = 2 */
    es_load(c);                                                          /* DratiniFS: an ES partition */
    return 0;
}

/* FUN_0000b338: clear the +0x150 table (+ clear ST_ASSIGNED) under intr lock */
void fatfs_clear_tbl150(fatms_ctx *c)
{
    int st = sceKernelCpuSuspendIntr();
    fatfs_set_flags(c, ST_ASSIGNED, 0);
    for (int i = 0; i < 32; i++) c->tbl150[i] = 0;
    sceKernelCpuResumeIntr(st);
}

/* FUN_00004f04: create a 1-block Fpl and allocate the block; returns ptr (or 0) */
void *fatfs_alloc_block(SceUID *out_fpl, int size, const char *name)
{
    SceUID fpl = sceKernelCreateFpl(name, 1, 0x101, size, 1, 0);
    if (fpl > 0) {
        void *blk = 0;
        *out_fpl = fpl;
        if (sceKernelTryAllocateFpl(fpl, &blk) >= 0) return blk;
        sceKernelDeleteFpl(fpl);
    }
    *out_fpl = 0;
    return 0;
}

/* vtable[0] (0xb4bc): allocate FS buffers + create the media thread */
static int fatfs_init(fatms_ctx *c)
{
    char nm[32];
    snprintf(nm, sizeof(nm), "%sFdBuf", c->obj_name);
    u8 *fdbuf = (u8 *)fatfs_alloc_block(&c->fdbuf_fpl, FAT_NUM_FD * FAT_FD_SIZE, nm);
    if (!fdbuf) return FAT_ENOMEM;
    for (int i = 0; i < FAT_NUM_FD; i++, fdbuf += FAT_FD_SIZE) { c->fd[i] = fdbuf; ((OpenFile *)fdbuf)->inuse = 0; }
    fatfs_clear_tbl150(c);

    snprintf(nm, sizeof(nm), "%sFatCache", c->obj_name);
    void *fc = fatfs_alloc_block(&c->fatcache_fpl, 0xc00, nm);
    if (!fc) { sceKernelDeleteFpl(c->fdbuf_fpl); return FAT_ENOMEM; }
    c->fat_cache = fc;
    c->fat_cache_tag = 0xffffffff;
    c->fat_cache_count = 6;

    snprintf(nm, sizeof(nm), "%sDirentCache", c->obj_name);
    u8 *dc = (u8 *)fatfs_alloc_block(&c->dirent_fpl, FAT_NUM_DIRENT * 0x800, nm);
    if (!dc) { sceKernelDeleteFpl(c->fatcache_fpl); sceKernelDeleteFpl(c->fdbuf_fpl); return FAT_ENOMEM; }
    for (int i = 0; i < FAT_NUM_DIRENT; i++) { c->dc[i].buf = dc; dc += 0x800; }
    dirent_reset(c);

    snprintf(nm, sizeof(nm), "%sDataBuf", c->obj_name);
    c->data_buf = fatfs_alloc_block(&c->databuf_fpl, 0x200, nm);
    if (!c->data_buf) {
        sceKernelDeleteFpl(c->dirent_fpl); sceKernelDeleteFpl(c->fatcache_fpl); sceKernelDeleteFpl(c->fdbuf_fpl);
        return FAT_ENOMEM;
    }
    c->fe.next_free = 2;
    c->media_flags = 0;
    SceUID th = sceKernelCreateThread("SceFatmsMedia", media_thread, 0x10, 0x800, 0x100001, 0);
    if (th < 0) return th;
    c->media_thread = th;
    return 0;
}

/* vtable[4] (0xb690): tear down on driver removal */
int fatfs_ioexit(fatms_ctx *c)
{
    sceKernelDeleteThread(c->media_thread);
    media_cleanup(c);
    sceKernelDeleteFpl(c->fatcache_fpl);
    sceKernelDeleteFpl(c->dirent_fpl);
    sceKernelDeleteFpl(c->databuf_fpl);
    sceKernelDeleteFpl(c->fdbuf_fpl);
    return 0;
}

/* vtable[8] (0xb718): start the media thread */
static int fatfs_init2(fatms_ctx *c)
{
    void *arg = c;
    int r = sceKernelStartThread(c->media_thread, 4, &arg);
    return r > 0 ? 0 : r;
}

/* media thread (FUN_0000c220): insert/eject detect loop */
static int media_thread(SceSize arglen, void *argp)
{
    (void)arglen;
    fatms_ctx *c = *(fatms_ctx **)argp;
    c->detect_evf = sceKernelCreateEventFlag("SceFatfsDetectMedium", 1, 0, 0);
    if (c->detect_evf < 0) return c->detect_evf;
    int r = k_sceIoDevctl("mscmhc0:", 0x203d802, &c->detect_evf, 4, &c->detect_arg, 4);
    if (r < 0) return r;
    for (;;) {
        u32 bits = 0;
        sceKernelWaitEventFlag(c->detect_evf, 3, 0x21 /* OR | CLEAR */, &bits, 0);
        if (bits & 1) {                                                   /* insert */
            fatfs_set_flags(c, ST_PRESENT, 1);
            c->in_insert = 1;
            c->media_flags |= MF_INSERTED;
            media_on_insert(c);
            media_post_insert(c);
            c->in_insert = 0;
            c->unk1e4 = 0;
        }
        if (bits & 2) {                                                   /* eject */
            c->media_flags = (c->media_flags & 0x9000) | MF_REMOVED;
            fatfs_set_flags(c, ST_PRESENT, 0);
            media_on_eject(c);
            media_cleanup(c);
        }
    }
}

/* vtable+0x18 (0xb974): if mscmhc0 reports media, replay eject+insert -> full remount */
int fatfs_rescan(fatms_ctx *c)
{
    int st = sceKernelCpuSuspendIntr(), v[4] = { 0 };
    int r = k_sceIoDevctl("mscmhc0:", 0x2025806, 0, 0, v, 4);
    if (r < 0 || v[0] == 1) {
        sceKernelSetEventFlag(c->detect_evf, 2);
        sceKernelSetEventFlag(c->detect_evf, 1);
    }
    sceKernelCpuResumeIntr(st);
    return 0;
}

/* vtable+0x24 (0xba10) */
int fatfs_blkdevctl(fatms_ctx *c, int enable)
{
    u32 arg = (enable != 0);
    int r = k_sceIoDevctl(c->name_msstor0, 0x211d814, &arg, 4, 0, 0);
    if (r >= 0 && (r = k_sceIoDevctl(c->name_msstor0, 0x210d816, 0, 0, 0, 0)) > 0) r = 0;
    return r;
}

/* vtable+0x10 (0xb7d4) */
int fatfs_mount(fatms_ctx *c, const char *fs_name, const char *blockdev, unsigned flags)
{
    (void)fs_name;
    u32 f = c->flags;
    if (!(f & ST_MOUNTED)) {
        int tmp = 0;
        int r = k_sceIoIoctl(c->ctrl_fd, 0x2125001, 0, 0, &tmp, 4);
        if (r < 0 || tmp == 0) return FAT_ENODEV;
        if ((r = c->ops->identify(c)) < 0) return r;
        f = c->flags;
    }
    int cmp;
    if (!(f & ST_WHOLEDISK)) {
        if (strlen(blockdev) > 0x3f) return FAT_EINVAL;
        cmp = strncmp(c->name_msstor0p1, blockdev, strlen(c->name_msstor0p1));
    } else cmp = strcmp(c->name_msstor0, blockdev);
    if (cmp != 0) return FAT_EMOUNTED;
    if ((c->flags & ST_RDONLY_SW) && !(flags & 1)) return -0x7ffeffff;
    c->mount_count++;
    fatfs_set_flags(c, ST_MOUNTED, 1);
    return 0;
}

/* vtable+0x14 (0xb910) */
int fatfs_umount(fatms_ctx *c)
{
    if (!(c->flags & ST_MOUNTED)) return (int)0x80000020;
    if ((int)--c->mount_count < 1) { fatfs_set_flags(c, ST_MOUNTED, 0); c->assign_count = 0; }
    return 0;
}

/* module_reboot_phase (0x4524, syslib NID 0xADF12745). loadexec calls phase 3, 2, then suspends
   every user thread, then phase 1. Holding the volume mutexes across that window means no user
   thread is frozen halfway through a filesystem op (with a mutex held) while the reboot goes on to
   load the next executable through this driver. The mutexes are recursive (attr 0x301). */
int module_reboot_phase(int phase, void *param, int a3, int a4)
{
    (void)param; (void)a3; (void)a4;
    for (int i = 0; i < 4; i++) {
        if (!g_vols[i]) continue;
        if (phase == 2) sceKernelLockMutex(g_vols[i]->mutex, 1, 0);
        else if (phase == 1) sceKernelUnlockMutex(g_vols[i]->mutex, 1);
    }
    return 0;
}

/* the volume table: FUN_00002c44 adds (0x80010010 when full), FUN_00002c94 finds by driver name */
static int vol_add(fatms_ctx *c)
{
    for (int i = 0; i < 4; i++) if (!g_vols[i]) { g_vols[i] = c; g_init_done++; return 0; }
    return -0x7ffefff0;
}
fatms_ctx *fatfs_find(const char *drvname)
{
    for (int i = 0; i < 4; i++) if (g_vols[i] && strcmp(drvname, g_vols[i]->name_fatms) == 0) return g_vols[i];
    return 0;
}
fatms_ctx *fatfs_redirect(void) { return g_redirect; }

/* fatms_IoInit (0x2ea0): the driver's volume gets its mutex and buffers. On the PSP Go, when ms0:
   is internal storage (vol_emulated), the fatms volume shares fatef's mutex and becomes the target
   of the redirected IoMount/IoUmount (main.c). */
int fatfs_ioinit(PspIoDrvArg *arg)
{
    fatms_ctx *c = fatfs_find(arg->drv->name);
    if (!c) return FAT_ENODEV;
    SceUID m;
    if (vol_emulated() && c->vol_type == 0) {
        if (!g_ef_mutex) return FAT_EINVAL;
        c->mutex = m = g_ef_mutex;
        g_redirect = c;
    } else {
        if ((m = sceKernelCreateMutex(c->obj_name, 0x301, 0, 0)) < 0) return m;
        c->mutex = m;
        if (c->vol_type != 2) goto init;
    }
    g_ef_mutex = m;
init:;
    int r = c->ops->init(c);
    if (r < 0) { sceKernelDeleteMutex(c->mutex); return r; }
    ((void **)arg)[1] = c;      /* driverArg[1] = ctx */
    return 0;
}

/* fatms_init_and_addDrv (0x2d08): module_start, once per driver */
int fatfs_register(PspIoDrv *drv, fatms_ctx *c)
{
    int r, a = 0, b = 0, cc = 0, d = 0;
    if (g_init_done == 0) {
        for (int i = 0; i < 4; i++) g_vols[i] = 0;
        if ((r = sceCodepage_driver_EE932176()) < 0) return r;
        if ((r = sceCodepage_driver_1D0DE569(&a, &b, &cc, &d)) != 0) return r;
        if (a != 0 || b != 0) return FAT_ENOMEM;
        u8 *t = (u8 *)fatfs_alloc_block(&g_cp_fpl, ((cc + 0x3f) & ~0x3f) + ((d + 0x3f) & ~0x3f), "SceFatfsCPTable");
        if (!t) return FAT_ENOMEM;
        /* hand the codepage driver its four tables, packed 64-byte aligned in that block (asm
           0x2de0..0x2e14: a0..a3,t0..t3; Ghidra drops these arguments). Sizes a/b are 0 here. */
        u8 *t1 = t + ((a + 0x3f) & ~0x3f), *t2 = t1 + ((b + 0x3f) & ~0x3f), *t3 = t2 + ((cc + 0x3f) & ~0x3f);
        if ((r = sceCodepage_driver_039BF9E9(t, a, t1, b, t2, cc, t3, d)) != 0) return r;
        if ((r = sceKernelRegisterSysEventHandler(&g_sysev)) < 0) return r;
    }
    if ((r = vol_add(c)) < 0) return r;
    g_heap = sceKernelCreateHeap(1, 0x4000, 1, c->obj_name);  /* FUN_00004f98: one per volume, the last kept */
    if ((r = k_sceIoAddDrv(drv)) < 0) return r;               /* -> fatms_IoInit -> fatfs_ioinit */
    return c->ops->start(c);                                  /* MS: the media thread; Go storage: insert */
}

const vol_ops g_ms_ops = {
    fatfs_init, fatfs_ioexit, fatfs_init2, fatfs_mount, fatfs_umount, fatfs_rescan,
    fatfs_blkdevctl, fatfs_identify, ms_devctl, ms_sysevent,
};
