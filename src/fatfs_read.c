/* fatfs_read.c - Sony 6.61 fatms data path shared by read + write, and the read worker.
 *   fatfs_walk       FUN_00004d90 (follow N links; error codes as Sony)
 *   fatfs_sector_io  FUN_000053f0 (bytes within one cluster; bounce partial sectors)
 *   fatfs_data_io    FUN_000050c0 + 53f0 loop of 86c4/8de8 (whole contiguous clusters in one call)
 *   fatfs_free_chain FUN_000047ec, fatfs_read FUN_000086c4
 * The per-fd checkpoint cache (one cluster per 0x400 links, Sony slot +0x424..) is kept as
 * ckpt[]/ckpt_count plus a forward cursor; same walks avoided, same results. */
#include "fatms.h"

#define CKPT_STEP 0x400u

u32 fatfs_clus2lba(fatms_ctx *c, u32 cl)                                /* FUN_000056e4 */
{
    return ((cl - 2) << c->cluster_shift) + c->data_start_lba;
}

int fatfs_walk(fatms_ctx *c, u32 cl, u32 n, u32 *out)                   /* FUN_00004d90 */
{
    if (cl < 2) return FAT_ENOSPC;
    for (u32 i = 0; i < n; i++) {
        u32 nx;
        if (fat_get(&c->fe, cl, &nx) < 0) {
            u32 lim = c->fat_type == 32 ? 0x0ffffff7u : c->fat_type == 16 ? 0xfff7u : 0xff7u;
            if (cl <= lim) return FAT_ENOSPC;                            /* broken link */
            *out = 0;
            return FAT_EINVAL;                                           /* walked past end-of-chain */
        }
        if (nx < 2) return FAT_ENOSPC;
        cl = nx;
    }
    *out = cl;                                                           /* may be the EOC value itself */
    return 0;
}

int fatfs_free_chain(fatms_ctx *c, u32 cl)                              /* FUN_000047ec */
{
    for (int n = 0; ; n++) {
        u32 nx;
        if (fat_get(&c->fe, cl, &nx) < 0) return 0;                      /* end of chain (or a free link) */
        int r = fat_set(&c->fe, cl, 0);
        if (r < 0) return r;
        if (n == 0) c->fe.next_free = cl;
        cl = nx;
    }
}

void fatfs_ckpt_record(OpenFile *f, u32 idx, u32 cl)
{
    if (idx == 0 || (idx & (CKPT_STEP - 1)) != 0) return;
    u32 j = idx / CKPT_STEP - 1;
    if (j < 128 && j == f->ckpt_count) { f->ckpt[j] = cl; f->ckpt_count = j + 1; }
}

/* cluster at index `cidx` of f's chain: start from the furthest cursor/checkpoint <= cidx */
int fatfs_seek_cluster(fatms_ctx *c, OpenFile *f, u32 cidx, u32 *out)
{
    u32 idx = 0, cl = f->first_cluster;
    if (f->cur_cluster >= 2 && f->cur_cluster_index <= cidx) { idx = f->cur_cluster_index; cl = f->cur_cluster; }
    u32 k = cidx / CKPT_STEP;
    if (k > f->ckpt_count) k = f->ckpt_count;
    if (k > 0 && k * CKPT_STEP > idx && f->ckpt[k - 1] >= 2) { idx = k * CKPT_STEP; cl = f->ckpt[k - 1]; }
    while (idx < cidx) {
        u32 nx;
        int r = fatfs_walk(c, cl, 1, &nx);
        if (r < 0) return r;
        cl = nx; idx++;
        if (cl >= c->fat_max_cluster) return FAT_ENOSPC;
        fatfs_ckpt_record(f, idx, cl);
    }
    if (cl < 2 || cl >= c->fat_max_cluster) return FAT_ENOSPC;
    f->cur_cluster = cl; f->cur_cluster_index = idx;
    *out = cl;
    return 0;
}

/* FUN_000053f0: move `len` bytes at byte `off` of the cluster starting at `lba` (clamped to the
   cluster end); partial sectors go through a scratch sector (read-modify-write when writing) */
int fatfs_sector_io(fatms_ctx *c, int write, u32 off, u32 len, u8 *buf, u32 lba, u32 *done)
{
    u32 bps = c->bytes_per_sector, soff = off % bps, total = 0;
    if (done) *done = 0;
    lba += off / bps;
    u32 room = bps * c->sectors_per_cluster - off;
    if (room < len) len = room;
    u32 chunk = (soff && bps - soff < len) ? bps - soff : len;
    while (total < len) {
        int r;
        if (chunk < bps) {
            u8 *sb = fatfs_get_scratch(c);
            if (!sb) return FAT_ENOMEM;
            r = c->blk->read_sectors(c, sb, lba, 1);
            if (r >= 0) {
                if (write) { memcpy(sb + soff, buf, chunk); r = c->blk->write_sectors(c, sb, lba, 1); }
                else memcpy(buf, sb + soff, chunk);
            }
            fatfs_put_scratch(c, sb);
            if (r < 0) return r;
            lba++;
        } else {
            u32 n = chunk / bps;
            r = write ? c->blk->write_sectors(c, buf, lba, n) : c->blk->read_sectors(c, buf, lba, n);
            if (r < 0) return r;
            chunk = n * bps;
            lba += n;
        }
        total += chunk; buf += chunk; soff = 0; chunk = len - total;
    }
    if (done) *done = total;
    return 0;
}

/* the 86c4/8de8 transfer loop from byte `pos` (cluster at index pos/cluster_bytes already seeked
   to `cl`): partial clusters via sector_io, whole clusters coalesced into contiguous runs */
int fatfs_data_io(fatms_ctx *c, OpenFile *f, int write, u8 *buf, u32 pos, u32 len, u32 cl, u32 *done)
{
    u32 cb = (u32)c->bytes_per_sector * c->sectors_per_cluster, off = pos % cb, cidx = pos / cb, total = 0;
    *done = 0;
    for (;;) {
        u32 got;
        if (off == 0 && len >= cb) {                                     /* FUN_000050c0: contiguous run */
            u32 n = 1, last = cl;
            while ((n + 1) * cb <= len) {
                u32 nx;
                if (fat_get(&c->fe, last, &nx) < 0 || nx != last + 1) break;
                last = nx; n++;
                fatfs_ckpt_record(f, cidx + n - 1, last);
            }
            u32 lba = fatfs_clus2lba(c, cl), ns = n * c->sectors_per_cluster;
            int r = write ? c->blk->write_sectors(c, buf, lba, ns) : c->blk->read_sectors(c, buf, lba, ns);
            if (r < 0) return r;
            got = n * cb; cl = last; cidx += n - 1;
        } else {
            int r = fatfs_sector_io(c, write, off, len, buf, fatfs_clus2lba(c, cl), &got);
            if (r < 0) return r;
        }
        buf += got; total += got; len -= got;
        f->cur_cluster = cl; f->cur_cluster_index = cidx;
        *done = total;
        if (len == 0) return 0;
        u32 nx;                                                          /* step into the next cluster */
        int r = fatfs_walk(c, cl, 1, &nx);
        if (r < 0) return r;
        if (nx >= c->fat_max_cluster) return FAT_ENOSPC;
        cl = nx; cidx++; off = 0;
        fatfs_ckpt_record(f, cidx, cl);
    }
}

int fatfs_read(fatms_ctx *c, OpenFile *f, void *buf, int len)          /* FUN_000086c4 */
{
    int r = fatfs_prep(c);
    if (r >= 0) r = fatfs_ready(c);
    if (r >= 0) r = fatfs_fd_valid(c, f);
    if (r < 0) return r;
    if (es_is_file(f)) return es_read(c, f, buf, len);
    if (c->exfat) return exfat_read(c, f, buf, len);
    if (!(f->oflags & 1)) return FAT_EACCES;                             /* not opened for reading */
    u32 pos = f->position, size = F_SIZE(f), n = (u32)len;
    if (pos >= size) return 0;
    if (size - pos < n) n = size - pos;
    if (n == 0) return 0;
    u32 cb = (u32)c->bytes_per_sector * c->sectors_per_cluster, cl, done;
    if ((r = fatfs_seek_cluster(c, f, pos / cb, &cl)) < 0) return r;
    if ((r = fatfs_data_io(c, f, 0, (u8 *)buf, pos, n, cl, &done)) < 0) return r;
    f->position = pos + done;
    return (int)done;
}
