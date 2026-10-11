/* fatfs_write.c - Sony 6.61 fatms write worker (FUN_00008de8).
 * Grows the chain up front (first cluster from the next-free hint, the rest linked by
 * FUN_00002664 from the old last cluster; a short allocation trims the write), stamps mtime +
 * archive, transfers through the shared data loop, then puts the updated entry into the
 * DirentCache. FAT window and DirentCache reach the card on close / the next flush, or at once
 * in write-through mode (devctl 0x241d814 = 1). */
#include "fatms.h"

int fatfs_write(fatms_ctx *c, OpenFile *f, const void *buf, int len)
{
    u32 n = (u32)len;
    if (n == 0) return 0;
    int r = fatfs_prep(c);
    if (r >= 0) r = fatfs_ready(c);
    if (r >= 0) r = fatfs_fd_valid(c, f);
    if (r < 0) return r;
    if (!(f->oflags & 2) || (F_ATTR(f) & 0x19) || (c->media_flags & MF_WP)) return FAT_EACCES;
    if (c->exfat) return exfat_write(c, f, buf, len);

    u32 cb = (u32)c->bytes_per_sector * c->sectors_per_cluster, size = F_SIZE(f);
    if (f->oflags & 0x100) f->position = size;                           /* O_APPEND */
    u32 pos = f->position;
    if (~pos < n) n = ~pos;                                              /* stay below 4 GB */
    u32 end = pos + n, had = f->first_cluster;
    u32 clusters = size / cb + (size % cb != 0);
    u32 alloc = (clusters <= 0xffffffffu / cb) ? clusters * cb : 0xffffffffu;
    if (alloc == 0) {                                                    /* empty file: first cluster */
        u32 nc;
        if ((r = fat_find_free(&c->fe, 0, &nc, 0)) < 0) return r;
        if ((r = fat_set(&c->fe, nc, 0x0fffffffu)) < 0) return r;
        f->first_cluster = nc;
        c->fe.next_free = nc + 1;
        f->entry[0x14] = (u8)(nc >> 16); f->entry[0x15] = (u8)(nc >> 24);
        f->entry[0x1a] = (u8)nc; f->entry[0x1b] = (u8)(nc >> 8);
        f->cur_cluster = nc; f->cur_cluster_index = 0;
        alloc = cb;
    }
    if (alloc < end) {                                                   /* extend from the last cluster */
        u32 lastcl, need = (end - alloc) / cb + ((end - alloc) % cb != 0), tail, got = 0;
        if (fatfs_seek_cluster(c, f, (alloc - 1) / cb, &lastcl) < 0) return FAT_ENOSPC;
        r = fat_extend(&c->fe, lastcl, need, &tail, &got);
        if (r < 0 && got == 0 && had != 0) return 0;                     /* card full: nothing written */
        if (need != got) {                                               /* short allocation: trim the write */
            n -= (need - got) * cb;
            if (end % cb) n += cb - end % cb;
        }
    }

    u32 cl, done = 0;
    if ((r = fatfs_seek_cluster(c, f, pos / cb, &cl)) < 0) {
        if (had == 0) { fatfs_free_chain(c, f->first_cluster); f->first_cluster = 0; }
        return r;
    }
    dirent_stamp(f->entry, 0);                                           /* FUN_0000aac0(entry, 0) */
    if ((r = fatfs_data_io(c, f, 1, (u8 *)buf, pos, n, cl, &done)) < 0) return r;

    pos += done;
    if (size < pos) F_SIZE(f) = pos;
    F_ATTR(f) |= 0x20;                                                   /* archive */
    f->position = pos;
    if ((r = dirent_put(c, f->entry, &f->dirpos)) < 0) return r;
    if (c->mode_c4 == 1) {
        if ((r = fat_flush(&c->fe)) < 0) return r;
        if ((r = dirent_flush(c)) < 0) return r;
    }
    return (int)done;
}
