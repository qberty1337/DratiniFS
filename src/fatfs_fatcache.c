/* fatfs_fatcache.c - Sony 6.61 fatms FAT-cache engine (see fatcache.h for the FUN_ map).
 *
 * Literal port of Sony's banked FAT-sector cache + allocator + free accounting. Two
 * deliberate, behaviour-identical simplifications of pure micro-optimisations in the
 * decompile (flagged inline, verified by test_fatcache.c):
 *   - window miss reloads the clamped window instead of memmove-sliding the overlap
 *     (FUN_000022a0 case B already full-reloads; A/D only save re-reads, same bytes);
 *   - FAT12 straddle is handled byte-wise instead of FUN_00002530's window-advance.
 * Everything observable (FAT bytes written, all-copy replication, free/contig counts,
 * allocation order from the next-free hint) matches Sony. */
#ifdef FATCACHE_HOST
#include <string.h>
#else
#include "fatms.h"
#endif
#include "fatcache.h"

#define E_BADCL  (-0x7ffefff3)   /* 0x8001000d  cluster out of range / not ready */
#define E_NOFREE (-0x7ffeffe4)   /* 0x8001001c  no free cluster / chain end */
#define E_INVAL  (-0x7ffeffea)   /* 0x80010016 */

/* byte offset of cluster's entry within one FAT copy */
static u32 fat_byte_off(const fat_engine *e, u32 cluster)
{
    if (e->tflags & TF_FAT32) return cluster * 4;
    if (e->tflags & TF_FAT16) return cluster * 2;
    return cluster + (cluster >> 1);          /* FAT12: cluster*3/2 */
}

void fat_window_reset(fat_engine *e)
{
    e->window_tag = 0xffffffffu;
    for (int i = 0; i < FC_DIRTYW; i++) e->dirty[i] = 0;
    e->tflags &= ~TF_DIRTY;
    e->free_clusters = 0;                     /* bit31 clear => stale */
    e->free_contig = 0;
    e->next_free = 2;
}

int fat_flush(fat_engine *e)                  /* FUN_00002a84 */
{
    if (!(e->tflags & TF_DIRTY)) return 0;
    e->tflags &= ~TF_DIRTY;
    u32 wlen = e->window_sectors;
    if (wlen > e->fat_size) wlen = e->fat_size;
    for (u32 r = 0; r < wlen; ) {
        if (!(e->dirty[r >> 5] & (1u << (r & 31)))) { r++; continue; }
        u32 run = 1;                          /* coalesce a run of dirty sectors */
        while (r + run < wlen && (e->dirty[(r + run) >> 5] & (1u << ((r + run) & 31)))) run++;
        for (u32 f = 0; f < e->num_fats; f++) {
            int rc = e->write(e->owner, e->cache + r * e->bps,
                              e->window_tag + r + f * e->fat_size, run);
            if (rc < 0) return rc;
        }
        for (u32 i = 0; i < run; i++) e->dirty[(r + i) >> 5] &= ~(1u << ((r + i) & 31));
        r += run;
    }
    return 0;
}

/* ensure the FAT (copy-0) sector `abs_sec` is in the window; return its window-relative index */
static int win_ensure(fat_engine *e, u32 abs_sec)
{
    if (e->window_tag != 0xffffffffu && abs_sec >= e->window_tag &&
        abs_sec < e->window_tag + e->window_sectors)
        return (int)(abs_sec - e->window_tag);
    int rc = fat_flush(e);
    if (rc < 0) return rc;
    u32 wlen = e->window_sectors;
    if (wlen > e->fat_size) wlen = e->fat_size;
    u32 fat_lo = e->reserved, fat_hi = e->reserved + e->fat_size, old = e->window_tag;
    u32 start = abs_sec, keep = 0, dst = 0;                /* FUN_000022a0: window starts at the sector */
    if (start + wlen > fat_hi) {                           /* clamp to the FAT end; keep the old window's tail */
        start = fat_hi - wlen;
        if (old != 0xffffffffu && start < old + wlen && start >= old) {
            keep = old + wlen - start;
            for (u32 i = 0; i < keep * e->bps; i++) e->cache[i] = e->cache[(start - old) * e->bps + i];   /* moves down */
        }
        dst = keep;
    } else if (old != 0xffffffffu && start < old && start + wlen > old) {   /* slide down: keep the old head */
        keep = start + wlen - old;
        for (u32 i = keep * e->bps; i-- > 0; ) e->cache[(old - start) * e->bps + i] = e->cache[i];   /* moves up */
    }
    if (start < fat_lo) start = fat_lo;
    u32 rd = (dst ? start + keep : start), n = wlen - keep;
    rc = n ? e->read(e->owner, e->cache + dst * e->bps, rd, n) : 0;
    if (rc < 0) { e->window_tag = 0xffffffffu; return rc; }
    e->window_tag = start;
    return (int)(abs_sec - start);
}

static int fat_rd_byte(fat_engine *e, u32 byte_off, u8 *out)
{
    int rel = win_ensure(e, e->reserved + byte_off / e->bps);
    if (rel < 0) return rel;
    *out = e->cache[(u32)rel * e->bps + byte_off % e->bps];
    return 0;
}
static int fat_wr_byte(fat_engine *e, u32 byte_off, u8 keepmask, u8 val)
{
    int rel = win_ensure(e, e->reserved + byte_off / e->bps);
    if (rel < 0) return rel;
    u8 *p = &e->cache[(u32)rel * e->bps + byte_off % e->bps];
    *p = (u8)((*p & keepmask) | val);
    e->dirty[(u32)rel >> 5] |= 1u << ((u32)rel & 31);
    e->tflags |= TF_DIRTY;
    return 0;
}

int fat_get(fat_engine *e, u32 cluster, u32 *value)       /* FUN_00004670 */
{
    *value = 0;
    if (cluster >= e->max_cluster || cluster < 2) return E_BADCL;
    u32 bo = fat_byte_off(e, cluster);
    u8 b0, b1;
    if (e->tflags & TF_FATSZ) {               /* FAT16/32: aligned, never straddles -> one window lookup */
        u32 sec = e->bps == 512 ? bo >> 9 : bo / e->bps, off = e->bps == 512 ? bo & 511 : bo % e->bps;
        int rel = win_ensure(e, e->reserved + sec);
        if (rel < 0) return E_BADCL;
        const u8 *p = e->cache + (u32)rel * e->bps + off;
        *value = (e->tflags & TF_FAT32) ? (u32)(p[0] | p[1] << 8 | p[2] << 16 | (u32)p[3] << 24)
                                        : (u32)(p[0] | p[1] << 8);
    } else {                                  /* FAT12 (straddle-safe, byte-wise) */
        if (fat_rd_byte(e, bo, &b0) < 0 || fat_rd_byte(e, bo + 1, &b1) < 0) return E_BADCL;
        u32 raw = b0 | (b1 << 8);
        *value = (cluster & 1) ? (raw >> 4) : (raw & 0xfff);
    }
    return 0;
}

/* FUN_00004cd0: are all other entries of `cluster`'s ms_block-aligned erase block free?
   Reads the cached window directly: a block is at most 0x80 entries and the window starts on a
   sector boundary, so for FAT16/32 the whole block sits in the window. FAT12 has ms_block 1. */
static int fat_contig_blk(fat_engine *e, u32 cluster, const u8 *ent)
{
    u32 blk = e->ms_block;
    if (blk < 2 || !(e->tflags & TF_FATSZ)) return 1;
    u32 esz = (e->tflags & TF_FAT32) ? 4 : 2;
    const u8 *p = ent - (cluster % blk) * esz;
    for (u32 k = 0; k < blk; k++, p += esz)
        if (p != ent && (esz == 4 ? (p[0] | p[1] | p[2] | p[3]) : (p[0] | p[1]))) return 0;
    return 1;
}

int fat_set(fat_engine *e, u32 cluster, u32 value)        /* FUN_00004884 */
{
    if (cluster >= e->max_cluster || cluster < 2) return E_NOFREE;
    int track = ((int)e->free_clusters < 0);              /* cached counts valid (bit31) */
    u32 oldv = 0;
    const u8 *ent = 0;

    u32 bo = fat_byte_off(e, cluster);
    if (e->tflags & TF_FATSZ) {               /* FAT16/32: one window lookup, read old + write in place */
        int rel = win_ensure(e, e->reserved + bo / e->bps);
        if (rel < 0) return E_BADCL;
        u8 *p = e->cache + (u32)rel * e->bps + bo % e->bps;
        if (e->tflags & TF_FAT32) {           /* Sony writes all 32 bits (top nibble not preserved) */
            oldv = (u32)(p[0] | p[1] << 8 | p[2] << 16 | (u32)p[3] << 24);
            p[2] = (u8)(value >> 16); p[3] = (u8)(value >> 24);
        } else {
            value &= 0xffff;
            oldv = (u32)(p[0] | p[1] << 8);
        }
        p[0] = (u8)value; p[1] = (u8)(value >> 8);
        e->dirty[(u32)rel >> 5] |= 1u << ((u32)rel & 31);
        e->tflags |= TF_DIRTY;
        ent = p;
    } else {                                  /* FAT12 */
        if (track && fat_get(e, cluster, &oldv) < 0) return E_BADCL;
        value &= 0xfff;
        u8 b0, b1;
        if (fat_rd_byte(e, bo, &b0) < 0 || fat_rd_byte(e, bo + 1, &b1) < 0) return E_BADCL;
        u16 raw = (u16)(b0 | (b1 << 8));
        raw = (cluster & 1) ? (u16)((raw & 0x000f) | (value << 4)) : (u16)((raw & 0xf000) | value);
        if (fat_wr_byte(e, bo, 0, (u8)raw) < 0 || fat_wr_byte(e, bo + 1, 0, (u8)(raw >> 8)) < 0) return E_BADCL;
    }

    if (track) {                              /* maintain cached free + contiguous counts (Sony LAB_00004a38) */
        int now_used = (value != 0), was_used = (oldv != 0);
        if (!now_used && was_used) {          /* freeing */
            e->free_clusters = (e->free_clusters & 0x7fffffffu) + 1;
            e->free_clusters |= 0x80000000u;
            u32 blk = e->ms_block ? e->ms_block : 1;
            if ((e->max_cluster / blk) * blk > cluster && fat_contig_blk(e, cluster, ent))
                e->free_contig = (((e->free_contig & 0x7fffffffu) + blk) | 0x80000000u);
        } else if (now_used && !was_used) {   /* allocating */
            e->free_clusters = (e->free_clusters & 0x7fffffffu) - 1;
            e->free_clusters |= 0x80000000u;
            u32 blk = e->ms_block ? e->ms_block : 1;
            if ((e->max_cluster / blk) * blk > cluster && fat_contig_blk(e, cluster, ent))
                e->free_contig = (((e->free_contig & 0x7fffffffu) - blk) | 0x80000000u);
        }
    }
    return 0;
}

int fat_find_free(fat_engine *e, u32 hint, u32 *out, int just_find)   /* FUN_000027dc */
{
    *out = 0;
    if ((int)e->free_clusters < 0 && (e->free_clusters & 0x7fffffffu) == 0) return 0;  /* disk full */
    u32 span = e->max_cluster - 2;
    if ((int)span <= 0) return 0;
    u32 start = hint;
    if (start < 2 || start >= e->max_cluster) {
        start = e->next_free;
        if (start < 2 || start >= e->max_cluster) start = 2;
    }
    u32 c = start;
    if (e->tflags & TF_FATSZ) {               /* FAT16/32: walk the window buffer directly (27dc) */
        u32 esz = (e->tflags & TF_FAT32) ? 4 : 2, wlen = e->window_sectors;
        if (wlen > e->fat_size) wlen = e->fat_size;
        for (u32 n = 0; n < span; ) {
            u32 bo = c * esz;
            int rel = win_ensure(e, e->reserved + bo / e->bps);
            if (rel < 0) return rel;
            const u8 *p = e->cache + (u32)rel * e->bps + bo % e->bps, *end = e->cache + wlen * e->bps;
            for (; p < end && n < span; p += esz, n++) {
                if (!(esz == 4 ? (p[0] | p[1] | p[2] | p[3]) : (p[0] | p[1])) &&
                    (just_find || !e->is_reserved || !e->is_reserved(e->owner, c))) { *out = c; return 0; }
                if (++c >= e->max_cluster) {
                    if (just_find) return 0;  /* param_4=1: stop at the FAT end */
                    c = 2; n++;               /* allocate mode: wrap to cluster 2 (reload the window) */
                    break;
                }
            }
        }
        return 0;
    }
    for (u32 n = 0; n < span; n++) {          /* FAT12: per entry (4670), wrap unless just_find */
        u32 v;
        int rc = fat_get(e, c, &v);
        if (rc < 0) return rc;
        if (v == 0 && (just_find || !e->is_reserved || !e->is_reserved(e->owner, c))) { *out = c; return 0; }
        if (++c >= e->max_cluster) { if (just_find) return 0; c = 2; }
    }
    return 0;                                 /* none found */
}

int fat_scan(fat_engine *e)                   /* FUN_00005ab0 */
{
    u32 free = 0, contig = 0, run = 0, prev = 1, c = 2;
    u32 blk = e->ms_block ? e->ms_block : 1;
    e->free_clusters = 0;                       /* invalid during the scan (no disk-full shortcut) */
    e->free_contig = 0;
    while (c < e->max_cluster) {
        u32 found;
        int rc = fat_find_free(e, c, &found, 1);
        if (rc < 0) return rc;
        if (found == 0) break;
        free++;
        u32 rem = found % blk;
        if (rem == 0) run = 0;
        run = (run + 1) & 0xffff;
        if (found == prev + 1 || rem == 0) { if (run == blk) { contig += run; run = 0; } }
        else run = 0;
        prev = found;
        c = found + 1;
    }
    e->free_clusters = free | 0x80000000u;
    e->free_contig = contig | 0x80000000u;
    return 0;
}

int fat_extend(fat_engine *e, u32 start, u32 count, u32 *last, u32 *allocated)   /* FUN_00002664 */
{
    u32 alloc = 0;
    int err = 0;
    for (u32 i = 0; i < count; i++) {
        u32 found;
        err = fat_find_free(e, start, &found, 0);
        if (err < 0) break;
        if (found == 0) { err = E_NOFREE; break; }
        err = fat_set(e, found, fateng_eoc_value(e));      /* new cluster = EOC */
        if (err < 0) break;
        err = fat_set(e, start, found);                 /* link prev -> new */
        if (err < 0) break;
        alloc++;                                        /* Sony leaves the next-free hint alone here */
        start = found;
    }
    *last = start;
    *allocated = alloc;
    return err;
}

int fat_walk(fat_engine *e, u32 start, u32 nlinks, u32 *out)   /* FUN_00004d90 (fd cache omitted) */
{
    *out = 0;
    if (start < 2) return E_NOFREE;
    u32 c = start;
    for (u32 i = 0; i < nlinks; i++) {
        u32 v;
        int rc = fat_get(e, c, &v);
        if (rc < 0) return rc;
        if (v < 2) return E_INVAL;            /* free/reserved mid-chain = corrupt */
        if (fateng_is_eoc(e, v)) { *out = c; return E_NOFREE; }   /* chain ended early */
        c = v;
    }
    *out = c;
    return 0;
}
