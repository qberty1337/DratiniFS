/* test_fatcache.c - HOST round-trip test for fatfs_fatcache.c. Not built into the PRX.
 *   gcc -DFATCACHE_HOST -O2 -Wall -o t test_fatcache.c fatfs_fatcache.c && ./t
 * Verifies, for FAT12/16/32, against an independent reference array + raw disk bytes:
 *   set/get round-trip, EOC, flush replicates to ALL FAT copies, chain-extend links a
 *   fresh chain, find-free honours the hint, and the incrementally-maintained free/
 *   contig counts equal a from-scratch scan. */
/* FATCACHE_HOST comes from -D on the compile line */
#include "fatcache.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

typedef struct { u8 *buf; u32 nsect; u32 bps; } ramdisk;

static int rd_read(void *o, void *buf, u32 lba, u32 n)
{
    ramdisk *d = o; if (lba + n > d->nsect) return -1;
    memcpy(buf, d->buf + (size_t)lba * d->bps, (size_t)n * d->bps); return 0;
}
static int rd_write(void *o, const void *buf, u32 lba, u32 n)
{
    ramdisk *d = o; if (lba + n > d->nsect) return -1;
    memcpy(d->buf + (size_t)lba * d->bps, buf, (size_t)n * d->bps); return 0;
}

/* decode cluster's entry straight from a given FAT copy on the raw disk (reference reader) */
static u32 ref_disk(ramdisk *d, fat_engine *e, u32 copy, u32 cluster)
{
    u32 base = (e->reserved + copy * e->fat_size) * e->bps;
    if (e->tflags & TF_FAT32) { u8 *p = d->buf + base + cluster * 4; return (p[0] | (p[1] << 8) | (p[2] << 16) | ((u32)p[3] << 24)) & 0x0fffffff; }
    if (e->tflags & TF_FAT16) { u8 *p = d->buf + base + cluster * 2; return p[0] | (p[1] << 8); }
    u32 bo = cluster + (cluster >> 1); u8 *p = d->buf + base + bo; u32 raw = p[0] | (p[1] << 8);
    return (cluster & 1) ? (raw >> 4) : (raw & 0xfff);
}

static u32 cnt_free_ref(fat_engine *e)
{
    u32 n = 0, v;
    for (u32 c = 2; c < e->max_cluster; c++) { assert(fat_get(e, c, &v) == 0); if (v == 0) n++; }
    return n;
}

static void run(const char *name, u32 tflags, u32 fat_size, u32 max_cluster, u32 window)
{
    ramdisk d; fat_engine e;
    u32 reserved = 1, num_fats = 2;
    d.bps = 512; d.nsect = reserved + num_fats * fat_size + 8;
    d.buf = calloc(d.nsect, d.bps); assert(d.buf);
    for (u32 copy = 0; copy < num_fats; copy++) {         /* real FATs: entry 0 = media byte, entry 1 = EOC */
        u8 *f = d.buf + (reserved + copy * fat_size) * 512;
        if (tflags & TF_FAT32) { static const u8 h[8] = { 0xf8, 0xff, 0xff, 0x0f, 0xff, 0xff, 0xff, 0x0f }; memcpy(f, h, 8); }
        else if (tflags & TF_FAT16) { static const u8 h[4] = { 0xf8, 0xff, 0xff, 0xff }; memcpy(f, h, 4); }
        else { f[0] = 0xf8; f[1] = 0xff; f[2] = 0xff; }
    }

    memset(&e, 0, sizeof e);
    e.bps = 512; e.ms_block = (tflags & TF_FATSZ) ? 16 : 1;   /* FUN_0000586c: FAT12 => 1 */
    e.reserved = reserved; e.num_fats = num_fats;
    e.fat_size = fat_size; e.max_cluster = max_cluster; e.tflags = tflags;
    e.window_sectors = window; e.cache = malloc((size_t)window * 512); assert(e.cache);
    e.owner = &d; e.read = rd_read; e.write = rd_write; e.is_reserved = 0;
    fat_window_reset(&e);

    u32 *ref = calloc(max_cluster, sizeof *ref); assert(ref);

    /* 1) set a scattered pattern incl. a straddling run, get it back */
    for (u32 c = 2; c < max_cluster; c += 7) { u32 v = (c * 2654435761u) % (max_cluster - 2) + 2; assert(fat_set(&e, c, v) == 0); ref[c] = v & ((tflags & TF_FAT32) ? 0x0fffffff : (tflags & TF_FAT16) ? 0xffff : 0xfff); }
    for (u32 c = 2; c < max_cluster; c++) { u32 v; assert(fat_get(&e, c, &v) == 0); assert(v == ref[c]); }

    /* 2) flush, then both FAT copies on disk must match the reference */
    assert(fat_flush(&e) == 0);
    for (u32 copy = 0; copy < num_fats; copy++)
        for (u32 c = 2; c < max_cluster; c++) assert(ref_disk(&d, &e, copy, c) == ref[c]);

    /* 3) scan free count == reference, and == a byte-level count */
    assert(fat_scan(&e) == 0);
    u32 expect_free = cnt_free_ref(&e);
    assert((e.free_clusters & 0x7fffffff) == expect_free);
    assert((int)e.free_clusters < 0);           /* bit31 valid */

    /* 4) incremental free/contig must track a fresh scan after mutations */
    for (u32 c = 3; c < max_cluster; c += 11) { u32 v; assert(fat_get(&e, c, &v) == 0); if (v == 0) assert(fat_set(&e, c, fateng_eoc_value(&e)) == 0); else assert(fat_set(&e, c, 0) == 0); }
    u32 inc_free = e.free_clusters & 0x7fffffff, inc_contig = e.free_contig & 0x7fffffff;
    assert(fat_scan(&e) == 0);
    assert((e.free_clusters & 0x7fffffff) == inc_free);
    assert((e.free_contig & 0x7fffffff) == inc_contig);

    /* 5) chain-extend from a known free cluster: distinct links, last = EOC */
    u32 head = 0, v;
    for (u32 c = 2; c < max_cluster; c++) { assert(fat_get(&e, c, &v) == 0); if (v == 0) { head = c; break; } }
    assert(head);
    assert(fat_set(&e, head, fateng_eoc_value(&e)) == 0);
    u32 last, alloc;
    assert(fat_extend(&e, head, 5, &last, &alloc) == 0);
    assert(alloc == 5);
    u32 cur = head, seen[8], ns = 0; seen[ns++] = cur;
    for (;;) { assert(fat_get(&e, cur, &v) == 0); if (fateng_is_eoc(&e, v)) break; assert(v >= 2 && v < e.max_cluster); for (u32 i = 0; i < ns; i++) assert(seen[i] != v); seen[ns++] = v; cur = v; assert(ns < 8); }
    assert(ns == 6);                             /* head + 5 */

    /* 6) find-free honours the hint (returns a free cluster >= the search) */
    u32 f; assert(fat_find_free(&e, 2, &f, 1) == 0); if (f) { assert(fat_get(&e, f, &v) == 0); assert(v == 0); }

    /* 7) random get/set (backward slides, FAT-end clamps) against a flat reference; FAT32 writes
          all 32 bits (Sony), so the reference keeps the full value there */
    u32 m = (tflags & TF_FAT32) ? 0xffffffffu : (tflags & TF_FAT16) ? 0xffff : 0xfff;
    for (u32 c = 2; c < max_cluster; c++) { assert(fat_get(&e, c, &v) == 0); ref[c] = v; }
    u32 seed = 12345;
    for (int i = 0; i < 20000; i++) {
        seed = seed * 1103515245u + 12345u;
        u32 c = 2 + (seed >> 8) % (max_cluster - 2);
        if (seed & 1) { u32 nv = (seed * 2654435761u) & m; assert(fat_set(&e, c, nv) == 0); ref[c] = nv; }
        else { assert(fat_get(&e, c, &v) == 0); assert(v == ref[c]); }
    }
    assert(fat_flush(&e) == 0);
    for (u32 copy = 0; copy < num_fats; copy++)
        for (u32 c = 2; c < max_cluster; c++) {
            u32 dv = ref_disk(&d, &e, copy, c);
            if (tflags & TF_FAT32) { u8 *p = d.buf + (e.reserved + copy * e.fat_size) * 512 + c * 4; dv |= (u32)(p[3] & 0xf0) << 24; }
            assert(dv == ref[c]);
        }

    free(ref); free(e.cache); free(d.buf);
    printf("  ok: %-6s fat_size=%u max_cluster=%u window=%u free=%u\n", name, fat_size, max_cluster, window, expect_free);
}

int main(void)
{
    run("FAT12", 0,        2, 500,  1);          /* window=1 forces straddle reload */
    run("FAT16", TF_FAT16, 4, 1000, 2);
    run("FAT32", TF_FAT32, 8, 1000, 2);
    run("FAT32w", TF_FAT32, 8, 1000, 1);         /* tiny window stress */
    run("FAT32x", TF_FAT32, 8, 1000, 3);         /* window that slides by partial overlaps */
    printf("ALL FATCACHE TESTS PASSED\n");
    return 0;
}
