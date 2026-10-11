/* fatcache.h - Sony 6.61 fatms FAT-cache engine, PSP-decoupled so it host-tests.
 *
 * Literal port of Sony's banked FAT-sector cache + allocator:
 *   fat_window_load  = FUN_000022a0   (slide/load the FAT-sector window)
 *   fat_window_next  = FUN_00002530   (advance one sector, for a straddling entry)
 *   fat_flush        = FUN_00002a84   (write dirty window sectors to ALL FAT copies)
 *   fat_get          = FUN_00004670   (read a cluster's FAT entry, 12/16/32)
 *   fat_set          = FUN_00004884   (write it + maintain cached free/contig counts)
 *   fat_contig_blk   = FUN_00004cd0   (is the MS erase-block of this entry fully free?)
 *   fat_find_free    = FUN_000027dc   (find a free cluster from the next-free hint)
 *   fat_reserved     = FUN_00002a28   (is a cluster inside an open fd's reserve run?)
 *   fat_scan         = FUN_00005ab0   (full free + contiguous-free scan)
 *   fat_extend       = FUN_00002664   (allocate+link N clusters onto a chain)
 *   fat_walk         = FUN_00004d90   (follow a chain N links w/ fd checkpoint cache)
 *
 * The engine owns only the FAT; block I/O and the open-fd reserve test are callbacks,
 * so the same code links into the PRX and into test_fatcache.c on the host. */
#ifndef FATCACHE_H
#define FATCACHE_H

#ifdef FATCACHE_HOST            /* host unit-test build: no PSP headers */
#include <stdint.h>
typedef uint8_t  u8;
typedef uint16_t u16;
typedef uint32_t u32;
#endif
/* in the PRX build, fatms.h has already pulled in pspkernel's u8/u16/u32 */

/* tflags bits (Sony ctx+0xdc, the subset the FAT engine reads) */
#define TF_FAT32  0x1000u       /* 4-byte entries */
#define TF_FAT16  0x8000u       /* 2-byte entries */
#define TF_FATSZ  0x9000u       /* FAT16|FAT32 mask; 0 => FAT12 (1.5-byte) */
#define TF_DIRTY  0x0004u       /* window has unflushed sectors */

#define FC_MAXFATS   2          /* Sony caps at 2 FAT copies */
#define FC_DIRTYW    16         /* dirty bitmap: 16 u32 => up to 512 window sectors */

typedef struct fat_engine fat_engine;

struct fat_engine {
    /* geometry (filled from the BPB by the mount path) */
    u16 bps;                    /* bytes per sector (0x200) */
    u16 ms_block;               /* MS erase-block size in clusters (ctx+0x0a) */
    u32 reserved;               /* reserved sectors before FAT #0 (exFAT: FatOffset, may pass 0xffff) */
    u8  num_fats;               /* FAT copy count */
    u32 fat_size;               /* sectors per FAT copy */
    u32 max_cluster;            /* highest valid cluster + 1 (ctx+0xd0) */
    u32 tflags;                 /* TF_* (type + dirty) */

    /* windowed FAT-sector cache */
    u8 *cache;                  /* window buffer, window_sectors*bps bytes (ctx+0x28) */
    u32 window_sectors;         /* window size in sectors (ctx+0xd4) */
    u32 window_tag;             /* abs disk sector at cache[0], 0xffffffff = empty (ctx+0x24) */
    u32 dirty[FC_DIRTYW];       /* per-window-sector dirty bits (ctx+0xe0) */

    /* allocator / free accounting */
    u32 next_free;              /* next-free-cluster hint (ctx+0x120) */
    u32 free_clusters;          /* cached free count, bit31 = valid (ctx+0x124) */
    u32 free_contig;            /* cached fully-free erase-blocks*ms_block, bit31=valid (ctx+0x128) */

    /* callbacks into the owner */
    void *owner;                                                   /* passed back to the callbacks */
    int (*read)(void *owner, void *buf, u32 lba, u32 n);           /* block read  (vtable+0x1c) */
    int (*write)(void *owner, const void *buf, u32 lba, u32 n);    /* block write (vtable+0x20) */
    int (*is_reserved)(void *owner, u32 cluster);                  /* 1 if cluster is in an open fd's run; may be 0 */
};

/* EOC sentinel per FAT width */
static inline u32 fateng_eoc_value(const fat_engine *e)
{
    return (e->tflags & TF_FAT32) ? 0x0fffffffu : (e->tflags & TF_FAT16) ? 0xffffu : 0xfffu;
}
static inline int fateng_is_eoc(const fat_engine *e, u32 v)
{
    return (e->tflags & TF_FAT32) ? v >= 0x0ffffff8u : (e->tflags & TF_FAT16) ? v >= 0xfff8u : v >= 0xff8u;
}

void fat_window_reset(fat_engine *e);                     /* invalidate window+counts (mount/remount) */
int  fat_flush(fat_engine *e);                            /* FUN_00002a84 */
int  fat_get(fat_engine *e, u32 cluster, u32 *value);     /* FUN_00004670 */
int  fat_set(fat_engine *e, u32 cluster, u32 value);      /* FUN_00004884 */
int  fat_find_free(fat_engine *e, u32 hint, u32 *out, int just_find);  /* FUN_000027dc */
int  fat_scan(fat_engine *e);                             /* FUN_00005ab0 */
int  fat_extend(fat_engine *e, u32 start, u32 count, u32 *last, u32 *allocated); /* FUN_00002664 */
int  fat_walk(fat_engine *e, u32 start, u32 nlinks, u32 *out);          /* FUN_00004d90 */

#endif
