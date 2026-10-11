/* format.c - DratiniFS's formatter (research/docs/FEATURE_PLAN.md phase 5).
 * Sony's 0x0241D813 (XMB "Format Memory Stick") asks the card itself to format (msstor sends the MS
 * Pro FORMAT command). An SD adapter's firmware then writes its own MBR and FAT32, which loses an ES
 * partition and turns exFAT into FAT32. Here the filesystem is written in software instead:
 *   - devctl 0x0241D813 (XMB): exFAT if the card is exFAT now, else FAT32;
 *   - devctl 0x04100005 (the ES app): 0 = FAT32, 1 = exFAT.
 * Only partition 0 is rewritten, at LBA 2048 up to the ES partition (or to partition 0's old end, or
 * the card's end). Every other MBR entry and the data it covers stay untouched; a superfloppy card is
 * formatted in place. FAT32: 32 KB clusters (smaller when the partition needs them to stay FAT32),
 * both FATs zeroed, FSInfo + backup boot sector. exFAT: 128 KB clusters, FAT and bitmap zeroed, both
 * boot regions with checksums, the standard upcase table. The rest is Sony's 0x0241D813 sequence
 * (no media while writing, msstor re-reads the card, rescan) with the writes in place of the FORMAT
 * command. A card too small for a valid FAT32 (an old Memory Stick) still gets Sony's card format. */
#include "fatms.h"
#include "exfat_upcase.h"

#define ZSEC 64                                  /* sectors per zero-fill write */

static struct {
    SceUID fd;
    u8 s[512] __attribute__((aligned(64)));      /* the sector being built */
    u8 mbr[512] __attribute__((aligned(64)));    /* sector 0 */
    u8 *z;                                       /* ZSEC zeroed sectors (Fpl, 64-byte aligned) */
} F;

static void put16(u8 *p, u32 v) { p[0] = (u8)v; p[1] = (u8)(v >> 8); }
static void put32(u8 *p, u32 v) { put16(p, v); put16(p + 2, v >> 16); }
static u32 le32(const u8 *p) { return (u32)(p[0] | p[1] << 8 | p[2] << 16 | (u32)p[3] << 24); }

static int wr(u32 lba, const void *buf, u32 n)  /* n sectors through msstor0: */
{
    SceOff o = k_sceIoLseek64k(F.fd, (SceOff)lba * 512, 0);
    if (o < 0) return (int)o;
    sceKernelDcacheWritebackInvalidateRange(buf, n * 512);
    int r = k_sceIoWrite(F.fd, buf, (int)(n * 512));
    return r < 0 ? r : 0;
}
static int zero(u32 lba, u32 n)
{
    for (int r; n; ) {
        u32 k = n < ZSEC ? n : ZSEC;
        if ((r = wr(lba, F.z, k)) < 0) return r;
        lba += k; n -= k;
    }
    return 0;
}

/* FAT32 cluster size: 32 KB, halved while that leaves too few clusters to be FAT32 (0 = too small) */
static u32 fat32_spc(u32 n)
{
    for (u32 spc = 64; spc; spc >>= 1) {
        u32 fatsz = (((n - 32) / spc + 2) * 4 + 511) / 512, data = (32 + 2 * fatsz + spc - 1) / spc * spc;
        if (data < n && (n - data) / spc >= 65525) return spc;
    }
    return 0;
}
static int fmt_fat32(u32 start, u32 n, u32 spc, u32 serial)
{
    u32 fatsz = (((n - 32) / spc + 2) * 4 + 511) / 512;
    u32 data = (32 + 2 * fatsz + spc - 1) / spc * spc;          /* the data area starts on a cluster */
    u32 rsv = data - 2 * fatsz, cl = (n - data) / spc;
    int r;
    if (cl > 0x0ffffff5u) cl = 0x0ffffff5u;
    if ((r = zero(start, rsv)) < 0 || (r = zero(start + rsv, 2 * fatsz)) < 0 || (r = zero(start + data, spc)) < 0) return r;
    u8 *b = F.s;
    memset(b, 0, 512);                                          /* boot sector */
    b[0] = 0xeb; b[1] = 0x58; b[2] = 0x90; memcpy(b + 3, "MSDOS5.0", 8);
    put16(b + 11, 512); b[13] = (u8)spc; put16(b + 14, rsv); b[16] = 2; b[21] = 0xf8;
    put16(b + 24, 63); put16(b + 26, 255); put32(b + 28, start); put32(b + 32, n);
    put32(b + 36, fatsz); put32(b + 44, 2); put16(b + 48, 1); put16(b + 50, 6);
    b[64] = 0x80; b[66] = 0x29; put32(b + 67, serial);
    memcpy(b + 71, "NO NAME    ", 11); memcpy(b + 82, "FAT32   ", 8);
    b[510] = 0x55; b[511] = 0xaa;
    if ((r = wr(start, b, 1)) < 0 || (r = wr(start + 6, b, 1)) < 0) return r;
    memset(b, 0, 512);                                          /* FSInfo: the root takes one cluster */
    put32(b, 0x41615252); put32(b + 484, 0x61417272); put32(b + 488, cl - 1); put32(b + 492, 3);
    put32(b + 508, 0xaa550000);
    if ((r = wr(start + 1, b, 1)) < 0 || (r = wr(start + 7, b, 1)) < 0) return r;
    memset(b, 0, 512);                                          /* FAT[0], FAT[1], root cluster 2 = end */
    put32(b, 0x0ffffff8); put32(b + 4, 0x0fffffff); put32(b + 8, 0x0fffffff);
    if ((r = wr(start + rsv, b, 1)) < 0 || (r = wr(start + rsv + fatsz, b, 1)) < 0) return r;
    return 0;
}

static int fmt_exfat(u32 start, u32 n, u32 serial)
{
    u32 shift = 8;                                              /* 128 KB clusters */
    while (shift > 3 && (n >> shift) < 2048) shift--;          /* small partitions: smaller clusters */
    u32 spc = 1u << shift, cb = spc * 512, fatoff = n >= 0x40000 ? 2048 : 128, cnt = (n - fatoff) / spc, fatlen = 0, heap = 0;
    for (int k = 0; k < 2; k++) {
        fatlen = ((cnt + 2) * 4 + 511) / 512;
        heap = (fatoff + fatlen + spc - 1) / spc * spc;         /* the heap starts on a cluster */
        if (heap >= n) return FAT_EINVAL;
        cnt = (n - heap) / spc;
    }
    u32 bmlen = (cnt + 7) / 8, bmcl = (bmlen + cb - 1) / cb, upcl = (sizeof exfat_upcase + cb - 1) / cb;
    u32 upfirst = 2 + bmcl, root = upfirst + upcl, used = root - 1;   /* clusters 2..root are in use */
    int r;
    if ((r = zero(start, 24)) < 0 || (r = zero(start + fatoff, fatlen)) < 0 ||
        (r = zero(start + heap, (root - 1) * spc)) < 0) return r;
    u8 *b = F.s;
    for (u32 sec = 0; sec * 128 < used + 2; sec++) {           /* FAT: 0, 1, then each run chained */
        memset(b, 0, 512);
        for (u32 i = 0; i < 128; i++) {
            u32 c = sec * 128 + i, v = 0;
            if (c == 0) v = 0xfffffff8u;
            else if (c == 1 || c == root || c == upfirst - 1 || c == root - 1) v = 0xffffffffu;
            else if (c >= 2 && c < root) v = c + 1;
            put32(b + i * 4, v);
        }
        if ((r = wr(start + fatoff + sec, b, 1)) < 0) return r;
    }
    for (u32 sec = 0; sec * 4096 < used; sec++) {               /* bitmap: the used clusters' bits */
        memset(b, 0, 512);
        for (u32 bit = sec * 4096; bit < used && bit < (sec + 1) * 4096; bit++) b[(bit % 4096) / 8] |= (u8)(1 << (bit & 7));
        if ((r = wr(start + heap + sec, b, 1)) < 0) return r;
    }
    u32 sum = 0;                                                /* upcase table */
    for (u32 i = 0; i < sizeof exfat_upcase; i++) sum = ((sum & 1) ? 0x80000000u : 0) + (sum >> 1) + exfat_upcase[i];
    for (u32 off = 0; off < sizeof exfat_upcase; off += 512) {
        memset(b, 0, 512);
        memcpy(b, exfat_upcase + off, sizeof exfat_upcase - off < 512 ? sizeof exfat_upcase - off : 512);
        if ((r = wr(start + heap + (upfirst - 2) * spc + off / 512, b, 1)) < 0) return r;
    }
    memset(b, 0, 512);                                          /* root: bitmap + upcase entries */
    b[0] = 0x81; put32(b + 20, 2); put32(b + 24, bmlen);
    b[32] = 0x82; put32(b + 36, sum); put32(b + 52, upfirst); put32(b + 56, sizeof exfat_upcase);
    if ((r = wr(start + heap + (root - 2) * spc, b, 1)) < 0) return r;
    for (u32 region = 0; region < 2; region++) {               /* main + backup boot region */
        u32 csum = 0, base = start + region * 12;
        for (u32 sec = 0; sec < 11; sec++) {
            memset(b, 0, 512);
            if (sec == 0) {
                b[0] = 0xeb; b[1] = 0x76; b[2] = 0x90; memcpy(b + 3, "EXFAT   ", 8);
                put32(b + 64, start); put32(b + 72, n);
                put32(b + 80, fatoff); put32(b + 84, fatlen); put32(b + 88, heap); put32(b + 92, cnt);
                put32(b + 96, root); put32(b + 100, serial); put16(b + 104, 0x0100);
                b[108] = 9; b[109] = (u8)shift; b[110] = 1; b[111] = 0x80; b[112] = 0;
                b[510] = 0x55; b[511] = 0xaa;
            } else if (sec <= 8) put32(b + 508, 0xaa550000u);   /* extended boot sectors */
            for (u32 i = 0; i < 512; i++)
                if (sec || (i != 106 && i != 107 && i != 112)) csum = ((csum & 1) ? 0x80000000u : 0) + (csum >> 1) + b[i];
            if ((r = wr(base + sec, b, 1)) < 0) return r;
        }
        for (u32 i = 0; i < 128; i++) put32(b + i * 4, csum);
        if ((r = wr(base + 11, b, 1)) < 0) return r;
    }
    return 0;
}

/* exfat: 1 = exFAT, 0 = FAT32, -1 = what the card is now (XMB) */
int dfs_format(fatms_ctx *c, int exfat)
{
    int r = fatfs_prep(c);                                      /* know what the card is now */
    if (exfat < 0) exfat = (r >= 0 && c->exfat) ? 1 : 0;
    u8 *mbr = F.mbr;
    u32 start = 0, end = 0, cap = 0, spc = 0, mbr_mode;
    F.fd = k_sceIoOpen(c->name_msstor0, 0x4000003, 0);
    if (F.fd < 0) return F.fd;
    k_sceIoIoctl(F.fd, 0x2125006, 0, 0, &cap, 4);               /* msstor's size (raised by msstor_cap.c) */
    r = (k_sceIoLseek64k(F.fd, 0, 0) == 0 && k_sceIoRead(F.fd, mbr, 512) == 512) ? 0 : FAT_EBADBPB;
    /* sector 0: a boot sector (superfloppy: format in place), an MBR, or nothing (a new MBR) */
    const char *m = (const char *)mbr;                          /* (strncmp: Sony's imports have no memcmp) */
    int boot = (mbr[0] == 0xeb || mbr[0] == 0xe9) && (!strncmp(m + 3, "EXFAT   ", 8) || !strncmp(m + 82, "FAT32", 5) ||
                                                     !strncmp(m + 54, "FAT1", 4));
    mbr_mode = !boot;
    if (r >= 0 && boot) end = c->total_sectors && (c->media_flags & MF_OK) ? c->total_sectors : cap;
    if (r >= 0 && !boot) {
        if (mbr[510] != 0x55 || mbr[511] != 0xaa) { memset(mbr, 0, 512); mbr[510] = 0x55; mbr[511] = 0xaa; }
        u32 es = 0;
        start = 2048;
        for (int i = 0; i < 4; i++) {
            const u8 *e = mbr + 0x1be + i * 16;
            if (e[4] == 0xda && le32(e + 8)) { if (i == 0) r = FAT_EINVAL; es = le32(e + 8); }   /* ES in slot 0: not mountable */
        }
        if (es) end = es;
        else if (mbr[0x1be + 4] && le32(mbr + 0x1be + 12)) end = le32(mbr + 0x1be + 8) + le32(mbr + 0x1be + 12);
        else end = cap;
        for (int i = 1; i < 4 && r >= 0; i++) {                 /* nothing else may sit in the new partition */
            const u8 *e = mbr + 0x1be + i * 16;
            u32 lba = le32(e + 8), len = le32(e + 12);
            if (e[4] && e[4] != 0xda && len && lba < end && lba + len > start) r = FAT_EINVAL;
        }
    }
    u32 n = end > start ? end - start : 0;
    if (r >= 0 && (n < 0x2000 || (!exfat && !(spc = fat32_spc(n))))) r = 1;   /* too small for FAT32 */
    if (r != 0) {
        k_sceIoClose(F.fd);
        if (r < 0) return r;
        u8 b = 0;                                               /* Sony's own: the card formats itself */
        fatfs_set_flags(c, ST_NOMEDIA, 1);
        r = fatfs_blkdevctl(c, b);
        fatfs_set_flags(c, ST_NOMEDIA, 0);
        int q = fatfs_rescan(c);
        return r < 0 ? r : q;
    }
    SceUID zfpl = 0;
    u8 *zb = (u8 *)fatfs_alloc_block(&zfpl, ZSEC * 512 + 64, "SceFatfsFormat");
    if (!zb) { k_sceIoClose(F.fd); return FAT_ENOMEM; }
    F.z = (u8 *)(((u32)zb + 63) & ~63u);
    memset(F.z, 0, ZSEC * 512);
    ScePspDateTime t; u32 dos = 0;                              /* volume serial from the clock, as Windows */
    if (sceRtc_driver_9012B140(&t) == 0) sceRtc_driver_A4A5BF1B(&t, &dos);
    u32 serial = dos ^ (start << 16) ^ n;

    fatfs_set_flags(c, ST_NOMEDIA, 1);                          /* Sony: no media while it changes */
    r = exfat ? fmt_exfat(start, n, serial) : fmt_fat32(start, n, spc, serial);
    if (r >= 0 && mbr_mode) {                                   /* partition 0 = the new filesystem */
        u8 *e = mbr + 0x1be;
        e[0] = 0x80; e[1] = 0xfe; e[2] = 0xff; e[3] = 0xff;
        e[4] = exfat ? 0x07 : 0x0c; e[5] = 0xfe; e[6] = 0xff; e[7] = 0xff;
        put32(e + 8, start); put32(e + 12, n);
        r = wr(0, mbr, 1);
    }
    k_sceIoClose(F.fd);
    sceKernelDeleteFpl(zfpl);
    k_sceIoDevctl(c->name_msstor0, 0x210d816, 0, 0, 0, 0);      /* msstor re-reads the card (as Sony) */
    c->media_flags |= MF_REMOVED | MF_INSERTED;                 /* nothing cached from the old FS survives */
    fatfs_set_flags(c, ST_NOMEDIA, 0);
    int q = fatfs_rescan(c);                                    /* eject + insert: Sony's remount */
    return r < 0 ? r : q;
}
