/* msstor_cap.c - cards up to 2 TB (DratiniFS's method; research/msstor_661_pseudocode/SIZE_LIMIT_ANALYSIS.md).
 * Sony's msstor takes the card size from the MS attribute data, which SD adapters fake (often about
 * 57 GB). With the MSSTOR patcher's NOP at text+0x3F40 the partition check no longer rejects bigger
 * cards, but msstor still uses that size for the seek limit of msstor0:/msstor0p1: and for READ
 * CAPACITY (ioctl 0x2125006, which USB mode reports to the PC). On every insert this raises it to
 * the end of the last MBR partition:
 *   - dev+0x4E60, msstor's size field, for the current card setup, and
 *   - the size getter at text+0x3334, rewritten to return our value, because msstor resets the field
 *     at every card setup (resume, re-insert, devctl 0x210d816 from USB mode or XMB format).
 * Only Sony 6.61's exact code is patched; anything else is left alone (the card then works up to the
 * adapter's size, as with stock fatms). msstor's own value is never lowered. The MBR read here also
 * gives the ES layer the partition table (mbr_entry). */
#include "fatms.h"
#include <psploadcore.h>

extern void sceKernelIcacheInvalidateRange(const void *p, unsigned int size);

static u32 g_cap;                                    /* what the patched getter returns */
static u8  g_mbr[512] __attribute__((aligned(64)));  /* only the media thread uses it */
static int g_mbr_ok;

/* Sony 6.61 msstor FUN_00003334 (12 words). Words 2..5 are relocated lui/lw pairs: only their opcode
   halves are fixed (lui v0 / lw v1,(v0) / lui v0 / lw a0,(v0)). */
static const u32 sony_3334[12] = { 0x27bdfff0, 0xafbf0000, 0x3c020000, 0x8c430000, 0x3c020000, 0x8c440000,
                                   0x8c620010, 0x0040f809, 0x00000000, 0x8fbf0000, 0x03e00008, 0x27bd0010 };

static u32 le32(const u8 *p) { return (u32)(p[0] | p[1] << 8 | p[2] << 16 | (u32)p[3] << 24); }

int mbr_entry(int i, u8 *type, u32 *lba, u32 *len)
{
    if (!g_mbr_ok || i < 0 || i > 3) return -1;
    const u8 *e = g_mbr + 0x1be + i * 16;
    *type = e[4]; *lba = le32(e + 8); *len = le32(e + 12);
    return 0;
}

/* the card's MBR through msstor0: (msstor0p1: can't reach sector 0), when msstor0p1: is a partition */
static u32 read_mbr(fatms_ctx *c)
{
    u32 whole = 0, part = 0, end = 0;
    g_mbr_ok = 0;
    if (k_sceIoIoctl(c->ctrl_fd, 0x2125006, 0, 0, &part, 4) < 0) return 0;
    SceUID fd = k_sceIoOpen(c->name_msstor0, 0x4000001, 0);
    if (fd < 0) return 0;
    if (k_sceIoIoctl(fd, 0x2125006, 0, 0, &whole, 4) >= 0 && whole > part &&     /* p1 starts past 0 */
        k_sceIoLseek64k(fd, 0, 0) == 0 && k_sceIoRead(fd, g_mbr, 512) == 512 &&
        g_mbr[510] == 0x55 && g_mbr[511] == 0xaa && g_mbr[0x1be] == 0x80 && g_mbr[0x1c2] != 0)   /* msstor's rule */
        g_mbr_ok = 1;
    k_sceIoClose(fd);
    for (int i = 0; g_mbr_ok && i < 4; i++) {
        const u8 *e = g_mbr + 0x1be + i * 16;
        u32 lba = le32(e + 8), len = le32(e + 12), e2 = lba + len;
        if (e[4] == 0 || len == 0) continue;
        if (e2 < lba) e2 = 0xffffffffu;                                          /* 2 TB: 32-bit LBAs */
        if (e2 > end) end = e2;
    }
    return end;
}

void msstor_cap_insert(fatms_ctx *c)
{
    u32 end = read_mbr(c);
    SceModule *m = sceKernelFindModuleByName("sceMSstor_Driver");
    if (!m) return;
    u32 *t = (u32 *)(m->text_addr + 0x3334), *get = (u32 *)(m->text_addr + 0x4554);
    u32 a = (u32)&g_cap, hi = (a + 0x8000) >> 16, lo = a & 0xffff;
    int ours = t[0] == (0x3c020000 | hi) && t[1] == (0x8c420000 | lo) && t[2] == 0x03e00008 && t[3] == 0;
    int sony = get[0] == 0x03e00008 && get[1] == 0x8c824e60;                     /* jr ra; lw v0, 0x4e60(a0) */
    for (int i = 0; sony && i < 12; i++)
        sony = (i >= 2 && i <= 5) ? (t[i] >> 16) == (sony_3334[i] >> 16) : t[i] == sony_3334[i];
    if (!ours && !sony) return;                                                  /* not Sony 6.61's msstor */
    if ((t[4] >> 16) != 0x3c02 || (t[5] >> 16) != 0x8c44) return;               /* lui v0 / lw a0: &dev */
    u32 dev = *(u32 *)((t[4] << 16) + (u32)(s32)(s16)t[5]);
    if (dev < 0x88000000u || dev >= 0x8c000000u) return;                          /* kernel RAM */
    u32 *size = (u32 *)(dev + 0x4e60), cap = end > *size ? end : *size;
    if (!ours && cap == *size) return;                                           /* nothing to raise */
    g_cap = cap;
    *size = cap;
    if (ours) return;
    int st = sceKernelCpuSuspendIntr();
    t[0] = 0x3c020000 | hi;                                                      /* lui v0, %hi(&g_cap) */
    t[1] = 0x8c420000 | lo;                                                      /* lw  v0, %lo(&g_cap)(v0) */
    t[2] = 0x03e00008;                                                           /* jr  ra */
    t[3] = 0;                                                                    /* nop */
    sceKernelCpuResumeIntr(st);
    sceKernelDcacheWritebackInvalidateRange(t, 16);
    sceKernelIcacheInvalidateRange(t, 16);
}
