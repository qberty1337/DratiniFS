/* test_path.c - host test for fatfs_path.c (Sony path/name helpers).
 * Not built into the PRX. Run:  gcc -DFATCACHE_HOST -o tp test_path.c fatfs_path.c && ./tp
 * Codepage stubs: ASCII 1:1; UCS-2 >= 0x100 becomes the 2-byte OEM pair {0x82, low byte}. */
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include "fatpath.h"

int sceCodepage_driver_855C5C2E(u16 *d, int cap, const char *s)
{ int n = 0; while (*s && n < cap / 2 - 1) { if ((u8)*s == 0x82 && s[1]) { d[n++] = (u16)(0x100 | (u8)s[1]); s += 2; } else d[n++] = (u8)*s++; } d[n] = 0; return n; }
int sceCodepage_driver_C899572E(u16 *d, int cap, const char *s) { return sceCodepage_driver_855C5C2E(d, cap, s); }
int sceCodepage_driver_47BDF633(char *d, int cap, const u16 *s)
{ int n = 0; for (; *s && n < cap - 2; s++) { if (*s >= 0x100) { d[n++] = (char)0x82; d[n++] = (char)(*s & 0xff); } else d[n++] = (char)*s; } d[n] = 0; return n; }
int sceCodepage_driver_907CBFD2(char *d, int cap, const u16 *s) { return sceCodepage_driver_47BDF633(d, cap, s); }
int sceCodepage_driver_11123ED1(int ch) { return ch >= 0x100; }
int sceCodepage_driver_014E0C72(const void *p) { return *(const u8 *)p == 0x82; }

static u16 W[0x400], T[0x400], L[0x100];
static char O[0x200];
static u16 *w(const char *s) { sceCodepage_driver_855C5C2E(W, sizeof(W), s); return W; }
static int weq(const u16 *a, const char *b) { while (*a && *a == (u8)*b) { a++; b++; } return *a == (u8)*b; }
static int norm(const char *in, const char *want)
{
    w(in);
    int r = fp_normalize(W, T);
    if (r < 0) return r;
    assert(weq(W, want));
    return 0;
}
static fpath P;
static int split(const char *in) { w(in); return fp_split(&P, W, L, O); }

int main(void)
{
    /* FUN_0000a120 / 9f80: leading '\' and "." / ".." folding */
    assert(norm("\\PSP\\GAME\\x.txt", "\\PSP\\GAME\\x.txt") == 0);
    assert(norm("PSP\\GAME", "\\PSP\\GAME") == 0);
    assert(norm("\\a\\.\\b", "\\a\\b") == 0);
    assert(norm("\\a\\b\\..\\c", "\\a\\c") == 0);
    assert(norm("\\a\\b\\..", "\\a\\") == 0);
    assert(norm("\\..", 0) == (int)0x80010002);

    /* FUN_000099fc: split + 8.3 eligibility */
    assert(split("\\PSP\\GAME\\EBOOT.PBP") == 0);
    assert(weq(P.dir, "\\PSP\\GAME\\") && weq(P.leaf, "EBOOT.PBP") && P.need_lfn == 0 && !strcmp(P.sfn, "EBOOT.PBP"));
    assert(split("\\NAME") == 0 && P.need_lfn == 0 && !strcmp(P.sfn, "NAME.") && weq(P.dir, "\\"));
    assert(split("\\PSP\\eboot.pbp") == 0 && P.need_lfn == 1);          /* lowercase */
    assert(split("\\verylongname.txt") == 0 && P.need_lfn == 1);
    assert(split("\\A B") == 0 && P.need_lfn == 1);                     /* space */
    assert(split("\\A.B.C") == 0 && P.need_lfn == 1);                   /* two dots */
    assert(split("\\") == (int)0x80010016);                             /* no leaf */
    assert(split("\\a\\\\b") == (int)0x80010016);                       /* empty component */
    assert(split("\\a\\b?") == (int)0x80010016);                        /* invalid char */

    /* FUN_000014f4 / 1598 */
    assert(fp_namecmp(w("eboot.pbp"), (sceCodepage_driver_855C5C2E(T, sizeof(T), "EBOOT.PBP"), T)) == 0);
    u16 pat[32], nm[32];
#define WM(p, n, m) (sceCodepage_driver_855C5C2E(pat, 64, p), sceCodepage_driver_855C5C2E(nm, 64, n), fp_wildmatch(pat, nm, m))
    assert(WM("*", "anything.at.all", 0) == 0);
    assert(WM("*.PNG", "ICON0.PNG", 1) == 0);
    assert(WM("*.png", "ICON0.PNG", 1) == 0);
    assert(WM("*.PNG", "ICON0.JPG", 1) != 0);
    assert(WM("*.*", "README", 1) == 0);
    assert(WM("*.*", "README", 0) == 0);
    assert(WM("?.TXT", "A.TXT", 1) == 0);
    assert(WM("?.TXT", "AB.TXT", 1) != 0);

    /* FUN_00000634 / 06d8 */
    u8 de[32]; char s[16];
    str_to_sfn(de, "EBOOT.PBP"); assert(!memcmp(de, "EBOOT   PBP", 11));
    sfn_to_str(s, de); assert(!strcmp(s, "EBOOT.PBP"));
    str_to_sfn(de, "README."); assert(!memcmp(de, "README     ", 11));
    sfn_to_str(s, de); assert(!strcmp(s, "README"));

    /* FUN_0000a3cc / a73c / a8f4: ~N aliases */
    fp_alias a; u8 sfn[12];
    assert(alias_init(&a, sfn, w("longfilename.txt"), O) == 0); assert(!memcmp(sfn, "LONGFI~1TXT", 11));
    for (int i = 2; i <= 9; i++) assert(alias_next(&a, sfn) == 0);
    assert(!memcmp(sfn, "LONGFI~9TXT", 11));
    assert(alias_next(&a, sfn) == 0); assert(!memcmp(sfn, "LONGF~10TXT", 11));
    assert(alias_init(&a, sfn, w("my.doc"), O) == 0); assert(!memcmp(sfn, "MY~1    DOC", 11));
    assert(alias_init(&a, sfn, w("abc"), O) == 0); assert(!memcmp(sfn, "ABC        ", 11));   /* not lossy: no ~1 */
    assert(alias_init(&a, sfn, w(".hidden"), O) == 0); assert(!memcmp(sfn, "HIDDEN~1   ", 11));

    /* FUN_00000940 / 078c / 08a0: LFN round trip (disk order = last fragment first) */
    const char *ln = "A Long File Name For Testing.txt";
    u16 name[64], buf[0x100] = { 0 };
    sceCodepage_driver_855C5C2E(name, sizeof(name), ln);
    int nent = lfn_entries(name) - 1;                                    /* 32 chars -> 3 fragments */
    assert(nent == 3 && lfn_entries(w("abc")) == 2 && lfn_entries(w("abcdefghijklm")) == 2);
    u8 frag[3][32];
    const u16 *q = name;
    for (int k = 0; k < nent; k++) q += lfn_fragment(q, frag[k]) / 2;
    assert(frag[2][16] == 0 && frag[2][17] == 0 && frag[2][18] == 0xff); /* char 6 (ofs 16) = NUL, char 7.. = 0xffff */
    for (int k = nent - 1; k >= 0; k--) lfn_prepend(frag[k], buf);      /* disk order: highest ordinal first */
    assert(weq(buf, ln));

    printf("ALL PATH TESTS PASSED\n");
    return 0;
}
