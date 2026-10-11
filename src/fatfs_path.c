/* fatfs_path.c - Sony 6.61 fatms path + name helpers (see fatpath.h for the FUN_ map).
 * Pure functions over caller-supplied buffers, so they host-test (test_path.c). */
#ifdef FATCACHE_HOST
#include <string.h>
#include "fatpath.h"
#else
#include "fatms.h"
#endif

#define FP_ENOENT (int)0x80010002
#define FP_EINVAL (int)0x80010016

static u16 up16(u16 c) { return (u16)((u16)(c - 'a') < 26 ? c - 0x20 : c); }   /* FUN_0000ab20 */
static int upc(int c) { return (c >= 'a' && c <= 'z') ? c - 0x20 : c; }   /* sysclib toupper */

int fp_wcslen(const u16 *s) { int n = 0; while (s[n]) n++; return n; }  /* FUN_0000ab44 */
void fp_wcscpy(u16 *d, const u16 *s) { while ((*d++ = *s++) != 0) ; }  /* FUN_0000ab68 */
void fp_wcsncpy(u16 *d, const u16 *s, u32 n)                           /* FUN_0000ac40 */
{
    u32 i = 0;
    for (; i < n && s[i]; i++) d[i] = s[i];
    for (; i < n; i++) d[i] = 0;
}

int fp_char_valid(u16 c, int allow_wild)                                /* FUN_00009924 */
{
    static const char bad[] = "\\\"./:<>|*?";
    for (const char *p = bad; ; p++) {
        if (c < 0x20 && c != 5) return FP_EINVAL;
        if (allow_wild && *p == '*') return 0;     /* '*' and everything after it in the list are allowed */
        if (*p == 0) return 0;
        if ((u16)*p == c) return FP_EINVAL;
    }
}

int fp_needs_lfn(const u16 *s)                                          /* FUN_00009ee0 */
{
    for (; *s; s++)
        if (*s > 0x7f || (u16)(*s - 'a') < 26 || *s == '=' || *s == '[' || *s == ']' ||
            *s == ';' || *s == '+' || *s == ',') return 1;
    return 0;
}

/* FUN_00009f80: fold all-dot components ("." = stay, ".." = up one, "..." = up two ...) */
static int fp_dots(u16 *path)
{
    int fresh = 1, ndots = 0;
    u16 *s = path, ch;
    do {
        ch = *s;
        if (ch == '.' && fresh) ndots++;
        else {
            if (ch == '\\' || ch == 0) {
                if (ch == 0) {
                    if (!fresh) {                              /* FUN_0000a0ec: strip the leaf's trailing dots */
                        if (s[-1] == '.') { s[-1] = 0; for (u16 *q = s - 1; q[-1] == '.'; q--) q[-1] = 0; }
                        return 0;
                    }
                    if (s[-1] == '.') { s[0] = '\\'; s[1] = 0; }   /* FUN_0000a0c8 */
                }
                if (ndots > 0) {
                    u16 *after = s + 1;
                    for (int k = 0; k < ndots; k++) {          /* FUN_0000a09c, k times */
                        if (s <= path) return FP_ENOENT;       /* walked above the root */
                        do s--; while (*s != '\\' && path < s);
                    }
                    fp_wcscpy(s + 1, after);
                }
                ch = *s;
                fresh = 1;
            } else fresh = 0;
            ndots = 0;
        }
        s++;
    } while (ch != 0);
    return 0;
}

int fp_normalize(u16 *path, u16 *tmp)                                   /* FUN_0000a120 */
{
    u16 *d = tmp;
    if (path[0] != '\\') *d++ = '\\';
    fp_wcscpy(d, path);
    for (u16 *q = tmp; *q; q++)
        if (*q == '.') { int r = fp_dots(tmp); if (r < 0) return r; break; }
    if (fp_wcslen(tmp) >= 0x104) return FP_EINVAL;
    fp_wcscpy(path, tmp);
    return 0;
}

int fp_split(fpath *p, u16 *path, u16 *leaf, char *oem)                /* FUN_000099fc */
{
    int n = fp_wcslen(path);
    if (n == 0) return FP_EINVAL;
    for (u16 *q = path + n; n > 0; ) { q--; n--; if (*q != '.') break; *q = 0; }   /* drop trailing dots */
    memset(leaf, 0, 0x200);
    memset(p, 0, sizeof(*p));
    p->need_lfn = 1;
    int state = 0, space = 0, multidot = 0, i = 0;                     /* state: 1 base, 2 ext */
    u32 base = 0, ext = 0, len = 0, dots = 0;
    for (const u16 *s = path; i < 0x105; i++, s++) {
        u16 ch = *s;
        if (ch == '.' || ch == ' ') {
            if (ch == ' ') space = 1;
            if (base == 0 || len == dots) base++;                      /* leading dots/spaces count as base */
            else {
                if (state == 0 || base > 0x1fe) return FP_EINVAL;
                if (state == 2) multidot = 1; else state = 2;
            }
            leaf[len++] = ch; dots++;
        } else if (ch == 0) {
            if (dots == len) return FP_EINVAL;                         /* empty or all-dot leaf */
            if ((state == 1 && base == 0) || (state == 2 && ext == 0)) return FP_EINVAL;
            if (len > 0xff) return FP_EINVAL;
            leaf[len] = 0;
            break;
        } else if (ch == '/' || ch == '\\') {
            dots = 0;
            if ((state == 1 && base == 0) || (state == 2 && ext == 0)) return FP_EINVAL;   /* "\\" or "x.\" */
            base = 0; state = 1; len = 0; ext = 0; space = 0; multidot = 0;
        } else {
            if (fp_char_valid(ch, 0) < 0) return FP_EINVAL;
            if (state == 1) { leaf[len++] = ch; base += (sceCodepage_driver_11123ED1(ch) != 0) + 1; }
            if (state == 2) { leaf[len++] = ch; ext += (sceCodepage_driver_11123ED1(ch) != 0) + 1; }
        }
    }
    if (len >= 0x100) return FP_EINVAL;
    memset(oem, 0, 0x100);
    if (base < 9 && ext < 4 && !multidot && len - 1 <= base + ext && !fp_needs_lfn(leaf) && !space) {
        int r = sceCodepage_driver_47BDF633(oem, 0x100, leaf);
        if (r < 0) return r;
        memcpy(p->sfn, oem, base);
        p->sfn[base] = '.';
        memcpy(p->sfn + base + 1, oem + base + 1, ext);
        p->need_lfn = 0;
        p->sfn[base + ext + 2] = 0;
        for (int k = 0; k < 0xc; k++) {
            if (sceCodepage_driver_014E0C72(p->sfn + k)) { k++; continue; }
            char c = p->sfn[k];
            if (c == '=' || c == '[' || c == ']' || c == ';' || c == '+' || c == ',') p->sfn[k] = '_';
        }
    }
    fp_wcsncpy(p->dir, path, (u32)i - len);
    fp_wcsncpy(p->leaf, leaf, 0x100);
    return 0;
}

int fp_namecmp(const u16 *a, const u16 *b)                              /* FUN_000014f4 */
{
    u32 ca = *a;
    if (ca != 0)
        while (*b != 0) {
            if (up16((u16)ca) != up16(*b)) break;
            ca = *++a; b++;
            if (ca == 0) break;
        }
    return (int)ca - (int)*b;
}

int fp_wildmatch(const u16 *pat, const u16 *name, int sfn_mode)         /* FUN_00001598 */
{
    int star = -1, st = 0, dots = 0, ni = 0, pi = 0, next;
    for (int k = 0; pat[k]; k++) dots += (pat[k] == '.');
    for (;;) {
        u16 c1, c2;
        for (;;) {                                  /* name ended: a lone '.' in the pattern matches nothing */
            c1 = up16(pat[pi]); c2 = up16(name[ni]);
            if (c2 != 0 || c1 != '.' || dots > 1) break;
            pi++;
        }
        if (c1 == '*') {
            star = pi + 1;
            if (pat[star] == 0) return 0;
            st = 1; next = star;
            if (sfn_mode == 1 && dots < 2 && pat[star] == '.') {   /* "*." in 8.3: jump to the name's dot */
                next = pi + 2;
                for (;;) {
                    st = 2;
                    if (name[ni] == '.') break;
                    if (name[ni] == 0) goto tail;
                    ni++;
                }
                ni++; st = 0;
            }
        } else if (c1 == '?' || c1 == c2) {
            ni++; next = pi + 1;
        } else {
            if (star == -1) return -1;
            ni++; next = pi;
            if (st == 2 || st == 0) return -1;
        }
tail:
        if (c1 == 0) return c2 == 0 ? 0 : -1;
        if (c2 == 0 && st != 2) return -1;
        pi = next;
    }
}

void sfn_to_str(char *out, const u8 *de)                                /* FUN_00000634 */
{
    int o = 0;
    for (int i = 0; i < 8 && de[i] != ' '; i++) out[o++] = (char)de[i];
    for (int i = 0; i < 3 && de[8 + i] != ' '; i++) { if (i == 0) out[o++] = '.'; out[o++] = (char)de[8 + i]; }
    out[o] = 0;
    if ((u8)out[0] == 0x05) out[0] = (char)0xe5;
}

void str_to_sfn(u8 *de, const char *s)                                  /* FUN_000006d8 */
{
    memset(de, ' ', 11);
    int i = 0, o = 0;
    for (;;) {
        char c = s[i];
        if (c == '.') break;
        i++;
        if (c == 0) goto done;
        de[o++] = (u8)c;
        if (o >= 8) break;
    }
    for (o = 0; o < 3; o++) { i++; if (s[i] == 0) break; de[8 + o] = (u8)s[i]; }
done:
    if (de[0] == 0xe5) de[0] = 0x05;
}

static const u8 lfn_ofs[13] = { 1, 3, 5, 7, 9, 14, 16, 18, 20, 22, 24, 28, 30 };   /* DAT_0000cf8c */

void lfn_prepend(const u8 *de, u16 *buf)                                /* FUN_0000078c */
{
    u16 frag[13];
    int n = fp_wcslen(buf), k = 0;
    for (; k < 13; k++) { const u8 *q = de + lfn_ofs[k]; if (!q[0] && !q[1]) break; frag[k] = (u16)(q[0] | q[1] << 8); }
    if (n + k < 0x100) {
        for (int i = n; i-- > 0; ) buf[i + k] = buf[i];                  /* shift right (no memmove import) */
        memcpy(buf, frag, (u32)k * 2); buf[n + k] = 0;
    }
    else { memset(buf, 0, 0x200); memcpy(buf, frag, (u32)k * 2); }   /* overflow: Sony keeps only the fragment */
}

int lfn_fragment(const u16 *name, u8 *de)                               /* FUN_00000940 */
{
    memset(de, 0, 32);
    int k = 0;
    for (; k < 13 && name[k]; k++) { de[lfn_ofs[k]] = (u8)name[k]; de[lfn_ofs[k] + 1] = (u8)(name[k] >> 8); }
    for (int j = k + 1; j < 13; j++) { de[lfn_ofs[j]] = 0xff; de[lfn_ofs[j] + 1] = 0xff; }
    return k * 2;
}

int lfn_entries(const u16 *name) { int n = fp_wcslen(name); return n / 13 + (n % 13 != 0) + 1; }  /* FUN_000008a0 */

u8 lfn_checksum(const u8 *sfn)                                          /* FUN_00000904 */
{
    u8 s = sfn[0];
    for (int i = 1; i < 11; i++) s = (u8)(((s & 1) << 7 | s >> 1) + sfn[i]);
    return s;
}

int alias_init(fp_alias *a, u8 *sfn, const u16 *leaf, char *oem)        /* FUN_0000a3cc */
{
    memset(a, 0, sizeof(*a));
    a->d[0] = '1';
    int r = sceCodepage_driver_47BDF633(oem, 0x200, leaf);
    if (r < 0) return r;
    int seen = 0, lossy = 0, extpos = 0;
    for (int i = 0; oem[i]; i++) {
        if ((oem[i] == '.' || oem[i] == ' ') && !seen) lossy = 1;      /* leading dots/spaces */
        else { seen = 1; if (oem[i] == '.') extpos = i + 1; }
    }
    memset(sfn, ' ', 11);
    int i = 0, o = 0;
    do {
        char c = oem[i];
        if (c == 0 && (!lossy || o > 8)) break;
        if (c == '.' && o == 0) { i++; continue; }
        if ((c == '.' && o != 6) || c == 0) {
            int last = (i == extpos - 1);
            i++;
            if (!last && c != 0) continue;                             /* inner dot: dropped */
            if (o < 6 || c == 0) {
                sfn[o] = '~'; a->pos = o + 1; sfn[o + 1] = (u8)a->d[0];
                if (c == 0) break;
            }
            o = 8; i = extpos;
        }
        if (o == 6) {                                                  /* base full: ~1 at 6..7 */
            sfn[6] = '~'; a->pos = 7; o = 8; sfn[7] = (u8)a->d[0]; i = extpos;
            if (extpos == 0) break;
        }
        c = oem[i];
        if (!sceCodepage_driver_014E0C72(oem + i)) {
            if (c != ' ') {
                if (c == '=' || c == '[' || c == ']' || c == ';' || c == '+' || c == ',') { c = '_'; lossy = 1; }
                sfn[o++] = (u8)upc((unsigned char)c);
            }
            i++;
        } else if (o < 5 || o == 8 || o == 9) {
            sfn[o] = (u8)c; sfn[o + 1] = (u8)oem[i + 1]; i += 2; o += 2;
        } else {
            if (o == 5) {                                              /* no room for a double-byte char */
                sfn[5] = '~'; a->pos = 6; o = 8; sfn[6] = (u8)a->d[0]; i = extpos;
                if (extpos == 0) break;
            }
            if (o == 10) break;
        }
    } while (o < 11);
    if (sfn[0] == 0xe5) sfn[0] = 0x05;
    return 0;
}

static void alias_widen(fp_alias *a, u8 *sfn, int n)                    /* FUN_0000a8f4 */
{
    if (a->pos == 7) {
        if (sceCodepage_driver_014E0C72(sfn + (7 - n))) { sfn[7] = ' '; a->pos--; }
    } else a->pos++;
    sfn[a->pos - n + 1] = '~';
}

int alias_next(fp_alias *a, u8 *sfn)                                    /* FUN_0000a73c */
{
    if (++a->d[0] == ':') {
        a->d[0] = '0';
        int k = 1;
        for (; k < 6; k++) {
            if (a->d[k] == 0) { a->d[k] = '1'; alias_widen(a, sfn, k + 2); break; }
            if (++a->d[k] != ':') break;
            if (k == 5) return FP_EINVAL;                              /* ~999999 exhausted */
            a->d[k] = '0';
        }
        for (int j = k; j >= 1; j--) sfn[a->pos - j] = (u8)a->d[j];
    }
    sfn[a->pos] = (u8)a->d[0];
    return 0;
}
