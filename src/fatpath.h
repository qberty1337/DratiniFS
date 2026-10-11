/* fatpath.h - Sony 6.61 fatms path + name helpers (pure; host-tested by test_path.c).
 *
 *   fp_normalize  = FUN_0000a120 (+ FUN_00009f80/a09c/a0c8/a0ec)  leading '\', "."/".." folding
 *   fp_split      = FUN_000099fc  {parent path, leaf, 8.3-or-LFN, ready 8.3 string}
 *   fp_namecmp    = FUN_000014f4  ASCII-case-insensitive UCS-2 compare
 *   fp_wildmatch  = FUN_00001598  dread wildcard matcher (* ?), sfn_mode = 8.3 dot rules
 *   fp_char_valid = FUN_00009924, fp_needs_lfn = FUN_00009ee0
 *   sfn_to_str    = FUN_00000634, str_to_sfn = FUN_000006d8
 *   lfn_prepend   = FUN_0000078c, lfn_fragment = FUN_00000940, lfn_entries = FUN_000008a0,
 *   lfn_checksum  = FUN_00000904
 *   alias_init    = FUN_0000a3cc, alias_next = FUN_0000a73c (+ FUN_0000a8f4)
 * Codepage conversions go through sceCodepage_driver (stubbed ASCII-only in the host test). */
#ifndef FATPATH_H
#define FATPATH_H

#if defined(FATCACHE_HOST) && !defined(FAT_HOST_TYPES)
#define FAT_HOST_TYPES
#include <stdint.h>
typedef uint8_t  u8;
typedef uint16_t u16;
typedef uint32_t u32;
#endif

/* FUN_000099fc output (0x41c bytes) */
typedef struct {
    u32  need_lfn;            /* +0x000 1 = leaf needs an LFN run */
    u16  dir[0x104];          /* +0x004 parent path, ends with '\' */
    u16  leaf[0x100];         /* +0x20c leaf name */
    char sfn[0x10];           /* +0x40c "NAME.EXT" (OEM) when need_lfn == 0 */
} fpath;

_Static_assert(sizeof(fpath) == 0x41c, "FUN_000099fc output layout");

typedef struct { int d[6]; int pos; } fp_alias;   /* ~N counter: digits ('0'..'9') + tail position */

extern int sceCodepage_driver_855C5C2E(u16 *dst, int cap, const char *src);   /* OEM (SJIS) -> UCS-2 */
extern int sceCodepage_driver_C899572E(u16 *dst, int cap, const char *src);   /* UTF-8 -> UCS-2 */
extern int sceCodepage_driver_47BDF633(char *dst, int cap, const u16 *src);   /* UCS-2 -> OEM */
extern int sceCodepage_driver_907CBFD2(char *dst, int cap, const u16 *src);   /* UCS-2 -> UTF-8 */
extern int sceCodepage_driver_11123ED1(int ch);          /* nonzero: UCS-2 char is double-byte in OEM */
extern int sceCodepage_driver_014E0C72(const void *p);   /* nonzero: *p is an OEM lead byte */

int  fp_wcslen(const u16 *s);
void fp_wcscpy(u16 *d, const u16 *s);
void fp_wcsncpy(u16 *d, const u16 *s, u32 n);
int  fp_char_valid(u16 c, int allow_wild);
int  fp_needs_lfn(const u16 *s);
int  fp_normalize(u16 *path, u16 *tmp);              /* tmp: >= 0x800 bytes */
int  fp_split(fpath *p, u16 *path, u16 *leafbuf, char *oem);   /* leafbuf 0x200, oem 0x100 bytes */
int  fp_namecmp(const u16 *a, const u16 *b);
int  fp_wildmatch(const u16 *pat, const u16 *name, int sfn_mode);
void sfn_to_str(char *out, const u8 *de);
void str_to_sfn(u8 *de, const char *s);
void lfn_prepend(const u8 *de, u16 *buf);            /* buf: 0x100 chars */
int  lfn_fragment(const u16 *name, u8 *de);
int  lfn_entries(const u16 *name);
u8   lfn_checksum(const u8 *sfn);
int  alias_init(fp_alias *a, u8 *sfn, const u16 *leaf, char *oem);   /* oem: 0x200 bytes */
int  alias_next(fp_alias *a, u8 *sfn);

#endif
