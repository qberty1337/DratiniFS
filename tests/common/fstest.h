/* fstest.h - the fatms conformance suite, shared by the host harness (tests/host) and the PSP
 * EBOOT (tests/psp). The suite only talks to the fs_* backend below, which has sceIo*
 * semantics: on the PSP it is sceIo* itself; on the host it is the baseline driver behind an
 * emulation of the iofilemgr behaviour that matters here (path parsing, fd read/write gating,
 * raw rename target). Expected results are Sony 6.61 fatms's (the baseline is a port of it). */
#ifndef FSTEST_H
#define FSTEST_H

#include <pspkernel.h>
#include <pspiofilemgr.h>
#include <string.h>

/* ---- backend: sceIo* semantics ---- */
int  fs_open(const char *path, int flags, int mode);
int  fs_close(int fd);
int  fs_read(int fd, void *buf, int n);
int  fs_write(int fd, const void *buf, int n);
s64  fs_lseek(int fd, s64 ofs, int whence);
int  fs_read_async(int fd, void *buf, int n);
int  fs_wait_async(int fd, s64 *res);
int  fs_remove(const char *path);
int  fs_mkdir(const char *path, int mode);
int  fs_rmdir(const char *path);
int  fs_rename(const char *oldpath, const char *newpath);
int  fs_dopen(const char *path);
int  fs_dread(int fd, SceIoDirent *d);
int  fs_dclose(int fd);
int  fs_getstat(const char *path, SceIoStat *st);
int  fs_chstat(const char *path, SceIoStat *st, int bits);
int  fs_devctl(const char *dev, unsigned int cmd, void *in, int inlen, void *out, int outlen);
int  fs_ioctl(int fd, unsigned int cmd, void *in, int inlen, void *out, int outlen);

/* ---- backend: environment ---- */
u32  fs_get_sdk(void);                       /* sceKernelGetCompiledSdkVersion() */
int  fs_threads(int n, int (*fn)(void *), void **args);   /* run n threads; 0 if all returned 0 */
int  fs_cb_create(void);                     /* a callback for the MS insert/eject devctls */
int  fs_cb_seen(int cb);                     /* last notify arg delivered to cb, -1 if none */
void fs_cb_delete(int cb);
u32  fs_now_ms(void);
void fs_print(const char *fmt, ...);         /* one line of output (no trailing newline needed) */
void fs_after_group(const char *name);       /* hook after each group (host: optional fsck) */

/* ---- the suite ---- */
#define FST_DETERMINISTIC 0x01   /* no threads/async: same bytes on disk every run (host golden image) */
#define FST_BIG           0x02   /* include the >1024-cluster file (cluster checkpoints, ~35 MB on a PSP) */
#define FST_KEEP          0x04   /* leave the test tree on the card */
#define FST_NAMES_ONLY    0x08   /* only the name / listing groups (second SDK-mode pass) */
#define FST_LARGE         0x10   /* files may pass 4 GB (exFAT under DratiniFS): a seek there succeeds */

typedef struct { int pass, fail, skip; } fst_result;

/* run every group under `root` (e.g. "ms0:/FATMSTEST"); results accumulate into *r */
void fst_run(const char *root, int flags, fst_result *r);

/* write / verify a deterministic file (also used by the PSP persistence check and the host harness) */
void fst_fill(u8 *buf, u32 seed, u32 off, u32 n);
int  fst_write_file(const char *path, u32 seed, u32 size);
int  fst_verify_file(const char *path, u32 seed, u32 size);   /* 0 = identical, else what differed */
int  fst_rm_tree(const char *path);

/* Sony 6.61 fatms / iofilemgr result codes */
#define E_ENOENT   (int)0x80010002
#define E_EBADF    (int)0x80010009
#define E_EACCES   (int)0x8001000D
#define E_EEXIST   (int)0x80010011
#define E_ENOTDIR  (int)0x80010014
#define E_EINVAL   (int)0x80010016
#define E_EMFILE   (int)0x80010018
#define E_IO_MFILE (int)0x80020320   /* iofilemgr: user-mode fd quota of the device (dev_type bits 16-23) */
#define E_IO_BADFD (int)0x80020323   /* iofilemgr: fd not open for that access */
#define E_IO_INVAL (int)0x80020324   /* iofilemgr: bad lseek whence */
#define E_IO_PERM  (int)0x800200D1   /* iofilemgr: kernel-only devctl (cmd bit 15) from user mode */
#define E_UNSUP    (int)0x80020325

#endif
