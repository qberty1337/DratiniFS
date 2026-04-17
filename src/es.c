// extremespeed raw iso partition
// separate translation so gcc doesn't change optimization decisions for pre-es vfs functions from the main.c (at least... that's the idea...)

#include <pspkernel.h>
#include <pspiofilemgr.h>
#include <string.h>
#include "es.h"

// more globals

int es_initialized = 0;
int es_iso_count = 0;
ExtremeSpeedEntry es_table[ES_MAX_ISOS];
static unsigned int es_partition_start = 0;
static unsigned int es_data_start_sec = 0;
char es_dir_paths[16][256];

// cache blk_fd locally to try and avoid cross-translation unit accessor calls on every read (I couldn't get the POC to work without this tbh)
static int es_cached_blk_fd = -1;
static unsigned long long es_cached_pos = 0xFFFFFFFFFFFFFFFFULL;

static void es_sync_fd(void)
{
    es_cached_blk_fd = es_get_blk_fd();
    es_cached_pos = es_get_blk_fd_pos();
}

// force the cache to be invalidated
void es_sync_blk_fd(void)
{
    es_cached_blk_fd = -1;
    es_cached_pos = 0xFFFFFFFFFFFFFFFFULL;
}

static int es_read_abs(unsigned int abs_sector, void *buf, unsigned int count)
{
    if (es_cached_blk_fd < 0) es_sync_fd();
    if (es_cached_blk_fd < 0) return -1;
    unsigned long long byte_off = (unsigned long long)abs_sector * 512;
    // check real fd position since es_cached_pos goes stale when main does io through blk_write_sectors typically
    if (byte_off != es_get_blk_fd_pos()) {
        long long sr = k_sceIoLseek64k(es_cached_blk_fd, 0,
            (unsigned int)(byte_off & 0xFFFFFFFF),
            (unsigned int)(byte_off >> 32), 0);
        if (sr < 0) return (int)sr;
    }
    if (count > 0x800000) return -1;
    unsigned int nbytes = count * 512;
    int bytes = k_sceIoRead(es_cached_blk_fd, buf, nbytes);
    if (bytes > 0) {
        es_cached_pos = byte_off + bytes;
        es_set_blk_fd_pos(es_cached_pos);
    } else {
        es_cached_pos = 0xFFFFFFFFFFFFFFFFULL;
        es_set_blk_fd_pos(es_cached_pos);
    }
    return bytes;
}

static int es_write_abs(unsigned int abs_sector, const void *buf, unsigned int count)
{
    int fd = es_get_blk_fd();
    if (fd < 0) return -1;
    unsigned long long byte_off = (unsigned long long)abs_sector * 512;
    if (byte_off != es_get_blk_fd_pos()) {
        long long sr = k_sceIoLseek64k(fd, 0,
            (unsigned int)(byte_off & 0xFFFFFFFF),
            (unsigned int)(byte_off >> 32), 0);
        if (sr < 0) return (int)sr;
    }
    if (count > 0x800000) return -1;
    unsigned int nbytes = count * 512;
    int bytes = k_sceIoWrite(fd, buf, nbytes);
    if (bytes > 0) {
        es_set_blk_fd_pos(byte_off + bytes);
        es_cached_pos = byte_off + bytes;
    } else {
        es_set_blk_fd_pos(0xFFFFFFFFFFFFFFFFULL);
        es_cached_pos = 0xFFFFFFFFFFFFFFFFULL;
    }
    return bytes;
}

static int es_stricmp(const char *a, const char *b)
{
    while (*a && *b) {
        char ca = *a, cb = *b;
        if (ca >= 'A' && ca <= 'Z') ca += 32;
        if (cb >= 'A' && cb <= 'Z') cb += 32;
        if (ca != cb) return ca - cb;
        a++; b++;
    }
    return (unsigned char)*a - (unsigned char)*b;
}

static int es_find_iso(const char *file)
{
    if (!es_initialized || es_iso_count <= 0) return -1;
    while (*file == '/') file++;
    int i;
    for (i = 0; i < es_iso_count; i++) {
        if (!(es_table[i].flags & ES_FLAG_ACTIVE)) continue;
        if (es_stricmp(file, es_table[i].filename) == 0)
            return i;
    }
    return -1;
}

// check whether any active es entry's filename begins with the given directory prefix followed by '/'.
// used to expose virtual subfolders like "ISO/Genre/" that only exist inside es entries.
int es_check_dir_prefix(const char *dir)
{
    if (!es_initialized || es_iso_count <= 0) return 0;
    while (*dir == '/') dir++;
    int dlen = 0;
    while (dir[dlen]) dlen++;
    if (dlen == 0) return 0;

    int i;
    for (i = 0; i < es_iso_count; i++) {
        if (!(es_table[i].flags & ES_FLAG_ACTIVE)) continue;
        const char *fn = es_table[i].filename;
        int match = 1, j;
        for (j = 0; j < dlen; j++) {
            char a = fn[j], b = dir[j];
            if (a >= 'A' && a <= 'Z') a += 32;
            if (b >= 'A' && b <= 'Z') b += 32;
            if (a != b) { match = 0; break; }
        }
        if (match && fn[dlen] == '/') return 1;
    }
    return 0;
}

static unsigned int es_next_free(void)
{
    unsigned int next = es_data_start_sec;
    int i;
    for (i = 0; i < es_iso_count; i++) {
        if (!(es_table[i].flags & ES_FLAG_ACTIVE)) continue;
        unsigned int end = es_table[i].start_sector + es_table[i].size_sectors;
        if (end < es_table[i].start_sector) continue;
        if (end > next) next = end;
    }
    return next;
}

// pub stuffs

typedef struct {
    int in_use;
    int is_dir;
    int writable;
    int is_dirty;
    unsigned int first_cluster;
    unsigned int orig_first_cluster;
    unsigned int file_size;
    unsigned int position;
    unsigned int tail_cluster;
    unsigned int parent_cluster;
    unsigned int dir_cluster;
    unsigned int dir_offset;
    unsigned int cur_cluster;
    unsigned int cur_cluster_index;
    char lfn_buf[256]; // 56 pin this down!!
    int lfn_ready;
    int no_fat_chain;
    int parent_no_fat_chain; // 320 pinm htis down too!! 
    unsigned int sfn_sector; // 324
    unsigned int sfn_index;
    int msstor_fd;
} EsOpenFile;

int es_try_open(PspIoDrvFileArg *arg, char *file, void *open_files_ptr, int max_files)
{
    int es_idx = es_find_iso(file);
    if (es_idx < 0) return -1;

    // look for free fd's... probably is a better way to do this...
    EsOpenFile *files = (EsOpenFile *)open_files_ptr;
    int fd = -1, i;
    for (i = 0; i < max_files; i++) {
        if (!files[i].in_use) { fd = i; break; }
    }
    if (fd < 0) return 0x80000020;

    memset(&files[fd], 0, sizeof(EsOpenFile));
    files[fd].in_use = 1;
    files[fd].file_size = es_table[es_idx].size_lo;
    files[fd].first_cluster = 0xE5000000 | (unsigned int)es_idx;
    files[fd].no_fat_chain = 2;
    arg->arg = (void *)fd;
    return 0;
}

int es_io_read(void *file_ptr, char *data, int len)
{
    EsOpenFile *f = (EsOpenFile *)file_ptr;
    int es_idx = (int)(f->first_cluster & 0x00FFFFFF);
    if (es_idx < 0 || es_idx >= es_iso_count) return -1;

    const ExtremeSpeedEntry *ent = &es_table[es_idx];
    unsigned int remaining = f->file_size - f->position;
    if ((unsigned int)len > remaining) len = (int)remaining;
    if (len <= 0) return 0;

    int total_read = 0;
    while (len > 0) {
        unsigned int sec_in_iso = f->position / 512;
        unsigned int sec_offset = f->position % 512;
        unsigned int abs_sector = es_partition_start + ent->start_sector + sec_in_iso;

        if (sec_offset == 0 && (unsigned int)len >= 512
            && ((unsigned int)(unsigned long)data & 3) == 0) {
            unsigned int nsectors = ((unsigned int)len) / 512;
            unsigned int iso_left = ent->size_sectors - sec_in_iso;
            if (nsectors > iso_left) nsectors = iso_left;
            int ret = es_read_abs(abs_sector, data, nsectors);
            if (ret < 0) return total_read > 0 ? total_read : ret;
            unsigned int bulk = nsectors * 512;
            data += bulk; f->position += bulk;
            total_read += bulk; len -= bulk;
        } else {
            static unsigned char es_bounce[512] __attribute__((aligned(64)));
            int ret = es_read_abs(abs_sector, es_bounce, 1);
            if (ret < 0) return total_read > 0 ? total_read : ret;
            unsigned int chunk = 512 - sec_offset;
            if (chunk > (unsigned int)len) chunk = (unsigned int)len;
            // increase performance by doing a memcpy instead of a dcache flush like sony does
            memcpy(data, es_bounce + sec_offset, chunk);
            data += chunk; f->position += chunk;
            total_read += chunk; len -= chunk;
        }
    }
    return total_read;
}

int es_try_getstat(const char *file, SceIoStat *stat)
{
    int es_idx = es_find_iso(file);
    if (es_idx < 0) return -1;
    memset(stat, 0, sizeof(SceIoStat));
    stat->st_size = es_table[es_idx].size_lo;
    stat->st_mode = 0x21FF;
    stat->st_attr = 0x20;
    // use magic date 1992-03-27 (my birthday!) to mark es entries (check the ISO's date in XMB :p)
    stat->sce_st_ctime.year = 1992; stat->sce_st_ctime.month = 3; stat->sce_st_ctime.day = 27;
    stat->sce_st_mtime.year = 1992; stat->sce_st_mtime.month = 3; stat->sce_st_mtime.day = 27;
    stat->sce_st_atime.year = 1992; stat->sce_st_atime.month = 3; stat->sce_st_atime.day = 27;
    return 0;
}

int es_try_ioctl(int no_fat_chain)
{
    return (no_fat_chain == 2) ? 0 : -1;
}

int es_try_remove(const char *file)
{
    int idx = es_find_iso(file);
    if (idx < 0) return -1;

    es_table[idx].flags &= ~ES_FLAG_ACTIVE;

    // switch to stricter atomic writes, previous attempt kinda messes with cross sector etc..
    es_write_abs(es_partition_start + 1 + idx, &es_table[idx], 1);
    return 0;
}

void es_save_dirname(int fd, const char *dirname)
{
    if (fd < 0 || fd >= 16) return;
    const char *dp = dirname;
    while (*dp == '/') dp++;
    int dlen = 0;
    while (dp[dlen] && dlen < 255) dlen++;
    memcpy(es_dir_paths[fd], dp, dlen);
    if (dlen > 0 && es_dir_paths[fd][dlen - 1] == '/') dlen--;
    es_dir_paths[fd][dlen] = '\0';
}

// case-insensitive compare of up to dlen bytes; returns 1 if equal
static int es_prefix_iequal(const char *a, const char *b, int dlen)
{
    int j;
    for (j = 0; j < dlen; j++) {
        char ca = a[j], cb = b[j];
        if (ca >= 'A' && ca <= 'Z') ca += 32;
        if (cb >= 'A' && cb <= 'Z') cb += 32;
        if (ca != cb) return 0;
    }
    return 1;
}

// !NOTE: out must have room for at least 256 bytes.
static int es_extract_subdir(const char *fn, const char *opened_dir, int opened_len, char *out)
{
    int skip;
    if (opened_len == 0) {
        skip = 0;
    } else {
        if (!es_prefix_iequal(fn, opened_dir, opened_len)) return 0;
        if (fn[opened_len] != '/') return 0;
        skip = opened_len + 1;
    }

    // find next '/' after skip. no next slash means it's a direct file, not a subdir.
    int i = 0;
    while (fn[skip + i] && fn[skip + i] != '/') {
        if (i >= 255) return 0;
        out[i] = fn[skip + i];
        i++;
    }
    if (fn[skip + i] != '/') return 0;
    out[i] = '\0';
    return i;
}

int es_dread_overlay(int fd, SceIoDirent *dir, int *iter)
{
    if (!es_initialized || es_iso_count <= 0) return 0;
    if (fd < 0 || fd >= 16) return 0;
    if (!iter) return 0;

    const char *opened_dir = es_dir_paths[fd];
    int opened_len = 0;
    while (opened_dir[opened_len]) opened_len++;

    // two-phase iterator:
    //   phase 1 (bit 31 clear): scan for files whose dir_part == opened_dir
    //   phase 2 (bit 31 set):   synthesize unique immediate subdirectory names
    // bits 0..30 hold the current table index within the phase.
    // ... maybe strike that - reverse it
    unsigned int raw = (unsigned int)*iter;
    int phase2 = (raw & 0x80000000u) ? 1 : 0;
    int idx = (int)(raw & 0x7FFFFFFFu);

    if (!phase2) {
        // ── phase 1: direct file matches ──
        while (idx < es_iso_count) {
            int cur = idx++;

            if (!(es_table[cur].flags & ES_FLAG_ACTIVE)) continue;
            if (!es_table[cur].filename[0]) continue;

            // extract directory and basename from es_table filename.
            // filenames look like "ISO/game.iso" or "ISO/Genre/game.iso".
            const char *fn = es_table[cur].filename;
            const char *last_slash = 0;
            const char *p;
            for (p = fn; *p; p++) {
                if (*p == '/') last_slash = p;
            }

            const char *base_name;
            char dir_part[256];
            if (last_slash) {
                int dlen = (int)(last_slash - fn);
                if (dlen > 255) dlen = 255;
                int di;
                for (di = 0; di < dlen; di++) dir_part[di] = fn[di];
                dir_part[dlen] = '\0';
                base_name = last_slash + 1;
            } else {
                dir_part[0] = '\0';
                base_name = fn;
            }

            const char *a = opened_dir;
            const char *b = dir_part;
            int match = 1;
            while (*a && *b) {
                char ca = *a, cb = *b;
                if (ca >= 'a' && ca <= 'z') ca -= 32;
                if (cb >= 'a' && cb <= 'z') cb -= 32;
                if (ca != cb) { match = 0; break; }
                a++; b++;
            }
            if (*a || *b) match = 0;

            if (!match) continue;

            memset(dir, 0, sizeof(SceIoDirent));
            int ni;
            for (ni = 0; base_name[ni] && ni < 255; ni++)
                dir->d_name[ni] = base_name[ni];
            dir->d_name[ni] = '\0';

            dir->d_stat.st_size = es_table[cur].size_lo;
            dir->d_stat.st_mode = 0x21FF; // regular file
            dir->d_stat.st_attr = 0x20;   // archive
            // magic dates!!
            dir->d_stat.sce_st_ctime.year = 1992; dir->d_stat.sce_st_ctime.month = 3; dir->d_stat.sce_st_ctime.day = 27;
            dir->d_stat.sce_st_mtime.year = 1992; dir->d_stat.sce_st_mtime.month = 3; dir->d_stat.sce_st_mtime.day = 27;
            dir->d_stat.sce_st_atime.year = 1992; dir->d_stat.sce_st_atime.month = 3; dir->d_stat.sce_st_atime.day = 27;

            *iter = (int)((unsigned int)idx & 0x7FFFFFFFu);
            return 1;
        }
        // phase 1 exhausted - transition to phase 2
        phase2 = 1;
        idx = 0;
    }

    // let's try to help GCLite better...
    // for each entry whose filename is "opened_dir/<sub>/..." emit <sub> once,
    // deduping against earlier entries at indices 0..cur-1.
    while (idx < es_iso_count) {
        int cur = idx++;
        if (!(es_table[cur].flags & ES_FLAG_ACTIVE)) continue;
        if (!es_table[cur].filename[0]) continue;

        char sub_name[256];
        int slen = es_extract_subdir(es_table[cur].filename, opened_dir,
                                      opened_len, sub_name);
        if (slen <= 0) continue;

        // dedupe: skip if any earlier active entry yields the same sub_name
        int dup = 0;
        int k;
        for (k = 0; k < cur; k++) {
            if (!(es_table[k].flags & ES_FLAG_ACTIVE)) continue;
            if (!es_table[k].filename[0]) continue;
            char prev[256];
            int plen = es_extract_subdir(es_table[k].filename, opened_dir,
                                          opened_len, prev);
            if (plen != slen) continue;
            int eq = 1, m;
            for (m = 0; m < slen; m++) {
                char ca = prev[m], cb = sub_name[m];
                if (ca >= 'A' && ca <= 'Z') ca += 32;
                if (cb >= 'A' && cb <= 'Z') cb += 32;
                if (ca != cb) { eq = 0; break; }
            }
            if (eq) { dup = 1; break; }
        }
        if (dup) continue;

        memset(dir, 0, sizeof(SceIoDirent));
        int ni;
        for (ni = 0; ni < slen && ni < 255; ni++)
            dir->d_name[ni] = sub_name[ni];
        dir->d_name[ni] = '\0';

        dir->d_stat.st_size = 0;
        dir->d_stat.st_mode = 0x11FF; // directory
        dir->d_stat.st_attr = 0x10;   // FAT directory attribute
        dir->d_stat.sce_st_ctime.year = 1992; dir->d_stat.sce_st_ctime.month = 3; dir->d_stat.sce_st_ctime.day = 27;
        dir->d_stat.sce_st_mtime.year = 1992; dir->d_stat.sce_st_mtime.month = 3; dir->d_stat.sce_st_mtime.day = 27;
        dir->d_stat.sce_st_atime.year = 1992; dir->d_stat.sce_st_atime.month = 3; dir->d_stat.sce_st_atime.day = 27;

        *iter = (int)(0x80000000u | ((unsigned int)idx & 0x7FFFFFFFu));
        return 1;
    }

    // both phases done - leave iter in a terminal state so later calls still return 0
    *iter = (int)(0x80000000u | (unsigned int)es_iso_count);
    return 0;
}

unsigned int es_mbr_find(const unsigned char *mbr_buf, unsigned int *out_num_sectors)
{
    if (mbr_buf[510] != 0x55 || mbr_buf[511] != 0xAA) return 0;
    typedef struct {
        unsigned char status; unsigned char chs_first[3];
        unsigned char type; unsigned char chs_last[3];
        unsigned int lba_start; unsigned int num_sectors;
    } __attribute__((packed)) MBREntry;
    const MBREntry *entries = (const MBREntry *)(mbr_buf + 0x1BE);
    int i;
    for (i = 0; i < 4; i++) {
        if (entries[i].type == ES_PARTITION_TYPE && entries[i].lba_start > 0) {
            if (out_num_sectors) *out_num_sectors = entries[i].num_sectors;
            return entries[i].lba_start;
        }
    }
    return 0;
}

int es_init_partition(unsigned int partition_start)
{
    es_initialized = 0;
    es_iso_count = 0;
    // invalidate the cached fd on reinit other wise the stale fd prevents games from running
    // the controller doesn't handle this with sony's default setup right? 
    es_cached_blk_fd = -1;
    es_cached_pos = 0xFFFFFFFFFFFFFFFFULL;
    es_data_start_sec = 0;
    es_partition_start = partition_start;

    if (partition_start == 0) return -1;

    unsigned char sb[512] __attribute__((aligned(64)));
    int r = es_read_abs(partition_start, sb, 1);
    if (r < 0) return -2;

    const ExtremeSpeedSuperblock *super = (const ExtremeSpeedSuperblock *)sb;
    if (super->magic != ES_MAGIC || super->version != ES_VERSION) return -3;

    es_data_start_sec = super->data_start_sector;
    int count = (int)super->iso_count;
    if (count > ES_MAX_ISOS) count = ES_MAX_ISOS;

    if (count > 0) {
        int i;
        for (i = 0; i < count; i++) {
            r = es_read_abs(partition_start + 1 + i, &es_table[i], 1);
            if (r < 0) return -4;
        }
    }

    es_iso_count = count;
    es_initialized = 1;
    return 0;
}

int es_devctl_read_sector(void *indata, int inlen)
{
    if (!indata || inlen < (int)sizeof(ExtremeSpeedDevCtlArg)) return 0x80010016;
    const ExtremeSpeedDevCtlArg *a = (const ExtremeSpeedDevCtlArg *)indata;
    if (!a->buffer || a->count == 0) return 0x80010016;
    return es_read_abs(a->sector, a->buffer, a->count);
}

int es_devctl_write_sector(void *indata, int inlen)
{
    if (!indata || inlen < (int)sizeof(ExtremeSpeedDevCtlArg)) return 0x80010016;
    const ExtremeSpeedDevCtlArg *a = (const ExtremeSpeedDevCtlArg *)indata;
    if (!a->buffer || a->count == 0) return 0x80010016;
    return es_write_abs(a->sector, a->buffer, a->count);
}

int es_devctl_get_info(void *outdata, int outlen)
{
    if (!outdata || outlen < (int)sizeof(ExtremeSpeedInfo)) return 0x80010016;
    ExtremeSpeedInfo *info = (ExtremeSpeedInfo *)outdata;
    info->initialized = es_initialized;
    info->partition_start_sector = es_partition_start;
    info->total_sectors = 0;
    info->iso_count = (unsigned int)es_iso_count;
    info->data_start_sector = es_data_start_sec;
    info->free_sector = es_next_free();
    if (es_initialized && es_partition_start > 0) {
        unsigned char sb[512] __attribute__((aligned(64)));
        if (es_read_abs(es_partition_start, sb, 1) >= 0) {
            const ExtremeSpeedSuperblock *s = (const ExtremeSpeedSuperblock *)sb;
            if (s->magic == ES_MAGIC) info->total_sectors = s->total_sectors;
        }
    }
    return 0;
}

int es_devctl_rescan(void)
{
    unsigned char mbr[512] __attribute__((aligned(64)));
    if (es_read_abs(0, mbr, 1) < 0) {
        es_initialized = 0;
        es_iso_count = 0;
        return -1;
    }
    unsigned int ns = 0;
    unsigned int lba = es_mbr_find(mbr, &ns);
    return es_init_partition(lba);
}
