#include <pspkernel.h>
#include "es.h"
#include <pspiofilemgr_kernel.h>
#include <pspiofilemgr.h>
#include <pspthreadman.h>
#include <pspthreadman_kernel.h>
#include <pspsysevent.h>
#include <psploadcore.h>
#include <string.h>
#include "exfat_upcase.h"

// i'm not gonna abstract this out, let's monolith it and try to target performance where possible

extern SceModule *sceKernelFindModuleByName(const char *name);

// use mutex to match sony's fatms behavior (switch from semaphore during POC)
// no measurable performance difference (only theoretical)
SceUID sceKernelCreateMutex(const char *name, unsigned int attr, int initial_count, void *option);
int sceKernelLockMutex(SceUID mutex_id, int count, unsigned int *timeout);
int sceKernelUnlockMutex(SceUID mutex_id, int count);
int sceKernelDeleteMutex(SceUID mutex_id);

PSP_MODULE_INFO("sceMSFAT_Driver", PSP_MODULE_KERNEL | PSP_MODULE_SINGLE_START | PSP_MODULE_SINGLE_LOAD | PSP_MODULE_NO_STOP, 1, 0);

static SceUID blk_fd = -1;

// 64-bit lseek via iofilemgr nid 0x27eb27b8
// mips o32 abi for sceiolseek(fd, sceoff offset, int whence)
// all kernel i/o stubs prefixed k_ to avoid pspsdk symbol conflicts (broke my local CI/CD piopeline!! )
extern int sceKernelInitApitype(void);
extern int msstorCacheInit(const char *driver);
// are these all 13 nids from sony's 6.61 fatms? I couldn't find any documented list...
extern int sceCodepage_driver_EE932176(void);
extern int sceCodepage_driver_1D0DE569(int *o1, int *o2, int *o3, int *o4);
extern int sceCodepage_driver_039BF9E9(int,int,int,int,int,int,int,int); // load tables n' stuffs
extern void sceCodepage_driver_907CBFD2(void *dst, int dstlen, const void *src_ucs2); // ucs-2 → local
extern int sceCodepage_driver_47BDF633(void *dst, int dstlen, const void *src_ucs2); // ucs-2 → local (old sdk, lets account for it)
extern int sceCodepage_driver_855C5C2E(void *dst, int dstlen, const void *src); // sfn conversion
extern int sceCodepage_driver_DCD95405(int arg1);
extern int sceCodepage_driver_014E0C72(int arg1);
extern int sceCodepage_driver_0AA54D6D(int arg1);
extern int sceCodepage_driver_11123ED1(int arg1);
extern void sceCodepage_driver_266ABDD8(void);
extern int sceCodepage_driver_B0AE63AA(int arg1);
extern void sceCodepage_driver_C899572E(void *dst, int dstlen, const void *src);
extern SceUID k_sceIoOpen(const char *file, int flags, SceMode mode);
extern int k_sceIoClose(SceUID fd);
extern int k_sceIoRead(SceUID fd, void *data, SceSize size);
extern int k_sceIoWrite(SceUID fd, const void *data, SceSize size);
extern SceOff k_sceIoLseek32(SceUID fd, int offset, int whence);
extern int k_sceIoAddDrv(PspIoDrv *drv);
extern int k_sceIoDelDrv(const char *drv_name);
extern int k_sceIoAssign(const char *dev1, const char *dev2, const char *dev3, int mode, void *unk1, long unk2);
extern int k_sceIoUnassign(const char *dev);
extern long long k_sceIoLseek64k(int fd, int pad, unsigned int offset_lo, unsigned int offset_hi, int whence);
extern int k_sceIoIoctl(int fd, unsigned int cmd, void *indata, int inlen, void *outdata, int outlen);
extern int k_sceIoDevctl(const char *dev, unsigned int cmd, void *indata, int inlen, void *outdata, int outlen);

// fat32 on-disk structs

typedef struct {
    unsigned char jmp[3];
    unsigned char oem[8];
    unsigned short bytes_per_sector;
    unsigned char sectors_per_cluster;
    unsigned short reserved_sectors;
    unsigned char num_fats;
    unsigned short root_entry_count;
    unsigned short total_sectors_16;
    unsigned char media_type;
    unsigned short fat_size_16;
    unsigned short sectors_per_track;
    unsigned short num_heads;
    unsigned int hidden_sectors;
    unsigned int total_sectors_32;
    unsigned int fat_size_32;
    unsigned short ext_flags;
    unsigned short fs_version;
    unsigned int root_cluster;
    unsigned short fs_info;
    unsigned short backup_boot;
    unsigned char reserved2[12];
} __attribute__((packed)) Fat32BPB;

typedef struct {
    unsigned char name[11];
    unsigned char attr;
    unsigned char nt_reserved;
    unsigned char create_time_tenth;
    unsigned short create_time;
    unsigned short create_date;
    unsigned short access_date;
    unsigned short cluster_hi;
    unsigned short modify_time;
    unsigned short modify_date;
    unsigned short cluster_lo;
    unsigned int file_size;
} __attribute__((packed)) FatDirEntry;

typedef struct {
    unsigned char order;
    unsigned short name1[5];
    unsigned char attr;
    unsigned char type;
    unsigned char checksum;
    unsigned short name2[6];
    unsigned short cluster;
    unsigned short name3[2];
} __attribute__((packed)) FatLfnEntry;

// exfat directory entry types
#define EXFAT_ENTRY_BITMAP 0x81
#define EXFAT_ENTRY_FILE 0x85
#define EXFAT_ENTRY_STREAM 0xC0
#define EXFAT_ENTRY_NAME 0xC1
#define EXFAT_ATTR_DIRECTORY 0x0010
#define EXFAT_ATTR_ARCHIVE 0x0020

typedef struct {
    unsigned char entry_type;
    unsigned char secondary_count;
    unsigned short set_checksum;
    unsigned short file_attributes;  // 0x10 is dir
    unsigned short reserved1;
    unsigned int create_timestamp;
    unsigned int modify_timestamp;
    unsigned int access_timestamp;
    unsigned char create_10ms;
    unsigned char modify_10ms;
    unsigned char create_utc_offset;
    unsigned char modify_utc_offset;
    unsigned char access_utc_offset;
    unsigned char reserved2[7];
} __attribute__((packed)) ExFatFileEntry;

typedef struct {
    unsigned char entry_type;       // 0xc0
    unsigned char general_flags;    // bit1 is nofatchain (let's reuse the idea of spare flags)
    unsigned char reserved1;
    unsigned char name_length;
    unsigned short name_hash;
    unsigned short reserved2;
    unsigned long long valid_data_length;
    unsigned int reserved3;
    unsigned int first_cluster;
    unsigned long long data_length;  // file size
} __attribute__((packed)) ExFatStreamEntry;

typedef struct {
    unsigned char entry_type;       // 0xc1
    unsigned char general_flags;
    unsigned short name[15];         // utf-16le, 15 chars per entry
} __attribute__((packed)) ExFatNameEntry;

#define FAT_ATTR_READ_ONLY 0x01
#define FAT_ATTR_HIDDEN 0x02
#define FAT_ATTR_SYSTEM 0x04
#define FAT_ATTR_VOLUME_ID 0x08
#define FAT_ATTR_DIRECTORY 0x10
#define FAT_ATTR_ARCHIVE 0x20
#define FAT_ATTR_LFN 0x0F

#define FAT32_EOC 0x0FFFFFF8
#define FAT32_BAD 0x0FFFFFF7
#define FAT32_MASK 0x0FFFFFFF

// déclaration anticipée
static int fat32_init(void);
static int exfat_init(void);
static unsigned int mbr_find_fat32_partition(const unsigned char *mbr_buf);
static unsigned int mbr_find_exfat_partition(const unsigned char *mbr_buf, unsigned int *out_num_sectors);
static void fat_current_time(unsigned short *fdate, unsigned short *ftime);
static unsigned short exfat_entry_checksum(const unsigned char *es, int size);
static void deferred_fs_init(void);

// fat32 fs state
static unsigned int partition_start_sector = 0;
static SceUID fs_mutex = -1; // handler-level mutex - serializes all vfs ops (matches sony's fatms per-device mutex)
static unsigned long long blk_fd_pos = 0xFFFFFFFFFFFFFFFFULL; // track fd position to skip redundant seeks
int es_get_blk_fd(void) { return blk_fd; }
unsigned long long es_get_blk_fd_pos(void) { return blk_fd_pos; }
void es_set_blk_fd_pos(unsigned long long pos) { blk_fd_pos = pos; }
static int g_use_exfat = 0; // 1 = exfat filesystem, 0 = fat32 (should I add fat16 support?)
static volatile int fs_initialized = 0; // set by deferred_fs_init on first iodevctl

// detect stale blk_fd -- sceiocloseall kills kernel fds during game launch
static int blk_fd_validated = 0;
static void reopen_blk_fd_if_needed(void)
{
    if (blk_fd_validated) return;
    if (fs_initialized != 2) return;
    if (blk_fd >= 0) {
        // use 64-bit seek to avoid overflow on >2gb positions
        // k_sceiolseek32 returns int which overflows negative at 2gb
        // it kind of falsely kills a valid fd after exfat bitmap scan.
        long long r = k_sceIoLseek64k(blk_fd, 0, 0, 0, PSP_SEEK_CUR);
        if (r >= 0) { blk_fd_validated = 1; return; }
        blk_fd = -1; blk_fd_pos = 0xFFFFFFFFFFFFFFFFULL;
    }
    blk_fd = k_sceIoOpen("msstor0:", 0x04000003, 0);
    if (blk_fd >= 0) {
        k_sceIoIoctl(blk_fd, 0x02125803, NULL, 0, NULL, 0);
        blk_fd_validated = 1;
        blk_fd_pos = 0xFFFFFFFFFFFFFFFFULL;
        // invalidate es position cache (make this conditional!))
        es_sync_blk_fd();                    
    }
}

static unsigned int fat32_bytes_per_sector;
static unsigned int fat32_sectors_per_cluster;
static unsigned int fat32_cluster_size;
static unsigned int fat32_fat_start_sector;
static unsigned int fat32_fat_size;
static unsigned int fat32_data_start_sector;
static unsigned int fat32_root_cluster;
static unsigned int fat32_total_sectors;   // this comes from bpb
static unsigned int fat32_total_clusters; 
static unsigned int fat32_free_clusters;   // cached free cluster count (IMPORTANT! computed at init!!)
static unsigned int fat32_next_free_hint = 2;
static unsigned int exfat_free_clusters;

static unsigned char fat_sector_buf[512] __attribute__((aligned(64)));
static unsigned int fat_cached_sector = 0xFFFFFFFF;

// use sony's 661 fatms mount state
// 0x1022 = unmounted (no card or card rejected)
// 0x1020 = mounted (card present and filesystem accessible)
// 0x???? = another state I don't know abouit? lol
static volatile int mount_state = 0x1022;

// internal media state flags (sony seems to track these separately from mount_state)
// 0x2000 = media_present (physical card detected)
// 0x4000 = media_removed
// placeholder for more if we find more...
static volatile int media_flags = 0;

// sony's scefatmsmedia waits on this (do not change)
#define EVT_INSERT  0x01
#define EVT_REMOVE  0x02
static SceUID event_flag_id = -1;

// fix this if sony is registering a different way...
static int fatms_sysevent_handler(int ev_id, char *ev_name, void *param, int *result);
static PspSysEventHandler fatms_sysevent = {
    .size     = sizeof(PspSysEventHandler),
    .name     = "SceFatms",
    .type_mask= 0x00FFFF00,
    .handler  = fatms_sysevent_handler,
    .r28      = 0,
    .busy     = 0,
    .next     = NULL,
    .reserved = {0}
};

// write-through sector cache - moved above sysevent handler so suspend/eject can invalidate
#define BLK_WR_CACHE_ENTRIES 2
static unsigned char blk_wr_cache[BLK_WR_CACHE_ENTRIES][512] __attribute__((aligned(64)));
static unsigned int blk_wr_cache_sector[BLK_WR_CACHE_ENTRIES] = { 0xFFFFFFFF, 0xFFFFFFFF };
static int blk_wr_cache_age[BLK_WR_CACHE_ENTRIES] = { 0, 0 };
static int blk_wr_cache_clock = 0;

// exfat state
// never touches partition_start_sector or fat32_* (which the other threads use)
static unsigned int exfat_partition_start_sector;
static unsigned int exfat_bytes_per_sector;
static unsigned int exfat_sectors_per_cluster;
static unsigned int exfat_cluster_size;
static unsigned int exfat_fat_start_sector;
static unsigned int __attribute__((unused)) exfat_fat_size;
static unsigned int exfat_data_start_sector;
static unsigned int exfat_root_cluster;
static unsigned int exfat_cluster_count;
static unsigned int exfat_volume_sectors;
static unsigned int exfat_partition_num_sectors; // from mbr - physical partition size limit (make sure we have perms!)
static unsigned int exfat_bitmap_cluster = 0;
static unsigned int exfat_bitmap_size = 0;

static unsigned char exfat_fat_sector_buf[512] __attribute__((aligned(64)));
static unsigned int exfat_fat_cached_sector = 0xFFFFFFFF;

// xmb registers callbacks through iodevctl 0x02415821
#define MAX_CALLBACKS 32
static SceUID cb_uids[MAX_CALLBACKS];
static int cb_count = 0;

static void notify_all_callbacks(int arg)
{
    // sony uses suspendDispatch for atomic callbacks
    int intr = sceKernelSuspendDispatchThread();
    int i;
    for (i = 0; i < cb_count; i++) {
        int ret = sceKernelNotifyCallback(cb_uids[i], arg);
        if (ret == (int)0x800201A1) {
            cb_uids[i] = cb_uids[--cb_count];
            i--;
        }
    }
    if (intr >= 0) sceKernelResumeDispatchThread(intr);
}

// matches sony's sub_0b974 with interrupt protection (suspect / resume)
static int fatms_sysevent_handler(int ev_id, char *ev_name, void *param, int *result)
{
    (void)ev_name; (void)param; (void)result;
    // sony only uses cpuSuspend around the card-presence check + event
    // flag signal (sub_0b974). file i/o (k_sceioclose) must not run with
    // interrupts disabled - it can block waiting for hardware dma (bricked a 1k on cold boot because of this haha..)
    switch (ev_id) {
    case 0x00000102: 
        // suspend / usb mode entering
        if (blk_fd >= 0) {
            k_sceIoClose(blk_fd);
            blk_fd = -1; blk_fd_pos = 0xFFFFFFFFFFFFFFFFULL;
        }
        blk_fd_validated = 0;
        fat_cached_sector = 0xFFFFFFFF;
        exfat_fat_cached_sector = 0xFFFFFFFF;
        blk_wr_cache_sector[0] = 0xFFFFFFFF;
        blk_wr_cache_sector[1] = 0xFFFFFFFF;
        break;
    case 0x00010000: 
        // resume / usb exit
        blk_fd_validated = 0;
        break;
    case 0x00100000: {
        // disk change (eject) - close fd
        if (blk_fd >= 0) {
            k_sceIoClose(blk_fd);
            blk_fd = -1; blk_fd_pos = 0xFFFFFFFFFFFFFFFFULL;
        }
        blk_fd_validated = 0;
        blk_wr_cache_sector[0] = 0xFFFFFFFF;
        blk_wr_cache_sector[1] = 0xFFFFFFFF;
        mount_state = 0x1022;
        media_flags = 0x4000;
        notify_all_callbacks(2);
        // protect event flag signal with interrupt disable (matches sony's behavior)
        unsigned int intr = sceKernelCpuSuspendIntr();
        if (event_flag_id >= 0)
            sceKernelSetEventFlag(event_flag_id, EVT_REMOVE);
        sceKernelCpuResumeIntr(intr);
        break;
    }
    case 0x00100002:
        break;
    default:
        break;
    }
    return 0;
}

// open file table stuff
#define MAX_OPEN_FILES 32

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
    char lfn_buf[256];
    int lfn_ready;
    int no_fat_chain;
    int parent_no_fat_chain;
    unsigned int sfn_sector;       // sector containing this file's 8.3 dir entry
    unsigned int sfn_index;        // byte offset of entry within that sector
    SceUID msstor_fd;        // per-file msstor fd (like sony's per-file struct[116]) - no wayh around this for now
} OpenFile;

static OpenFile open_files[MAX_OPEN_FILES];

static void __attribute__((unused)) fat32_update_dir_size(unsigned int dir_cluster, unsigned int file_cluster,
                                   unsigned int new_size);

static int blk_read_sectors(unsigned int sector, void *buf, unsigned int count);
static int blk_write_sectors(unsigned int sector, const void *buf, unsigned int count);
static unsigned int cluster_to_sector(unsigned int cluster);
static unsigned int fat32_next_cluster(unsigned int cluster);

// blk_wr_cache moved above sysevent handler (see declaration near fatms_sysevent)
// static bounce buffer - sony uses fpl-allocated persistent struct[29], never stack
// had to switch from using our 4 bit aligned buf
static unsigned char blk_bounce_buf[512] __attribute__((aligned(64)));

// !NOTE: directory sector read cache. sony's driver caches this at a higher layer
// without caching, gclite's per-entry dread loop turns into constant block reads. I need to
// do this as gclite does some weird writing when selecting categories. every cat select is an i/o write :/
static unsigned int dir_rd_cache_sec = 0xFFFFFFFF;
static unsigned char dir_rd_cache_buf[512] __attribute__((aligned(64)));
static inline void dir_rd_cache_invalidate(void) { dir_rd_cache_sec = 0xFFFFFFFF; }


static void blk_wr_cache_push(unsigned int sector, const void *data)
{
    int i;
    for (i = 0; i < BLK_WR_CACHE_ENTRIES; i++) {
        if (blk_wr_cache_sector[i] == sector) {
            memcpy(blk_wr_cache[i], data, 512);
            blk_wr_cache_age[i] = ++blk_wr_cache_clock;
            return;
        }
    }
    // evict the oldest entry
    // bastardized lru when RAM flips that proverbial bit lol
    int oldest = 0;
    for (i = 1; i < BLK_WR_CACHE_ENTRIES; i++) {
        if (blk_wr_cache_age[i] < blk_wr_cache_age[oldest]) oldest = i;
    }
    memcpy(blk_wr_cache[oldest], data, 512);
    blk_wr_cache_sector[oldest] = sector;
    blk_wr_cache_age[oldest] = ++blk_wr_cache_clock;
}

static int blk_read_sectors(unsigned int sector, void *buf, unsigned int count)
{
    if (blk_fd < 0)
        return -1;
    unsigned int abs_sector = partition_start_sector + sector;
    if (count == 1) {
        int i;
        for (i = 0; i < BLK_WR_CACHE_ENTRIES; i++) {
            if (blk_wr_cache_sector[i] == abs_sector) {
                memcpy(buf, blk_wr_cache[i], 512);
                return 512;
            }
        }
    }
    unsigned long long byte_off = (unsigned long long)abs_sector * fat32_bytes_per_sector;
    // skip seek if fd is already at the right position (sony seems to seek once per cluster)
    if (byte_off != blk_fd_pos) {
        long long seek_ret = k_sceIoLseek64k(blk_fd, 0, (unsigned int)(byte_off & 0xFFFFFFFF),
                        (unsigned int)(byte_off >> 32), PSP_SEEK_SET);
        if (seek_ret < 0) return (int)seek_ret;
    }
    unsigned int nbytes = count * fat32_bytes_per_sector;
    int bytes = k_sceIoRead(blk_fd, buf, nbytes);
    if (bytes > 0) blk_fd_pos = byte_off + bytes;
    else blk_fd_pos = 0xFFFFFFFFFFFFFFFFULL;
    // cannot avoid it...so slowww...
    sceKernelDcacheWritebackInvalidateRange(buf, nbytes);
    return bytes;
}

static int __attribute__((unused)) blk_write_sectors(unsigned int sector, const void *buf, unsigned int count)
{
    if (blk_fd < 0)
        return -1;
    // any write could touch a cached dir sector, drop the cache conservatively (or try to lol)
    dir_rd_cache_invalidate();
    unsigned int abs_sector = partition_start_sector + sector;
    unsigned long long byte_off = (unsigned long long)abs_sector * fat32_bytes_per_sector;
    if (byte_off != blk_fd_pos) {
        long long seek_ret = k_sceIoLseek64k(blk_fd, 0, (unsigned int)(byte_off & 0xFFFFFFFF),
                        (unsigned int)(byte_off >> 32), PSP_SEEK_SET);
        if (seek_ret < 0) return (int)seek_ret;
    }
    unsigned int nbytes = count * fat32_bytes_per_sector;
    sceKernelDcacheWritebackInvalidateRange((void *)buf, nbytes);
    int bytes = k_sceIoWrite(blk_fd, buf, nbytes);
    if (bytes > 0) blk_fd_pos = byte_off + bytes;
    else blk_fd_pos = 0xFFFFFFFFFFFFFFFFULL;
    // don't cache here - data writes evict the dir sector cache
    // metadata writers (fat32_write_dir_entry) push to cache explicitly
    return bytes;
}

static unsigned int cluster_to_sector(unsigned int cluster)
{
    if (cluster < 2) return fat32_data_start_sector;
    return fat32_data_start_sector + (cluster - 2) * fat32_sectors_per_cluster;
}

static unsigned int fat32_next_cluster(unsigned int cluster)
{
    // no per-call lock - callers hold fs_mutex at handler level
    if (cluster < 2 || (fat32_total_clusters > 0 && cluster >= fat32_total_clusters + 2)) return FAT32_EOC;
    unsigned int fat_offset = cluster * 4;
    unsigned int fat_sector = fat32_fat_start_sector + (fat_offset / fat32_bytes_per_sector);
    unsigned int entry_offset = fat_offset % fat32_bytes_per_sector;

    if (fat_sector != fat_cached_sector) {
        int ret = blk_read_sectors(fat_sector, fat_sector_buf, 1);
        if (ret < 0)
            return FAT32_EOC;
        fat_cached_sector = fat_sector;
    }

    unsigned int val;
    memcpy(&val, fat_sector_buf + entry_offset, 4);
    return val & FAT32_MASK;
}

// - nofatchain (exfat contiguous): clusters are sequential
// - exfat with chain: use exfat_next_cluster (64-bit seek, no mask)
// - fat32: use fat32_next_cluster
// - maybe add fat16 support...
static unsigned int next_cluster_for(unsigned int cluster, int no_fat_chain);

static int __attribute__((unused)) exfat_blk_read_sectors(unsigned int sector, void *buf, unsigned int count)
{
    if (blk_fd < 0)
        return -1;
    // check thhe write-through cache - msstor returns stale data after write
    if (count == 1) {
        unsigned int abs_chk = exfat_partition_start_sector + sector;
        int i;
        for (i = 0; i < BLK_WR_CACHE_ENTRIES; i++) {
            if (blk_wr_cache_sector[i] == abs_chk) {
                memcpy(buf, blk_wr_cache[i], 512);
                return 512;
            }
        }
    }
    if (exfat_cluster_count > 0) {
        unsigned int max_sector = exfat_data_start_sector +
            exfat_cluster_count * exfat_sectors_per_cluster;
        if (max_sector > 0 && sector + count > max_sector)
            return -1;
    }
    unsigned int abs_sector = exfat_partition_start_sector + sector;
    unsigned long long byte_off = (unsigned long long)abs_sector * exfat_bytes_per_sector;
    if (byte_off != blk_fd_pos) {
        long long seek_ret = k_sceIoLseek64k(blk_fd, 0, (unsigned int)(byte_off & 0xFFFFFFFF),
                        (unsigned int)(byte_off >> 32), PSP_SEEK_SET);
        if (seek_ret < 0) return (int)seek_ret;
    }
    unsigned int nbytes = count * exfat_bytes_per_sector;
    int bytes = k_sceIoRead(blk_fd, buf, nbytes);
    if (bytes > 0) blk_fd_pos = byte_off + bytes;
    else blk_fd_pos = 0xFFFFFFFFFFFFFFFFULL;
    // :/.....zzzz....
    sceKernelDcacheWritebackInvalidateRange(buf, nbytes);
    return bytes;
}

static unsigned int __attribute__((unused)) exfat_cluster_to_sector(unsigned int cluster)
{
    if (cluster < 2) return exfat_data_start_sector;
    return exfat_data_start_sector + (cluster - 2) * exfat_sectors_per_cluster;
}

static unsigned int __attribute__((unused)) exfat_next_cluster(unsigned int cluster)
{
    // reject invalid cluster numbers, we'll deadlock if we don't
    if (cluster < 2 || cluster >= exfat_cluster_count + 2)
        return FAT32_EOC;
    unsigned int fat_offset = cluster * 4;
    unsigned int fat_sector = exfat_fat_start_sector + (fat_offset / exfat_bytes_per_sector);
    unsigned int entry_offset = fat_offset % exfat_bytes_per_sector;

    if (fat_sector != exfat_fat_cached_sector) {
        int ret = exfat_blk_read_sectors(fat_sector, exfat_fat_sector_buf, 1);
        if (ret < 0)
            return FAT32_EOC;
        exfat_fat_cached_sector = fat_sector;
    }

    unsigned int val;
    memcpy(&val, exfat_fat_sector_buf + entry_offset, 4);
    // no mask, full 32-bit cluster numbers, don't worry we can handle it...probably...
    return val;
}

static unsigned int next_cluster_for(unsigned int cluster, int no_fat_chain)
{
    if (no_fat_chain)
        return cluster + 1;
    if (g_use_exfat)
        return exfat_next_cluster(cluster);
    return fat32_next_cluster(cluster);
}

static unsigned int __attribute__((unused)) exfat_follow_chain(unsigned int start, unsigned int count)
{
    unsigned int cluster = start;
    unsigned int i;
    for (i = 0; i < count && cluster >= 2 && cluster < FAT32_EOC; i++)
        cluster = exfat_next_cluster(cluster);
    return cluster;
}

// exfat primies

static int exfat_blk_write_sectors(unsigned int sector, const void *buf, unsigned int count)
{
    if (blk_fd < 0) return -1;
    dir_rd_cache_invalidate();
    unsigned int abs_sector = exfat_partition_start_sector + sector;
    unsigned long long byte_off = (unsigned long long)abs_sector * exfat_bytes_per_sector;
    if (byte_off != blk_fd_pos) {
        long long seek_ret = k_sceIoLseek64k(blk_fd, 0, (unsigned int)(byte_off & 0xFFFFFFFF),
                        (unsigned int)(byte_off >> 32), PSP_SEEK_SET);
        if (seek_ret < 0) return (int)seek_ret;
    }
    unsigned int nbytes = count * exfat_bytes_per_sector;
    if (count == 1) blk_wr_cache_push(abs_sector, buf);
    sceKernelDcacheWritebackInvalidateRange((void *)buf, nbytes);
    int bytes = k_sceIoWrite(blk_fd, buf, nbytes);
    if (bytes > 0) blk_fd_pos = byte_off + bytes;
    else blk_fd_pos = 0xFFFFFFFFFFFFFFFFULL;
    return bytes;
}

static int exfat_write_fat_entry(unsigned int cluster, unsigned int value)
{
    unsigned int fat_offset   = cluster * 4;
    unsigned int fat_sector   = exfat_fat_start_sector + (fat_offset / exfat_bytes_per_sector);
    unsigned int entry_offset = fat_offset % exfat_bytes_per_sector;

    if (fat_sector != exfat_fat_cached_sector) {
        if (exfat_blk_read_sectors(fat_sector, exfat_fat_sector_buf, 1) < 0)
            return -1;
        exfat_fat_cached_sector = fat_sector;
    }
    memcpy(exfat_fat_sector_buf + entry_offset, &value, 4);
    return exfat_blk_write_sectors(fat_sector, exfat_fat_sector_buf, 1);
}

// free an exfat cluster - clear bitmap bit + zero fat entry
static void exfat_free_cluster(unsigned int cluster)
{
    if (cluster < 2 || cluster >= exfat_cluster_count + 2) return;
    // clear allocation bitmap bit first
    unsigned int idx = cluster - 2;
    unsigned int byte_off = idx / 8;
    unsigned int bit = idx % 8;
    unsigned int bmp_sec = exfat_data_start_sector +
        (exfat_bitmap_cluster - 2) * exfat_sectors_per_cluster +
        byte_off / 512;
    unsigned char bmp_buf[512] __attribute__((aligned(64)));
    if (exfat_blk_read_sectors(bmp_sec, bmp_buf, 1) >= 0) {
        bmp_buf[byte_off % 512] &= ~(1 << bit);
        exfat_blk_write_sectors(bmp_sec, bmp_buf, 1);
    }
    exfat_write_fat_entry(cluster, 0);
    exfat_free_clusters++;
}

static void exfat_free_chain(unsigned int cluster)
{
    while (cluster >= 2 && cluster < FAT32_EOC) {
        unsigned int next = exfat_next_cluster(cluster);
        exfat_free_cluster(cluster);
        cluster = next;
    }
}

// mark an exfat directory entry set as deleted (clear bit 7 of each entry type)
// dir_cluster = parent dir cluster, target_cluster = file/dir first_cluster to match
// dir_no_chain = 1 if parent dir uses contiguous allocation (nofatchain)
// this was a pain in the ass to get working. only mess with it if there's a better way... (I'm sure there is)
static int exfat_delete_dir_entry(unsigned int dir_cluster, unsigned int target_cluster, int dir_no_chain)
{
    unsigned char buf[512] __attribute__((aligned(64)));
    unsigned int cl = dir_cluster;
    while (cl >= 2 && cl < FAT32_EOC) {
        unsigned int sec_base = exfat_cluster_to_sector(cl);
        unsigned int s;
        for (s = 0; s < exfat_sectors_per_cluster; s++) {
            if (exfat_blk_read_sectors(sec_base + s, buf, 1) < 0) return -1;
            unsigned int off;
            for (off = 0; off < 512; off += 32) {
                unsigned char etype = buf[off];
                if (etype == 0x00) return 0x80010002; // not found
                if (etype != EXFAT_ENTRY_FILE) continue;
                ExFatFileEntry *fe = (ExFatFileEntry *)(buf + off);
                int sec_count = fe->secondary_count;
                unsigned int noff = off + 32;
                unsigned char stream_type = 0;
                unsigned int stream_cluster = 0;
                unsigned char buf2[512] __attribute__((aligned(64)));
                int have_buf2 = 0;
                if (noff < 512) {
                    stream_type = buf[noff];
                    if (stream_type == EXFAT_ENTRY_STREAM) {
                        ExFatStreamEntry *se = (ExFatStreamEntry *)(buf + noff);
                        stream_cluster = se->first_cluster;
                    }
                } else {
                    // stream entry is in next sector
                    unsigned int next_sec = sec_base + s + 1;
                    if (exfat_blk_read_sectors(next_sec, buf2, 1) >= 0) {
                        have_buf2 = 1;
                        unsigned int noff2 = noff - 512;
                        stream_type = buf2[noff2];
                        if (stream_type == EXFAT_ENTRY_STREAM) {
                            ExFatStreamEntry *se = (ExFatStreamEntry *)(buf2 + noff2);
                            stream_cluster = se->first_cluster;
                        }
                    }
                }
                if (stream_type == EXFAT_ENTRY_STREAM && stream_cluster == target_cluster) {
                    // mark all entries in this set as deleted (clear bit 7)
                    // !NOTE: handle cross-sector - entries may span current and next sector
                    int i;
                    int wrote_buf2 = 0;
                    for (i = 0; i <= sec_count; i++) {
                        unsigned int eoff = off + i * 32;
                        if (eoff < 512) {
                            buf[eoff] &= 0x7F;
                        } else {
                            if (!have_buf2) {
                                unsigned int next_sec = sec_base + s + 1;
                                if (exfat_blk_read_sectors(next_sec, buf2, 1) >= 0)
                                    have_buf2 = 1;
                                else break;
                            }
                            buf2[eoff - 512] &= 0x7F;
                            wrote_buf2 = 1;
                        }
                    }
                    exfat_blk_write_sectors(sec_base + s, buf, 1);
                    if (wrote_buf2)
                        exfat_blk_write_sectors(sec_base + s + 1, buf2, 1);
                    return 0;
                }
                // skip remaining secondary entries or it'll hang (for loop adds +32 for the file entry)
                off += sec_count * 32;
            }
        }
        if (dir_no_chain)
            cl = cl + 1;
        else
            cl = exfat_next_cluster(cl);
    }
    return 0x80010002;
}

// bespoke hint for the bitmap
static unsigned int exfat_next_free_hint = 0;

static unsigned int exfat_alloc_cluster(void)
{
    if (exfat_bitmap_cluster < 2 || exfat_bitmap_size == 0) return 0;

    unsigned int bitmap_sec_base = exfat_data_start_sector +
        (exfat_bitmap_cluster - 2) * exfat_sectors_per_cluster;
    unsigned char bmp_buf[512] __attribute__((aligned(64)));
    unsigned int total_bytes = exfat_bitmap_size;
    unsigned int hint_byte = exfat_next_free_hint;
    if (hint_byte >= total_bytes) hint_byte = 0;

    // two-pass scan: hint to end, then 0 to hint (wrap-around)
    int pass;
    for (pass = 0; pass < 2; pass++) {
        unsigned int abs_byte = (pass == 0) ? hint_byte : 0;
        unsigned int limit = (pass == 0) ? total_bytes : hint_byte;

        while (abs_byte < limit) {
            unsigned int sec_off = abs_byte / 512;
            unsigned int sector = bitmap_sec_base + sec_off;
            if (exfat_blk_read_sectors(sector, bmp_buf, 1) < 0) return 0;

            unsigned int start = abs_byte % 512;
            unsigned int end = limit - (sec_off * 512);
            if (end > 512) end = 512;

            unsigned int off;
            for (off = start; off < end; off++) {
                abs_byte = sec_off * 512 + off;
                if (bmp_buf[off] == 0xFF) continue; // all 8 bits allocated...
                int bit;
                for (bit = 0; bit < 8; bit++) {
                    if (!(bmp_buf[off] & (1 << bit))) {
                        unsigned int cluster = abs_byte * 8 + bit + 2;
                        if (cluster >= exfat_cluster_count + 2) continue;

                        // mark allocated in bitmap (interrupt-safe like I'm sure sony does)
                        {
                            unsigned int intr = sceKernelCpuSuspendIntr();
                            bmp_buf[off] |= (1 << bit);
                            sceKernelCpuResumeIntr(intr);
                        }
                        sceKernelDcacheWritebackInvalidateRange((void *)bmp_buf, 512);
                        exfat_blk_write_sectors(sector, bmp_buf, 1);

                        // mark in fat as eoc
                        exfat_write_fat_entry(cluster, 0xFFFFFFFF);
                        exfat_fat_cached_sector = 0xFFFFFFFF;
                        exfat_next_free_hint = abs_byte; // resume from here next time
                        if (exfat_free_clusters > 0) exfat_free_clusters--;
                        return cluster;
                    }
                }
            }
            abs_byte = sec_off * 512 + end;
        }
    }
    // if no free clusters...
    return 0;
}

static int exfat_zero_cluster_ex(unsigned int cluster)
{
    // IMPORTANT! write zeros directly - do not go through exfat_blk_write_sectors
    // because that pushes each sector to the 4-entry write cache, evicting
    // important metadata (parent dir entries) that ioopen needs immediately.
    unsigned char zero_buf[512] __attribute__((aligned(64)));
    memset(zero_buf, 0, 512);
    unsigned int base = exfat_cluster_to_sector(cluster);
    unsigned int abs_base = exfat_partition_start_sector + base;
    unsigned long long byte_off = (unsigned long long)abs_base * exfat_bytes_per_sector;
    if (byte_off != blk_fd_pos) {
        k_sceIoLseek64k(blk_fd, 0, (unsigned int)(byte_off & 0xFFFFFFFF),
                        (unsigned int)(byte_off >> 32), PSP_SEEK_SET);
    }
    unsigned int s;
    for (s = 0; s < exfat_sectors_per_cluster; s++) {
        int r = k_sceIoWrite(blk_fd, zero_buf, 512);
        if (r < 0) return -1;
    }
    blk_fd_pos = byte_off + (unsigned long long)exfat_sectors_per_cluster * 512;
    blk_wr_cache_push(abs_base, zero_buf);
    return 0;
}

static unsigned int exfat_extend_chain_ex(unsigned int tail_cluster)
{
    unsigned int newc = exfat_alloc_cluster();
    if (newc == 0) return 0;
    exfat_write_fat_entry(tail_cluster, newc);
    return newc;
}

// update the exfat directory entry for a file: size + first_cluster
// dir_cluster = cluster of the directory containing the file
// old_first_cl = the file's first_cluster (used to find the entry)
// new_size = updated data length
static void exfat_update_dir_entry(unsigned int dir_cluster,
                                   unsigned int old_first_cl,
                                   unsigned int new_first_cl,
                                   unsigned int new_size,
                                   int dir_no_chain)
{
    unsigned char buf[512] __attribute__((aligned(64)));
    unsigned int cl  = dir_cluster;
    unsigned int off = 0;

    while (cl >= 2 && cl < FAT32_EOC) {
        unsigned int sec = off / 512;
        unsigned int abs_sec = exfat_cluster_to_sector(cl) + sec;
        if (exfat_blk_read_sectors(abs_sec, buf, 1) < 0) return;
        unsigned int eoff = off % 512;

        while (eoff < 512) {
            unsigned char etype = buf[eoff];
            if (etype == 0x00) return;

            if (etype == EXFAT_ENTRY_FILE) {
                ExFatFileEntry *fe = (ExFatFileEntry *)(buf + eoff);
                int sec_count = fe->secondary_count;

                unsigned int noff = off + 32;
                unsigned int ncl  = cl;
                if (noff >= exfat_cluster_size) { noff = 0; ncl = dir_no_chain ? ncl + 1 : exfat_next_cluster(ncl); }
                unsigned int nsec = noff / 512;
                unsigned int nabs = exfat_cluster_to_sector(ncl) + nsec;
                unsigned char nbuf[512] __attribute__((aligned(64)));
                if (exfat_blk_read_sectors(nabs, nbuf, 1) < 0) { off += 32*(sec_count+1); break; }
                unsigned int neoff = noff % 512;
                if (nbuf[neoff] == EXFAT_ENTRY_STREAM) {
                    ExFatStreamEntry *se = (ExFatStreamEntry *)(nbuf + neoff);
                    if (se->first_cluster == old_first_cl) {
                        se->data_length       = new_size;
                        se->valid_data_length = new_size;
                        se->first_cluster     = new_first_cl;
                        exfat_blk_write_sectors(nabs, nbuf, 1);
                        // recompute file entry checksum (stale after size update)
                        // we crush sector 0 (shouldn't) of the dir cluster instead of the actual file entry's sector
                        {
                            unsigned int fabs = exfat_cluster_to_sector(cl) + (off / 512);
                            unsigned char fbuf[512] __attribute__((aligned(64)));
                            if (exfat_blk_read_sectors(fabs, fbuf, 1) >= 0) {
                                unsigned int foff = eoff % 512;
                                int sc = ((ExFatFileEntry *)(fbuf + foff))->secondary_count;
                                if (sc > 18) sc = 18;
                                int set_size = (1 + sc) * 32;
                                unsigned char tmp[640];
                                memcpy(tmp, fbuf + foff, 32);
                                // copy stream entry (already in nbuf)
                                memcpy(tmp + 32, nbuf + neoff, 32);
                                // copy remaining secondaries from disk
                                // can we speed this up withoiut causing a race condition? (!investigate)
                                int si;
                                unsigned int soff = noff + 32;
                                for (si = 1; si < sc; si++) {
                                    unsigned int scl2 = ncl;
                                    if (soff >= exfat_cluster_size) { soff = 0; scl2 = dir_no_chain ? scl2 + 1 : exfat_next_cluster(scl2); }
                                    unsigned int ssec = exfat_cluster_to_sector(scl2) + soff / 512;
                                    unsigned char sbuf[512] __attribute__((aligned(64)));
                                    if (exfat_blk_read_sectors(ssec, sbuf, 1) >= 0)
                                        memcpy(tmp + 32 + (si+1-1)*32, sbuf + (soff % 512), 32);
                                    soff += 32;
                                }
                                unsigned short ck = exfat_entry_checksum(tmp, set_size);
                                fbuf[foff + 2] = (unsigned char)(ck & 0xFF);
                                fbuf[foff + 3] = (unsigned char)(ck >> 8);
                                exfat_blk_write_sectors(fabs, fbuf, 1);
                            }
                        }
                        return;
                    }
                }
                unsigned int skip = sec_count + 1;
                off += skip * 32;
                if (off >= exfat_cluster_size) {
                    unsigned int crosses = off / exfat_cluster_size;
                    off = off % exfat_cluster_size;
                    unsigned int c; for (c = 0; c < crosses; c++) cl = dir_no_chain ? cl + 1 : exfat_next_cluster(cl);
                }
                eoff = off % 512;
                // re-read sector if skip crossed a sector boundary (buf is stale otherwise)
                {
                    unsigned int nsec2 = exfat_cluster_to_sector(cl) + off / 512;
                    if (nsec2 != abs_sec) {
                        if (exfat_blk_read_sectors(nsec2, buf, 1) < 0) return;
                    }
                }
                continue;
            }

            eoff += 32; off += 32;
            if (off >= exfat_cluster_size) {
                off = 0; cl = dir_no_chain ? cl + 1 : exfat_next_cluster(cl);
                break;
            }
        }
        if (off == 0) continue;
        unsigned int nextsec = off / 512 + 1;
        if (nextsec >= exfat_sectors_per_cluster) { cl = dir_no_chain ? cl + 1 : exfat_next_cluster(cl); off = 0; }
        else off = nextsec * 512;
    }
}

// std compute exfat namehash per the spec - rotate-right-1 + add per utf-16 byte
static unsigned short exfat_name_hash(const char *name, int nlen)
{
    unsigned short hash = 0;
    int i;
    for (i = 0; i < nlen; i++) {
        unsigned short ch = (unsigned char)name[i];
        if (ch >= 'a' && ch <= 'z') ch -= 32;
        hash = ((hash << 15) | (hash >> 1)) + (ch & 0xFF);
        hash = ((hash << 15) | (hash >> 1)) + (ch >> 8);
    }
    return hash;
}

// compute entrysetchecksum
static unsigned short exfat_entry_checksum(const unsigned char *es, int size)
{
    unsigned short ck = 0;
    int i;
    for (i = 0; i < size; i++) {
        if (i == 2 || i == 3) continue;
        ck = ((ck << 15) | (ck >> 1)) + es[i];
    }
    return ck;
}

// build a complete exfat entry set (file + stream + name entries)
static int exfat_build_entry_set(unsigned char *es,
                                  const char *name, int nlen,
                                  unsigned short file_attributes,
                                  unsigned int first_cluster,
                                  unsigned long long data_length)
{
    int name_entries = (nlen + 14) / 15;
    int total = 1 + 1 + name_entries;
    int es_size = total * 32;
    int i;
    memset(es, 0, es_size);


    // file entry (0x85) - 32 bytes at offset 0
    es[0] = 0x85;
    es[1] = (unsigned char)(1 + name_entries);
    es[4] = (unsigned char)(file_attributes & 0xFF);
    es[5] = (unsigned char)(file_attributes >> 8);
    {
        unsigned short fdate, ftime;
        fat_current_time(&fdate, &ftime);
        unsigned int ts = ((unsigned int)fdate << 16) | ftime;
        memcpy(es + 8,  &ts, 4);  // create_timestamp
        memcpy(es + 12, &ts, 4);  // modify_timestamp
        memcpy(es + 16, &ts, 4);  // access_timestamp
    }

    // stream entry (0xc0) - 32 bytes at offset 32
    es[32] = 0xC0;
    es[33] = 0x01;
    es[35] = (unsigned char)nlen;
    {
        unsigned short nhash = exfat_name_hash(name, nlen);
        es[36] = (unsigned char)(nhash & 0xFF);
        es[37] = (unsigned char)(nhash >> 8);
    }
    memcpy(es + 32 + 8,  &data_length, 8);
    memcpy(es + 32 + 20, &first_cluster, 4);
    memcpy(es + 32 + 24, &data_length, 8);

    for (i = 0; i < name_entries; i++) {
        int b = (2 + i) * 32;
        int k;
        es[b] = 0xC1;
        for (k = 0; k < 15; k++) {
            int ci = i * 15 + k;
            if (ci < nlen) {
                es[b + 2 + k * 2] = (unsigned char)name[ci];
                es[b + 3 + k * 2] = 0;
            }
        }
    }

    // compute and store set_checksum
    {
        unsigned short ck = exfat_entry_checksum(es, es_size);
        es[2] = (unsigned char)(ck & 0xFF);
        es[3] = (unsigned char)(ck >> 8);
    }
    return es_size;
}

// write an exfat directory entry set into a parent directory
// finds contiguous free slots, writes entry set + end-of-dir marker
// returns 0 on success, -1 on failure
static int exfat_write_dir_entry(unsigned int parent_cluster,
                                  const char *name,
                                  unsigned short file_attributes,
                                  unsigned int first_cluster,
                                  unsigned long long data_length)
{
    int nlen = 0;
    { const char *p = name; while (*p && nlen < 255) { nlen++; p++; } }

    unsigned char entry_set[640];
    int es_size = exfat_build_entry_set(entry_set, name, nlen,
                                         file_attributes, first_cluster,
                                         data_length);
    if (es_size > (int)sizeof(entry_set)) return -1;

    unsigned char dbuf[512] __attribute__((aligned(64)));
    unsigned int dcl = parent_cluster;

    while (dcl >= 2 && dcl < FAT32_EOC) {
        unsigned int dsec = exfat_cluster_to_sector(dcl);
        unsigned int ds;
        for (ds = 0; ds < exfat_sectors_per_cluster; ds++) {
            if (exfat_blk_read_sectors(dsec + ds, dbuf, 1) < 0)
                return -1;
            unsigned int eo;
            for (eo = 0; eo < 512; eo += 32) {
                unsigned char et = dbuf[eo];
                // skip active entries (bit 7 set)
                if (et & 0x80) continue;
                // found free slot (0x00 end-of-dir or deleted)
                if (et == 0x00) {
                    if (eo + (unsigned int)es_size + 32 <= 512) {
                        memcpy(dbuf + eo, entry_set, es_size);
                        dbuf[eo + es_size] = 0x00;
                        exfat_blk_write_sectors(dsec + ds, dbuf, 1);
                        return 0;
                    }
                    // doesn't fit - cross-sector write
                    {
                        unsigned int first_part = 512 - eo;
                        memcpy(dbuf + eo, entry_set, first_part);
                        exfat_blk_write_sectors(dsec + ds, dbuf, 1);
                        // write remainder to next sector
                        unsigned int nds = ds + 1;
                        unsigned int ndcl = dcl;
                        if (nds >= exfat_sectors_per_cluster) {
                            nds = 0;
                            ndcl = exfat_next_cluster(dcl);
                            if (ndcl < 2 || ndcl >= FAT32_EOC) return -1;
                        }
                        unsigned int nsec = exfat_cluster_to_sector(ndcl) + nds;
                        unsigned char dbuf2[512] __attribute__((aligned(64)));
                        memset(dbuf2, 0, 512);
                        unsigned int rem = (unsigned int)es_size - first_part;
                        memcpy(dbuf2, entry_set + first_part, rem);
                        if (rem < 512) dbuf2[rem] = 0x00;
                        exfat_blk_write_sectors(nsec, dbuf2, 1);
                        return 0;
                    }
                }
                // deleted entry (non-zero, bit7 clear) - skip for now
            }
        }
        // advance to next dir cluster so ifthe chain ends, extend it (solves a lot of games custom data install issues thanks @kingKoopad for helping!)
        unsigned int next_dcl = exfat_next_cluster(dcl);
        if (next_dcl < 2 || next_dcl >= FAT32_EOC) {
            unsigned int nc = exfat_alloc_cluster();
            if (nc == 0) return -1;
            exfat_zero_cluster_ex(nc);
            exfat_write_fat_entry(dcl, nc);
            dcl = nc;
        } else {
            dcl = next_dcl;
        }
    }
    return -1;
}

// path resolvers

static int to_fat83(const char *name, unsigned char out[11])
{
    memset(out, ' ', 11);

    if (name[0] == '.' && name[1] == '\0') {
        out[0] = '.';
        return 1;
    }
    if (name[0] == '.' && name[1] == '.' && name[2] == '\0') {
        out[0] = '.';
        out[1] = '.';
        return 1;
    }

    const char *dot = NULL;
    int i;
    for (i = 0; name[i]; i++) {
        if (name[i] == '.')
            dot = &name[i];
    }
    int namelen = i;

    int baselen, extlen;
    if (dot) {
        baselen = (int)(dot - name);
        extlen = namelen - baselen - 1;
    } else {
        baselen = namelen;
        extlen = 0;
    }

    if (baselen > 8 || extlen > 3)
        return 0;

    for (i = 0; i < baselen && i < 8; i++) {
        char c = name[i];
        if (c >= 'a' && c <= 'z')
            c -= 32;
        out[i] = c;
    }
    if (dot) {
        for (i = 0; i < extlen && i < 3; i++) {
            char c = dot[1 + i];
            if (c >= 'a' && c <= 'z')
                c -= 32;
            out[8 + i] = c;
        }
    }

    return 1;
}

static int strcasecmp_local(const char *a, const char *b)
{
    while (*a && *b) {
        char ca = *a, cb = *b;
        if (ca >= 'a' && ca <= 'z') ca -= 32;
        if (cb >= 'a' && cb <= 'z') cb -= 32;
        if (ca != cb)
            return ca - cb;
        a++;
        b++;
    }
    return (unsigned char)*a - (unsigned char)*b;
}

static void fat_extract_shortname(const FatDirEntry *entry, char *out)
{
    int i, j = 0;
    for (i = 7; i >= 0 && entry->name[i] == ' '; i--);
    int base_end = i + 1;
    for (i = 0; i < base_end; i++)
        out[j++] = entry->name[i];

    for (i = 10; i >= 8 && entry->name[i] == ' '; i--);
    int ext_end = i + 1;
    if (ext_end > 8) {
        out[j++] = '.';
        for (i = 8; i < ext_end; i++)
            out[j++] = entry->name[i];
    }

    out[j] = '\0';
}

static void fat_extract_lfn_part(const FatLfnEntry *lfn, char *buf, int seq)
{
    int base = (seq - 1) * 13;
    int i, pos;

    for (i = 0; i < 5; i++) {
        pos = base + i;
        if (pos < 255 && lfn->name1[i] && lfn->name1[i] != 0xFFFF)
            buf[pos] = (char)(lfn->name1[i] & 0xFF);
        else if (pos < 255)
            buf[pos] = '\0';
    }
    for (i = 0; i < 6; i++) {
        pos = base + 5 + i;
        if (pos < 255 && lfn->name2[i] && lfn->name2[i] != 0xFFFF)
            buf[pos] = (char)(lfn->name2[i] & 0xFF);
        else if (pos < 255)
            buf[pos] = '\0';
    }
    for (i = 0; i < 2; i++) {
        pos = base + 11 + i;
        if (pos < 255 && lfn->name3[i] && lfn->name3[i] != 0xFFFF)
            buf[pos] = (char)(lfn->name3[i] & 0xFF);
        else if (pos < 255)
            buf[pos] = '\0';
    }
}

// extract lfn part as raw ucs-2 for the codepage conversion
static void fat_extract_lfn_part_ucs2(const FatLfnEntry *lfn, unsigned short *buf, int seq)
{
    int base = (seq - 1) * 13;
    int i, pos;

    for (i = 0; i < 5; i++) {
        pos = base + i;
        if (pos < 255) buf[pos] = lfn->name1[i];
    }
    for (i = 0; i < 6; i++) {
        pos = base + 5 + i;
        if (pos < 255) buf[pos] = lfn->name2[i];
    }
    for (i = 0; i < 2; i++) {
        pos = base + 11 + i;
        if (pos < 255) buf[pos] = lfn->name3[i];
    }
}

static unsigned int fat32_find_in_dir_ex(unsigned int dir_cluster, const char *name,
                                          unsigned int *out_size, unsigned char *out_attr,
                                          unsigned int *out_sfn_sector, unsigned int *out_sfn_index)
{
    unsigned char sfn[11];
    int use_sfn = to_fat83(name, sfn);
    char lfn_buf[256];
    int lfn_ready = 0;
    unsigned char dir_sec_buf[512] __attribute__((aligned(64)));

    unsigned int cluster = dir_cluster;
    unsigned int iter32 = 0;
    memset(lfn_buf, 0, sizeof(lfn_buf));

    while (cluster >= 2 && cluster < FAT32_EOC && iter32 < fat32_total_clusters) {
        iter32++;
        unsigned int base_sector = cluster_to_sector(cluster);
        unsigned int secs_in_cluster = fat32_cluster_size / fat32_bytes_per_sector;
        unsigned int s;

        for (s = 0; s < secs_in_cluster; s++) {
            int ret = blk_read_sectors(base_sector + s, dir_sec_buf, 1);
            if (ret < 0)
                return 0;

            unsigned int entries_in_sec = fat32_bytes_per_sector / 32;
            unsigned int e;
            for (e = 0; e < entries_in_sec; e++) {
                FatDirEntry *entry = (FatDirEntry *)(dir_sec_buf + e * 32);

                if (entry->name[0] == 0x00)
                    return 0;

                if (entry->name[0] == 0xE5) {
                    memset(lfn_buf, 0, sizeof(lfn_buf));
                    lfn_ready = 0;
                    continue;
                }

                if (entry->attr == FAT_ATTR_LFN) {
                    FatLfnEntry *lfn = (FatLfnEntry *)entry;
                    int seq = lfn->order & 0x3F;
                    if (lfn->order & 0x40)
                        memset(lfn_buf, 0, sizeof(lfn_buf));
                    fat_extract_lfn_part(lfn, lfn_buf, seq);
                    if (seq == 1)
                        lfn_ready = 1;
                    continue;
                }

                if (entry->attr & FAT_ATTR_VOLUME_ID) {
                    lfn_ready = 0;
                    continue;
                }

                int matched = 0;

                if (lfn_ready && lfn_buf[0]) {
                    if (strcasecmp_local(name, lfn_buf) == 0)
                        matched = 1;
                }

                if (!matched) {
                    if (use_sfn) {
                        if (memcmp(entry->name, sfn, 11) == 0)
                            matched = 1;
                    } else {
                        char sname[13];
                        fat_extract_shortname(entry, sname);
                        if (strcasecmp_local(name, sname) == 0)
                            matched = 1;
                    }
                }

                if (matched) {
                    unsigned int cl = ((unsigned int)entry->cluster_hi << 16) | entry->cluster_lo;
                    if (out_size) *out_size = entry->file_size;
                    if (out_attr) *out_attr = entry->attr;
                    if (out_sfn_sector) *out_sfn_sector = base_sector + s;
                    if (out_sfn_index) *out_sfn_index = e * 32;
                    return cl;
                }

                memset(lfn_buf, 0, sizeof(lfn_buf));
                lfn_ready = 0;
            }
        }

        cluster = fat32_next_cluster(cluster);
    }

    return 0;
}

static unsigned int fat32_resolve_path_ex(const char *path, unsigned int *out_size,
                                           unsigned char *out_attr,
                                           unsigned int *out_sfn_sector,
                                           unsigned int *out_sfn_index)
{
    unsigned int cluster = fat32_root_cluster;
    unsigned char attr = FAT_ATTR_DIRECTORY;
    unsigned int size = 0;

    while (*path == '/')
        path++;

    if (*path == '\0') {
        if (out_size) *out_size = 0;
        if (out_attr) *out_attr = FAT_ATTR_DIRECTORY;
        return cluster;
    }

    while (*path) {
        char component[256];
        int i = 0;
        while (*path && *path != '/' && i < 255)
            component[i++] = *path++;
        component[i] = '\0';
        while (*path == '/')
            path++;

        if (!(attr & FAT_ATTR_DIRECTORY))
            return 0;

        cluster = fat32_find_in_dir_ex(cluster, component, &size, &attr,
                                        out_sfn_sector, out_sfn_index);
        if (cluster == 0)
            return 0;
    }

    if (out_size) *out_size = size;
    if (out_attr) *out_attr = attr;
    return cluster;
}

static unsigned int fat32_resolve_path(const char *path, unsigned int *out_size, unsigned char *out_attr)
{
    return fat32_resolve_path_ex(path, out_size, out_attr, NULL, NULL);
}

static int fat32_write_fat(unsigned int cluster, unsigned int value);

// deferred fat2 write - track dirty fat sectors, flush once at end of operation
// eliminates 50% of fat i/o during cluster allocation chains increasing performance
static unsigned int fat2_dirty_sector = 0xFFFFFFFF;

static void fat2_flush(void)
{
    if (fat2_dirty_sector != 0xFFFFFFFF) {
        if (fat_cached_sector == fat2_dirty_sector) {
            blk_write_sectors(fat2_dirty_sector + fat32_fat_size, fat_sector_buf, 1);
        } else {
            // cache was evicted - re-read the dirty sector from disk
            unsigned char tmp_buf[512] __attribute__((aligned(64)));
            if (blk_read_sectors(fat2_dirty_sector, tmp_buf, 1) >= 0)
                blk_write_sectors(fat2_dirty_sector + fat32_fat_size, tmp_buf, 1);
        }
        fat2_dirty_sector = 0xFFFFFFFF;
    }
}

static int fat32_write_fat(unsigned int cluster, unsigned int value)
{
    unsigned int fat_offset = cluster * 4;
    unsigned int fat_sector = fat32_fat_start_sector + (fat_offset / fat32_bytes_per_sector);
    unsigned int entry_offset = fat_offset % fat32_bytes_per_sector;

    if (fat2_dirty_sector != 0xFFFFFFFF && fat2_dirty_sector != fat_sector) {
        fat2_flush();
    }

    if (fat_sector != fat_cached_sector) {
        int ret = blk_read_sectors(fat_sector, fat_sector_buf, 1);
        if (ret < 0) return ret;
        fat_cached_sector = fat_sector;
    }
    unsigned int old_raw = *(unsigned int *)(fat_sector_buf + entry_offset);
    unsigned int old_value = old_raw & 0x0FFFFFFF;
    *(unsigned int *)(fat_sector_buf + entry_offset) = (old_raw & 0xF0000000) | (value & 0x0FFFFFFF);

    if (old_value != 0 && value == 0) fat32_free_clusters++;
    else if (old_value == 0 && value != 0) {
        if (fat32_free_clusters > 0) fat32_free_clusters--;
    }

    int ret = blk_write_sectors(fat_sector, fat_sector_buf, 1);
    if (ret < 0) return ret;
    blk_wr_cache_push(fat_sector, fat_sector_buf);

    // mark fat2 as needing sync - will be flushed when sector changes or at fat2_flush()
    fat2_dirty_sector = fat_sector;
    return 0;
}

// scans from next_free_hint (last alloc position) and wraps around once (not too shabby... revisit?)
static unsigned int fat32_alloc_cluster(void)
{
    unsigned int total_clusters = fat32_total_clusters + 2;
    unsigned int start = fat32_next_free_hint;
    if (start < 2 || start >= total_clusters) start = 2;
    unsigned int c;
    for (c = start; c < total_clusters; c++) {
        if (fat32_next_cluster(c) == 0) {
            fat32_write_fat(c, FAT32_EOC);
            fat32_next_free_hint = c + 1;
            return c;
        }
    }
    for (c = 2; c < start; c++) {
        if (fat32_next_cluster(c) == 0) {
            fat32_write_fat(c, FAT32_EOC);
            fat32_next_free_hint = c + 1;
            return c;
        }
    }
    return 0; // disk is probs full
}

static unsigned int fat32_extend_chain(unsigned int tail)
{
    unsigned int newc = fat32_alloc_cluster();
    if (newc == 0) return 0;
    fat32_write_fat(tail, newc);
    return newc;
}

static int fat32_zero_cluster(unsigned int cluster)
{
    unsigned char zero[512] __attribute__((aligned(64)));
    memset(zero, 0, sizeof(zero));
    unsigned int sec = cluster_to_sector(cluster);
    unsigned int i;
    for (i = 0; i < fat32_sectors_per_cluster; i++) {
        int ret = blk_write_sectors(sec + i, zero, 1);
        if (ret < 0) return ret;
    }
    return 0;
}

static void __attribute__((unused)) fat32_update_dir_size(unsigned int dir_cluster, unsigned int file_cluster,
                                   unsigned int new_size)
{
    unsigned char sec_buf[512] __attribute__((aligned(64)));
    unsigned int cluster = dir_cluster;
    unsigned int iter32u = 0;
    while (cluster >= 2 && cluster < FAT32_EOC && iter32u < fat32_total_clusters) {
        iter32u++;
        unsigned int sec = cluster_to_sector(cluster);
        unsigned int s;
        for (s = 0; s < fat32_sectors_per_cluster; s++) {
            if (blk_read_sectors(sec + s, sec_buf, 1) < 0) return;
            unsigned int e;
            for (e = 0; e < fat32_bytes_per_sector / 32; e++) {
                FatDirEntry *de = (FatDirEntry *)(sec_buf + e * 32);
                if (de->name[0] == 0x00) return;
                if (de->name[0] == 0xE5) continue;
                if (de->attr == FAT_ATTR_LFN) continue;
                unsigned int cl = ((unsigned int)de->cluster_hi << 16) | de->cluster_lo;
                if (cl == file_cluster) {
                    de->file_size = new_size;
                    blk_write_sectors(sec + s, sec_buf, 1);
                    blk_wr_cache_push(sec + s, sec_buf);
                    return;
                }
            }
        }
        cluster = fat32_next_cluster(cluster);
    }
}

// write a raw 8.3 directory entry into dir_cluster. returns 0 on success.
// outputs the sector and byte offset of the written sfn entry (for ioclose fast path).
static int fat32_write_dir_entry(unsigned int dir_cluster, FatDirEntry *entry,
                                  unsigned int *out_sfn_sector, unsigned int *out_sfn_index)
{
    unsigned char sec_buf[512] __attribute__((aligned(64)));
    unsigned int cluster = dir_cluster;
    while (1) {
        if (cluster < 2 || cluster >= FAT32_EOC) return -1;
        unsigned int sec = cluster_to_sector(cluster);
        unsigned int s;
        for (s = 0; s < fat32_sectors_per_cluster; s++) {
            if (blk_read_sectors(sec + s, sec_buf, 1) < 0) return -1;
            unsigned int e;
            for (e = 0; e < fat32_bytes_per_sector / 32; e++) {
                FatDirEntry *de = (FatDirEntry *)(sec_buf + e * 32);
                if (de->name[0] == 0x00 || de->name[0] == 0xE5) {
                    memcpy(de, entry, 32);
                    if (out_sfn_sector) *out_sfn_sector = sec + s;
                    if (out_sfn_index) *out_sfn_index = e * 32;
                    int wr = blk_write_sectors(sec + s, sec_buf, 1);
                    blk_wr_cache_push(sec + s, sec_buf);
                    // sony clamps positive returns to 0 something like (min(ret, 0))
                    return (wr < 0) ? wr : 0;
                }
            }
        }
        // follow existing chain before extending
        unsigned int next = fat32_next_cluster(cluster);
        if (next >= 2 && next < FAT32_EOC) {
            cluster = next;
            continue;
        }
        unsigned int newc = fat32_extend_chain(cluster);
        if (newc == 0) return -1;
        fat32_zero_cluster(newc);
        cluster = newc;
    }
}

// convert fat date/time fields to scepspdatetime
static void fat_time_to_psp(unsigned short fdate, unsigned short ftime,
                             ScePspDateTime *out)
{
    out->year = ((fdate >> 9) & 0x7F) + 1980;
    out->month = (fdate >> 5) & 0x0F;
    out->day = fdate & 0x1F;
    out->hour = (ftime >> 11) & 0x1F;
    out->minute = (ftime >> 5) & 0x3F;
    out->second = (ftime & 0x1F) * 2;
    out->microsecond = 0;
}

// uno reverso
static void psp_to_fat_time(const ScePspDateTime *in,
                             unsigned short *fdate, unsigned short *ftime)
{
    int y = in->year - 1980; if (y < 0) y = 0; if (y > 127) y = 127;
    int m = in->month; if (m < 1) m = 1; if (m > 12) m = 12;
    int d = in->day; if (d < 1) d = 1; if (d > 31) d = 31;
    *fdate = (unsigned short)((y << 9) | (m << 5) | d);
    int h = in->hour; if (h > 23) h = 23;
    int mi = in->minute; if (mi > 59) mi = 59;
    int s = in->second / 2; if (s > 29) s = 29;
    *ftime = (unsigned short)((h << 11) | (mi << 5) | s);
}

static void fat_current_time(unsigned short *fdate, unsigned short *ftime)
{
    unsigned int t = (unsigned int)sceKernelLibcTime(NULL);
    // if rtc not set or error, use safe default (2025-01-01)
    if (t == 0 || t == 0xFFFFFFFF) {
        *fdate = (unsigned short)((45 << 9) | (1 << 5) | 1);
        *ftime = 0;
        return;
    }
    // use 32-bit math only
    unsigned int days = t / 86400;
    unsigned int rem = t % 86400;
    int h = (int)(rem / 3600); rem %= 3600;
    int mi = (int)(rem / 60);
    int s = (int)(rem % 60);
    int y = 1970; int m = 1; int d;
    while (1) {
        int ydays = (y % 4 == 0 && (y % 100 != 0 || y % 400 == 0)) ? 366 : 365;
        if (days < (unsigned int)ydays) break;
        days -= ydays; y++;
    }
    static const int mdays[] = {31,28,31,30,31,30,31,31,30,31,30,31};
    int leap = (y % 4 == 0 && (y % 100 != 0 || y % 400 == 0));
    for (m = 0; m < 12; m++) {
        int md = mdays[m] + (m == 1 ? leap : 0);
        if (days < (unsigned int)md) break;
        days -= md;
    }
    m++; d = (int)days + 1;
    int fy = y - 1980; if (fy < 0) fy = 0;
    *fdate = (unsigned short)((fy << 9) | (m << 5) | d);
    *ftime = (unsigned short)((h << 11) | (mi << 5) | (s / 2));
}

// hmmm... I may not need this one. !NOTE: Revisit this
static void fat_fill_stat_times(const FatDirEntry *de, SceIoStat *stat)
{
    fat_time_to_psp(de->create_date, de->create_time, &stat->sce_st_ctime);
    fat_time_to_psp(de->modify_date, de->modify_time, &stat->sce_st_atime);
    fat_time_to_psp(de->access_date, 0, &stat->sce_st_mtime);
}

// check if a name needs lfn (if itdoesn't fit in 8.3)
static int needs_lfn(const char *name)
{
    int len = 0, dot = -1, i;
    for (i = 0; name[i]; i++) {
        len++;
        if (name[i] == '.') { if (dot >= 0) return 1; dot = i; }
        if (name[i] == ' ' || name[i] == '+' || name[i] == ',' ||
            name[i] == ';' || name[i] == '=' || name[i] == '[' || name[i] == ']')
            return 1;
    }
    if (dot < 0) return len > 8;
    if (dot > 8) return 1;
    if (len - dot - 1 > 3) return 1;
    // check for lowercase
    for (i = 0; name[i]; i++)
        if (name[i] >= 'a' && name[i] <= 'z') return 1;
    return 0;
}

// build a fat 8.3 short name. for names that don't fit, generates '~1' suffix.
static void make_sfn(const char *name, unsigned char *sfn)
{
    memset(sfn, ' ', 11);
    int i = 0, j = 0;
    int name_len = 0, dot_pos = -1;
    for (i = 0; name[i]; i++) {
        name_len++;
        if (name[i] == '.') dot_pos = i;
    }
    // can we make this more efficient?
    int base_len = (dot_pos >= 0) ? dot_pos : name_len;
    const char *ext = (dot_pos >= 0) ? name + dot_pos + 1 : "";
    int ext_len = 0;
    for (i = 0; ext[i]; i++) ext_len++;

    if (base_len <= 8 && ext_len <= 3 && !needs_lfn(name)) {
        for (i = 0, j = 0; i < base_len && j < 8; i++) {
            char c = name[i];
            if (c >= 'a' && c <= 'z') c -= 32;
            sfn[j++] = c;
        }
        for (i = 0, j = 8; i < ext_len && j < 11; i++) {
            char c = ext[i];
            if (c >= 'a' && c <= 'z') c -= 32;
            sfn[j++] = c;
        }
    } else {
        int copy = base_len < 6 ? base_len : 6;
        for (i = 0, j = 0; i < copy; i++) {
            char c = name[i];
            if (c == '.') continue;
            if (c >= 'a' && c <= 'z') c -= 32;
            if (j < 6) sfn[j++] = c;
        }
        sfn[j++] = '~';
        sfn[j++] = '1';
        for (i = 0, j = 8; i < ext_len && j < 11; i++) {
            char c = ext[i];
            if (c >= 'a' && c <= 'z') c -= 32;
            sfn[j++] = c;
        }
    }
}

static unsigned char lfn_checksum(const unsigned char *sfn)
{
    unsigned char sum = 0;
    int i;
    for (i = 0; i < 11; i++)
        sum = ((sum & 1) ? 0x80 : 0) + (sum >> 1) + sfn[i];
    return sum;
}

// write a directory entry with lfn support. creates lfn entries + 8.3 entry.
// outputs the sector and byte offset of the written sfn entry (like sony's per-file offset 0x20c).
static int fat32_write_dir_entry_lfn(unsigned int dir_cluster, FatDirEntry *entry,
                                      const char *long_name,
                                      unsigned int *out_sfn_sector,
                                      unsigned int *out_sfn_index)
{
    if (!needs_lfn(long_name)) {
        // simple 8.3 - just write the single entry
        return fat32_write_dir_entry(dir_cluster, entry, out_sfn_sector, out_sfn_index);
    }

    // let's build lfn entries
    int name_len = 0;
    while (long_name[name_len]) name_len++;
    int num_lfn = (name_len + 12) / 13; // 13 ucs-2 chars per lfn entry
    int total_entries = num_lfn + 1; // lfn entries + 8.3 entry
    unsigned char cksum = lfn_checksum(entry->name);

    // find contiguous free slots in the directory
    unsigned char sec_buf[512] __attribute__((aligned(64)));
    unsigned int cluster = dir_cluster;
    while (1) {
        if (cluster < 2 || cluster >= FAT32_EOC) return -1;
        unsigned int sec = cluster_to_sector(cluster);
        unsigned int s;
        for (s = 0; s < fat32_sectors_per_cluster; s++) {
            if (blk_read_sectors(sec + s, sec_buf, 1) < 0) return -1;
            unsigned int entries_per_sec = fat32_bytes_per_sector / 32;
            unsigned int e;
            for (e = 0; e < entries_per_sec; e++) {
                unsigned char *slot = sec_buf + e * 32;
                if (slot[0] != 0x00 && slot[0] != 0xE5) continue;

                // check if enough contiguous slots from here
                // for simplicity sake, just write entries one at a time starting from first free
                // we can speed this up later...maybe..probably not to avoid a race condition
                int slots_needed = total_entries;
                int slots_here = 0;
                unsigned int ce = e;
                while (ce < entries_per_sec && slots_here < slots_needed) {
                    unsigned char *cs = sec_buf + ce * 32;
                    if (cs[0] != 0x00 && cs[0] != 0xE5) break;
                    slots_here++;
                    ce++;
                }
                if (slots_here < slots_needed) continue;

                // write lfn entries (reverse order: last lfn first)
                int lfn_idx;
                for (lfn_idx = num_lfn; lfn_idx >= 1; lfn_idx--) {
                    unsigned char *lfn = sec_buf + (e + (num_lfn - lfn_idx)) * 32;
                    memset(lfn, 0xFF, 32);
                    lfn[0] = lfn_idx | (lfn_idx == num_lfn ? 0x40 : 0); // seq + last flag
                    lfn[11] = 0x0F; 
                    lfn[12] = 0;
                    lfn[13] = cksum;
                    lfn[26] = 0; lfn[27] = 0;

                    int base = (lfn_idx - 1) * 13;
                    // fill ucs-2 chars into lfn entry (5+6+2 = 13 chars)
                    int ci;
                    unsigned char *positions[] = {
                        lfn+1, lfn+3, lfn+5, lfn+7, lfn+9, // 5 chars at offset 1
                        lfn+14, lfn+16, lfn+18, lfn+20, lfn+22, lfn+24, // 6 chars at offset 14
                        lfn+28, lfn+30 // 2 chars at offset 28
                    };
                    for (ci = 0; ci < 13; ci++) {
                        int idx = base + ci;
                        if (idx < name_len) {
                            positions[ci][0] = (unsigned char)long_name[idx];
                            positions[ci][1] = 0;
                        } else if (idx == name_len) {
                            positions[ci][0] = 0; positions[ci][1] = 0;
                        } else {
                            positions[ci][0] = 0xFF; positions[ci][1] = 0xFF; 
                        }
                    }
                }
                memcpy(sec_buf + (e + num_lfn) * 32, entry, 32);
                if (out_sfn_sector) *out_sfn_sector = sec + s;
                if (out_sfn_index) *out_sfn_index = (e + num_lfn) * 32;
                if (e + total_entries < entries_per_sec) {
                    unsigned char *next = sec_buf + (e + total_entries) * 32;
                    if (next[0] == 0xFF || next[0] == 0x00) next[0] = 0x00;
                }
                {
                    int wr = blk_write_sectors(sec + s, sec_buf, 1);
                    blk_wr_cache_push(sec + s, sec_buf);
                    return (wr < 0) ? wr : 0;
                }
            }
            // sony counts free slots across sector boundaries (around sub_017a4).
            // we aren't gonna do that, because the in mem bitwise flip is fast enough, so convert trailing 0x00 to 0xe5 to prevent the
            // scanner from treating them as eod markers
            {
                int patched = 0;
                unsigned int pi;
                for (pi = 0; pi < entries_per_sec; pi++) {
                    if (sec_buf[pi * 32] == 0x00) {
                        sec_buf[pi * 32] = 0xE5;
                        patched = 1;
                    }
                }
                if (patched) {
                    blk_write_sectors(sec + s, sec_buf, 1);
                    blk_wr_cache_push(sec + s, sec_buf);
                }
            }
        }
        // follow the existing chain before extending it
        unsigned int next_c = fat32_next_cluster(cluster);
        if (next_c >= 2 && next_c < FAT32_EOC) {
            cluster = next_c;
            continue;
        }
        unsigned int newc = fat32_extend_chain(cluster);
        if (newc == 0) return -1;
        fat32_zero_cluster(newc);
        cluster = newc;
    }
}

// file mgmnt stuffss

static int alloc_fd(void)
{
    int i;
    for (i = 0; i < MAX_OPEN_FILES; i++) {
        if (!open_files[i].in_use)
            return i;
    }
    return -1;
}

// walk an exfat directory starting at dir_cluster, looking for 'name'
// parses 0x85 (file) → 0xc0 (stream) → 0xc1 (name) entry sets
static unsigned int exfat_find_in_dir(unsigned int dir_cluster,
                                       const char *name,
                                       unsigned int *out_size,
                                       unsigned char *out_attr,
                                       int *out_no_fat_chain,
                                       int dir_no_chain)
{
    unsigned int cluster = dir_cluster;
    unsigned char buf[512] __attribute__((aligned(64)));
    unsigned int iter = 0;

    while (cluster >= 2 && cluster < FAT32_EOC && iter < exfat_cluster_count) {
        iter++;
        unsigned int sec_base = exfat_data_start_sector +
            (cluster - 2) * exfat_sectors_per_cluster;
        unsigned int s;
        for (s = 0; s < exfat_sectors_per_cluster; s++) {
            int r = exfat_blk_read_sectors(sec_base + s, buf, 1);
            if (r < 0) return 0;

            unsigned int off;
            for (off = 0; off < 512; off += 32) {
                unsigned char etype = buf[off];
                if (etype == 0x00) return 0; // this is eod
                if (etype != EXFAT_ENTRY_FILE) continue;

                ExFatFileEntry *fe = (ExFatFileEntry *)(buf + off);
                int sec_count = fe->secondary_count;
                unsigned short file_attr = fe->file_attributes;

                // let's read the secondary entries (stream + name entries)
                unsigned int first_cl = 0;
                unsigned int fsize = 0;
                int no_chain = 0;
                char collected_name[256];
                int name_pos = 0;
                int subs_read = 0;

                // ok,advance to next entry
                unsigned int eoff = off + 32;
                unsigned int esec = s;
                unsigned int ecluster = cluster;

                int si;
                for (si = 0; si < sec_count && subs_read < sec_count; si++) {
                    if (eoff >= 512) {
                        eoff = 0;
                        esec++;
                        if (esec >= exfat_sectors_per_cluster) {
                            esec = 0;
                            if (dir_no_chain)
                                ecluster = ecluster + 1;
                            else
                                ecluster = exfat_next_cluster(ecluster);
                            if (ecluster < 2 || ecluster >= FAT32_EOC) goto done;
                        }
                        unsigned int nsec = exfat_data_start_sector +
                            (ecluster - 2) * exfat_sectors_per_cluster + esec;
                        r = exfat_blk_read_sectors(nsec, buf, 1);
                        if (r < 0) goto done;
                    }

                    unsigned char st = buf[eoff];
                    if (st == EXFAT_ENTRY_STREAM) {
                        ExFatStreamEntry *se = (ExFatStreamEntry *)(buf + eoff);
                        first_cl = se->first_cluster;
                        fsize = (unsigned int)se->data_length;
                        no_chain = (se->general_flags & 0x02) ? 1 : 0;
                    } else if (st == EXFAT_ENTRY_NAME) {
                        ExFatNameEntry *ne = (ExFatNameEntry *)(buf + eoff);
                        int k;
                        for (k = 0; k < 15 && name_pos < 255; k++) {
                            unsigned short ch = ne->name[k];
                            if (ch == 0) break;
                            collected_name[name_pos++] = (ch < 128) ? (char)ch : '?';
                        }
                    }
                    subs_read++;
                    eoff += 32;
                }

                collected_name[name_pos] = '\0';

                const char *a = name;
                const char *b = collected_name;
                int match = 1;
                while (*a && *b) {
                    char ca = *a, cb = *b;
                    if (ca >= 'a' && ca <= 'z') ca -= 32;
                    if (cb >= 'a' && cb <= 'z') cb -= 32;
                    if (ca != cb) { match = 0; break; }
                    a++; b++;
                }
                if (*a || *b) match = 0;

                if (match && first_cl >= 2) {
                    if (out_size) *out_size = fsize;
                    unsigned char a = 0;
                    if (file_attr & EXFAT_ATTR_DIRECTORY) a |= FAT_ATTR_DIRECTORY;
                    if (file_attr & EXFAT_ATTR_ARCHIVE) a |= FAT_ATTR_ARCHIVE;
                    if (out_attr) *out_attr = a;
                    if (out_no_fat_chain) *out_no_fat_chain = no_chain;
                    return first_cl;
                }

                if (ecluster != cluster || esec != s) {
                    r = exfat_blk_read_sectors(sec_base + s, buf, 1);
                    if (r < 0) return 0;
                }
            }
        }
        if (dir_no_chain)
            cluster = cluster + 1;
        else
            cluster = exfat_next_cluster(cluster);
    }
done:
    return 0;
}

// resolve a full path like "PSP/GAME/EBOOT.PBP" on exfat here we go
static unsigned int exfat_resolve_path(const char *path,
                                        unsigned int *out_size,
                                        unsigned char *out_attr,
                                        int *out_no_fat_chain)
{
    unsigned int cluster = exfat_root_cluster;
    unsigned char attr = FAT_ATTR_DIRECTORY;
    unsigned int size = 0;
    int no_chain = 0;

    // skip leading slash manually
    while (*path == '/') path++;
    if (*path == '\0') {
        if (out_size) *out_size = 0;
        if (out_attr) *out_attr = FAT_ATTR_DIRECTORY;
        if (out_no_fat_chain) *out_no_fat_chain = 0;
        return cluster;
    }

    while (*path) {
        char component[256];
        int ci = 0;
        while (*path && *path != '/' && ci < 255)
            component[ci++] = *path++;
        component[ci] = '\0';
        while (*path == '/') path++;

        if (!(attr & FAT_ATTR_DIRECTORY))
            return 0;

        int parent_nc = no_chain;
        cluster = exfat_find_in_dir(cluster, component, &size, &attr, &no_chain, parent_nc);
        if (cluster == 0) return 0;
    }

    if (out_size) *out_size = size;
    if (out_attr) *out_attr = attr;
    if (out_no_fat_chain) *out_no_fat_chain = no_chain;
    return cluster;
}

// annoying vfs handlers

static int exfat_IoInit(PspIoDrvArg *arg)
{
    (void)arg;
    return 0;
}

static int exfat_IoExit(PspIoDrvArg *arg)
{
    (void)arg;
    return 0;
}

static int exfat_IoOpen(PspIoDrvFileArg *arg, char *file, int flags, SceMode mode)
{
    (void)mode;
    if (fs_initialized != 2) deferred_fs_init();
    reopen_blk_fd_if_needed();
    if (blk_fd < 0) {
        return -1;
    }

    int want_write = (flags & PSP_O_WRONLY) || ((flags & PSP_O_RDWR) == PSP_O_RDWR);
    int want_creat = (flags & PSP_O_CREAT);
    int want_trunc = (flags & PSP_O_TRUNC);
    int want_append= (flags & PSP_O_APPEND);

    // es fast path (only applicable when using ES partition)
    if (es_initialized && !want_write && !want_creat && !want_trunc) {
        int r = es_try_open(arg, file, open_files, MAX_OPEN_FILES);
        if (r >= 0) return r;
    }

    unsigned int size = 0;
    unsigned char attr = 0;
    unsigned int cluster;
    unsigned int parent_cluster = fat32_root_cluster;
    int parent_nfc = 0;
    int no_chain = 0;
    unsigned int sfn_sec = 0, sfn_idx = 0;

    {
        const char *fbase = file;
        while (*fbase == '/') fbase++;
        const char *flast = NULL;
        { const char *fp = fbase; while (*fp) { if (*fp == '/') flast = fp; fp++; } }
        if (flast) {
            char ppath[256];
            int pplen = (int)(flast - fbase);
            if (pplen > 255) pplen = 255;
            memcpy(ppath, fbase, pplen);
            ppath[pplen] = '\0';
            unsigned int ps2 = 0; unsigned char pa2 = 0;
            if (g_use_exfat) { int nc2 = 0; parent_cluster = exfat_resolve_path(ppath, &ps2, &pa2, &nc2); parent_nfc = nc2; }
            else parent_cluster = fat32_resolve_path(ppath, &ps2, &pa2);
            if (parent_cluster == 0) parent_cluster = fat32_root_cluster;
        }

        if (g_use_exfat) {
            int nc = 0;
            cluster = exfat_resolve_path(file, &size, &attr, &nc);
            no_chain = nc;
        } else {
            cluster = fat32_resolve_path_ex(file, &size, &attr, &sfn_sec, &sfn_idx);
        }

        if (cluster == 0 && size == 0 && attr == 0) {
            if (!want_creat) {
                return 0x80010002;
            }
            if (g_use_exfat) {
                // exfat file creation using clean helper
                // maybe lets lower the duplicate mess a bit?
                const char *base2 = file;
                while (*base2 == '/') base2++;
                const char *last2 = NULL;
                const char *p2 = base2;
                while (*p2) { if (*p2 == '/') last2 = p2; p2++; }
                const char *fname2 = base2;
                if (last2 != NULL) {
                    char pp[256];
                    int pl = (int)(last2 - base2);
                    if (pl > 255) pl = 255;
                    memcpy(pp, base2, pl); pp[pl] = '\0';
                    fname2 = last2 + 1;
                    unsigned int ps2 = 0; unsigned char pa2 = 0; int nc2 = 0;
                    parent_cluster = exfat_resolve_path(pp, &ps2, &pa2, &nc2);
                    if (parent_cluster == 0) return 0x80010002;
                    parent_nfc = nc2;
                }
                cluster = exfat_alloc_cluster();
                if (cluster == 0) return 0x80010024;
                size = 0;
                int wret = exfat_write_dir_entry(parent_cluster, fname2,
                                                  EXFAT_ATTR_ARCHIVE, cluster, 0ULL);
                if (wret < 0) { exfat_free_cluster(cluster); return -1; }
                no_chain = 0;
                attr = FAT_ATTR_ARCHIVE;
                goto skip_fat32_creat;
            }
            const char *slash = file;
            (void)slash;
            // skip leading slashes before searching for last slash
            // redundant?
            const char *base = file;
            while (*base == '/') base++;
            const char *last = NULL;
            const char *p = base;
            while (*p) { if (*p == '/') last = p; p++; }
            char parent_path[256];
            const char *fname = base;
            if (last != NULL) {
                int plen = (int)(last - base);
                if (plen > 255) plen = 255;
                memcpy(parent_path, base, plen);
                parent_path[plen] = '\0';
                fname = last + 1;
                unsigned int ps = 0; unsigned char pa = 0;
                if (g_use_exfat) {
                    int nc2 = 0;
                    parent_cluster = exfat_resolve_path(parent_path, &ps, &pa, &nc2);
                } else {
                    parent_cluster = fat32_resolve_path(parent_path, &ps, &pa);
                }
                if (parent_cluster == 0) {
                    return 0x80010002;
                }
            }
            cluster = fat32_alloc_cluster();
            if (cluster == 0) { return 0x80010024; }
            // sony does not zero data clusters - file_size = 0 bounds reads (saves 64 writes/file extra perf!)
            size = 0;
            FatDirEntry de;
            memset(&de, 0, sizeof(de));
            make_sfn(fname, de.name);
            de.attr = FAT_ATTR_ARCHIVE;
            de.cluster_hi = (unsigned short)(cluster >> 16);
            de.cluster_lo = (unsigned short)(cluster & 0xFFFF);
            de.file_size = 0;
            { unsigned short td, tt;
              fat_current_time(&td, &tt);
              de.create_date = td; de.create_time = tt;
              de.modify_date = de.access_date = td;
              de.modify_time = tt; }
            if (fat32_write_dir_entry_lfn(parent_cluster, &de, fname, &sfn_sec, &sfn_idx) < 0) {
                fat32_write_fat(cluster, 0);
                return -1;
            }
        skip_fat32_creat: ;
        } else if (attr & FAT_ATTR_DIRECTORY) {
            return 0x80010015;
        }
    }

    if (parent_cluster == fat32_root_cluster && cluster != 0) {
        parent_cluster = fat32_root_cluster;
    }

    int fd = alloc_fd();
    if (fd < 0) {
        { int cnt = 0, j;
          for (j = 0; j < MAX_OPEN_FILES; j++) if (open_files[j].in_use) cnt++;
        }
        return 0x80000020; // too many open files (sony uses this)
    }

    unsigned int orig_cluster = cluster;

    if (want_trunc && want_write && size > 0) {
        // size it down because sony's fatms clearly is doing something that is non-standard. they have the cluster chain preserved
        // so we set it to 0 like they do. but this isn't correct, as the most efficient way would be to free the chain but in this case sony loves to reuse dirty opens
        // that's clearly a main theme when copying sony code, a lot of dirty opens and dirty reuse...
        size = 0;
        (void)cluster;
    }

    unsigned int tail = cluster;
    if (cluster >= 2 && (want_append || want_write)) {
        if (no_chain && size > 0) {
            // nofatchain - clusters are contiguous, compute the tail directly
            tail = cluster + ((size - 1) / fat32_cluster_size);
        } else {
            unsigned int nc = cluster;
            unsigned int iter_limit = fat32_total_clusters;
            unsigned int iter_count = 0;
            while (1) {
                unsigned int next = g_use_exfat ? exfat_next_cluster(nc) : fat32_next_cluster(nc);
                if (next >= FAT32_EOC || next < 2) break;
                nc = next;
                if (++iter_count >= iter_limit) break;
            }
            tail = nc;
        }
    }

    open_files[fd].in_use             = 1;
    open_files[fd].is_dir             = 0;
    open_files[fd].writable           = want_write;
    open_files[fd].is_dirty           = 0;
    open_files[fd].first_cluster      = cluster;
    open_files[fd].orig_first_cluster = orig_cluster;
    open_files[fd].file_size          = size;
    open_files[fd].position           = want_append ? size : 0;
    open_files[fd].tail_cluster       = tail;
    open_files[fd].parent_cluster     = parent_cluster;
    open_files[fd].no_fat_chain       = no_chain;
    open_files[fd].parent_no_fat_chain = parent_nfc;
    open_files[fd].cur_cluster        = cluster;
    open_files[fd].cur_cluster_index  = 0;
    open_files[fd].sfn_sector         = sfn_sec;
    open_files[fd].sfn_index          = sfn_idx;
    open_files[fd].msstor_fd          = -1; // unused - sony uses single per-mount fd

    arg->arg = (void *)fd;
    return 0;
}

static int exfat_IoClose(PspIoDrvFileArg *arg)
{
    int fd = (int)arg->arg;
    if (fd >= 0 && fd < MAX_OPEN_FILES && open_files[fd].in_use) {
        OpenFile *f = &open_files[fd];
        if (g_use_exfat && f->is_dirty && f->writable && !f->is_dir) {
            exfat_update_dir_entry(f->parent_cluster, f->first_cluster,
                                   f->first_cluster, f->file_size, f->parent_no_fat_chain);
        }
        // flush deferred fat2 writes before releasing fd
        fat2_flush();
        f->in_use = 0;
    }
    return 0;
}

static int exfat_IoRead(PspIoDrvFileArg *arg, char *data, int len)
{
    int fd = (int)arg->arg;
    if (fd < 0 || fd >= MAX_OPEN_FILES || !open_files[fd].in_use)
        return -1;

    OpenFile *f = &open_files[fd];

    if (f->is_dir)
        return -1;

    if (f->no_fat_chain == 2)
        return es_io_read(f, data, len);

    if (f->position >= f->file_size) return 0;
    unsigned int remaining = f->file_size - f->position;
    if ((unsigned int)len > remaining)
        len = (int)remaining;
    if (len <= 0)
        return 0;

    int total_read = 0;

    while (len > 0) {
        unsigned int cluster_index = f->position / fat32_cluster_size;
        unsigned int offset_in_cluster = f->position % fat32_cluster_size;

        // advance cached cluster forward (never backward, seeks reset the cache)
        while (f->cur_cluster_index < cluster_index &&
               f->cur_cluster >= 2 && f->cur_cluster < FAT32_EOC) {
            f->cur_cluster = next_cluster_for(f->cur_cluster, f->no_fat_chain);
            f->cur_cluster_index++;
        }
        unsigned int cluster = f->cur_cluster;
        if (cluster < 2 || cluster >= FAT32_EOC) {
            break;
        }

        unsigned int avail = fat32_cluster_size - offset_in_cluster;
        unsigned int to_read = (unsigned int)len < avail ? (unsigned int)len : avail;

        unsigned int sector = cluster_to_sector(cluster) + (offset_in_cluster / fat32_bytes_per_sector);
        unsigned int sec_offset = offset_in_cluster % fat32_bytes_per_sector;

        // fast path is sector-aligned + 64-byte cache-line-aligned buffer → direct dma
        // sony uses 3-tiers from what I can tell: 64-byte dma, 4-byte word copy, and else byte copy
        // psp dma requires 64-byte alignment obviously, 4-byte-aligned buffers corrupt the mem more or less
        if (sec_offset == 0 && to_read >= fat32_bytes_per_sector
            && ((unsigned int)data & 63) == 0) {
            unsigned int nsectors = to_read / fat32_bytes_per_sector;
            int ret = g_use_exfat
                ? exfat_blk_read_sectors(sector, data, nsectors)
                : blk_read_sectors(sector, data, nsectors);
            if (ret < 0)
                return total_read > 0 ? total_read : ret;
            unsigned int bulk = nsectors * fat32_bytes_per_sector;
            sceKernelDcacheWritebackInvalidateRange(data, bulk);
            data += bulk; f->position += bulk;
            total_read += bulk; len -= bulk;
            to_read -= bulk; sector += nsectors;
        }
        // slow path is partial/unaligned goes to bounce buffer
        while (to_read > 0) {
            unsigned int chunk = fat32_bytes_per_sector - sec_offset;
            if (chunk > to_read) chunk = to_read;
            int ret = g_use_exfat
                ? exfat_blk_read_sectors(sector, blk_bounce_buf, 1)
                : blk_read_sectors(sector, blk_bounce_buf, 1);
            if (ret < 0)
                return total_read > 0 ? total_read : ret;
            memcpy(data, blk_bounce_buf + sec_offset, chunk);
            sceKernelDcacheWritebackInvalidateRange(data, chunk);
            data += chunk; f->position += chunk;
            total_read += chunk; len -= chunk;
            to_read -= chunk; sector++;
            sec_offset = 0;
        }
    }

    return total_read;
}

static int exfat_IoWrite(PspIoDrvFileArg *arg, const char *data, int len)
{
    int fd = (int)arg->arg;
    if (fd < 0 || fd >= MAX_OPEN_FILES || !open_files[fd].in_use)
        return -1;
    OpenFile *f = &open_files[fd];
    if (!f->writable || f->is_dir) {
        return 0x80010013;
    }
    if (len <= 0) return 0;


    int total_written = 0;

    // fat32 write path similar to ioread but changes that are PSP specific (or technically sony specific?)
    // iolseek resets cur_cluster/cur_cluster_index so backward seeks are safe in this case
    while (len > 0) {
        unsigned int cluster_index = f->position / fat32_cluster_size;
        unsigned int offset_in_cluster = f->position % fat32_cluster_size;

        unsigned int wc;
        if (f->first_cluster < 2) {
            // file has no clusters yet - allocate first one
            f->first_cluster = g_use_exfat ? exfat_alloc_cluster() : fat32_alloc_cluster();
            if (f->first_cluster == 0) {
                break;
            }
            // no zero_cluster - file_size bounds reads, data will overwrite
            f->tail_cluster      = f->first_cluster;
            f->cur_cluster       = f->first_cluster;
            f->cur_cluster_index = 0;
            wc = f->first_cluster;
            offset_in_cluster = 0;
        } else {
            // advance cached cluster forward one hop at a time
            // walk existing chain; stop BEFORE stepping into eoc so cur_cluster stays valid
            // (= tail). this preserves tail tracking for the extend path below.
            while (f->cur_cluster_index < cluster_index) {
                unsigned int next = next_cluster_for(f->cur_cluster, f->no_fat_chain);
                if (next < 2 || next >= FAT32_EOC) break;
                f->cur_cluster = next;
                f->cur_cluster_index++;
            }
            // it looks like if the chain was shorter than needed  we havbe toextend enough clusters to reach cluster_index
            // my previous try only moved one cluster and set cur_cluster_index to the targ...basiaclly lying about chain length so later reads
            // at those positions walked to the wrong cluster and returned 0 past the chain end! my initial try  copying sony directly just didn't work sonyyyyyyy this doesn't make sense!! argh!
            if (f->cur_cluster_index < cluster_index) {
                unsigned int tail = f->cur_cluster;
                int alloc_failed = 0;
                while (f->cur_cluster_index < cluster_index) {
                    unsigned int newc = g_use_exfat ? exfat_extend_chain_ex(tail) : fat32_extend_chain(tail);
                    if (newc == 0) { alloc_failed = 1; break; }
                    tail = newc;
                    f->cur_cluster = newc;
                    f->cur_cluster_index++;
                }
                f->tail_cluster = tail;
                if (alloc_failed) break;
            }
            wc = f->cur_cluster;
            if (wc < 2 || wc >= FAT32_EOC) {
                break;
            }
        }

        unsigned int sector  = cluster_to_sector(wc) +
                               (offset_in_cluster / fat32_bytes_per_sector);
        unsigned int sec_off = offset_in_cluster % fat32_bytes_per_sector;
        unsigned int avail   = fat32_cluster_size - offset_in_cluster;
        unsigned int to_write = (unsigned int)len < avail ? (unsigned int)len : avail;

        // cap to 128 sectors (64kb) - msstor returns short writes above this
        // I tried this uncapped and msstor actually capped us so let's optimize for that
        // ...though... the hardware can handle more.... perhaps...
        if (sec_off == 0 && to_write >= fat32_bytes_per_sector
            && ((unsigned int)data & 3) == 0) {
            unsigned int nsectors = to_write / fat32_bytes_per_sector;
            if (nsectors > 128) nsectors = 128;
            unsigned int nbytes = nsectors * fat32_bytes_per_sector;
            int ret = g_use_exfat
                ? exfat_blk_write_sectors(sector, (void *)data, nsectors)
                : blk_write_sectors(sector, (void *)data, nsectors);
            if (ret < 0) return ret;
            // use actual bytes written to handle short writes
            unsigned int written = (ret > 0) ? (unsigned int)ret : nbytes;
            if (written > nbytes) written = nbytes;
            data          += written;
            f->position   += written;
            total_written += written;
            len           -= (int)written;
        } else {
            unsigned int chunk = fat32_bytes_per_sector - sec_off;
            if (chunk > to_write) chunk = to_write;
            if (chunk > (unsigned int)len) chunk = (unsigned int)len;
            int ret = g_use_exfat
                ? exfat_blk_read_sectors(sector, blk_bounce_buf, 1)
                : blk_read_sectors(sector, blk_bounce_buf, 1);
            if (ret < 0) return ret;
            memcpy(blk_bounce_buf + sec_off, data, chunk);
            ret = g_use_exfat
                ? exfat_blk_write_sectors(sector, blk_bounce_buf, 1)
                : blk_write_sectors(sector, blk_bounce_buf, 1);
            if (ret < 0) return ret; // sony returns error immediately let's do what they do
            data          += chunk;
            f->position   += chunk;
            total_written += chunk;
            len           -= (int)chunk;
        }

        f->is_dirty = 1; // sony sets dirty flag (struct[55] |= 0x04) on any write, again... do what they do, for now
        if (f->position > f->file_size) {
            f->file_size = f->position;
            // sony's sub_08de8 flushes dir entry to disk on every write that
            // grows the file (calls sub_00094). without this, a subsequent
            // ioopen sees stale file_size=0 on disk (scekernelloadmodule fails) and calls a whole heap o' trouble
            if (!g_use_exfat && f->sfn_sector != 0) {
                unsigned char de_buf[512] __attribute__((aligned(64)));
                if (blk_read_sectors(f->sfn_sector, de_buf, 1) >= 0) {
                    FatDirEntry *de = (FatDirEntry *)(de_buf + f->sfn_index);
                    de->file_size = f->file_size;
                    de->cluster_hi = (unsigned short)(f->first_cluster >> 16);
                    de->cluster_lo = (unsigned short)(f->first_cluster & 0xFFFF);
                    de->attr |= FAT_ATTR_ARCHIVE;
                    blk_write_sectors(f->sfn_sector, de_buf, 1);
                    blk_wr_cache_push(f->sfn_sector, de_buf);
                }
            }
        }
    }

    return total_written;
}

static SceOff exfat_IoLseek(PspIoDrvFileArg *arg, SceOff ofs, int whence)
{
    int fd = (int)arg->arg;
    if (fd < 0 || fd >= MAX_OPEN_FILES || !open_files[fd].in_use)
        return -1;

    OpenFile *f = &open_files[fd];
    long long new_pos;

    switch (whence) {
    case PSP_SEEK_SET:
        new_pos = ofs;
        break;
    case PSP_SEEK_CUR:
        new_pos = (long long)f->position + ofs;
        break;
    case PSP_SEEK_END:
        new_pos = (long long)f->file_size + ofs;
        break;
    default:
        return -1;
    }

    if (new_pos < 0)
        new_pos = 0;
    // do not clamp to file_size, sony doesn't so we shouldn't either... though that doesnt make much sense...

    f->position = (unsigned int)new_pos;
    // only reset cluster cache on backward seek, forward seeks keep cache valid
    // ioread advances cache forward anyway so resetting on every seek forced o(n)
    // cluster chain walk from the beginning on every seek+read pair
    // SONY DOES NOT DO THIS, this *should* be better... but i think we can optimize this...
    {
        unsigned int new_ci = (unsigned int)new_pos / fat32_cluster_size;
        if (new_ci < f->cur_cluster_index) {
            f->cur_cluster       = f->first_cluster;
            f->cur_cluster_index = 0;
        }
    }
    return (SceOff)new_pos;
}

static int exfat_IoIoctl(PspIoDrvFileArg *arg, unsigned int cmd, void *indata, int inlen, void *outdata, int outlen)
{
    (void)outdata; (void)outlen;
    switch (cmd) {
    // the elusive findfile! it walks dir tree, returns entry info...
    case 0x02415050:
        if (!indata) return 0x80010016;
        return 0;

    // seek within file by cluster offset
    case 0x0242D016:
    {
        int fd = (int)arg->arg;
        if (fd < 0 || fd >= MAX_OPEN_FILES || !open_files[fd].in_use) return 0x80010009;
        if (!indata || inlen < 8) return 0x80010016;
        unsigned int *params = (unsigned int *)indata;
        unsigned int cluster_off = params[0];
        unsigned int sector_off = params[1];
        OpenFile *f = &open_files[fd];
        unsigned int pos = cluster_off * fat32_cluster_size + sector_off * fat32_bytes_per_sector;
        f->position = pos;
        f->cur_cluster = f->first_cluster;
        f->cur_cluster_index = 0;
        return 0;
    }

    // sony's outer ioioctl early returns 0 for these 4 commands only
    // what they do or what they're for I have no idea...but lets just return 0
    case 0x00208002:
    case 0x00208013:
    case 0x00208014:
    case 0x00208082:
        return 0;
    default:
    {
        int fd = (int)arg->arg;
        if (fd >= 0 && fd < MAX_OPEN_FILES && open_files[fd].in_use
            && es_try_ioctl(open_files[fd].no_fat_chain) == 0)
            return 0;
        return 0x80020325;
    }
    }
}

static int exfat_IoRemove(PspIoDrvFileArg *arg, const char *name)
{
    (void)arg;
    if (fs_initialized != 2) deferred_fs_init();
    if (blk_fd < 0) return -1;

    // try filesystem first. if the file exists on the real fs, delete it there.
    // only fall through to es if the file is not on the filesystem.
    // this ensures "delete original after transfer to es" behavior removes the fs copy, not the es entry

    if (g_use_exfat) {
        unsigned int sz = 0; unsigned char at = 0; int nc = 0;
        unsigned int cl = exfat_resolve_path(name, &sz, &at, &nc);
        if (cl == 0) {
            if (es_initialized && es_try_remove(name) >= 0) return 0;
            return 0x80010002;
        }
        if (at & 0x10) return 0x80010014;
        const char *b = name; while (*b == '/') b++;
        const char *l = NULL; const char *p = b;
        while (*p) { if (*p == '/') l = p; p++; }
        unsigned int pcl = exfat_root_cluster;
        int parent_nc = 0;
        if (l) {
            char pp[256]; int pl = (int)(l - b); if (pl > 255) pl = 255;
            memcpy(pp, b, pl); pp[pl] = '\0';
            unsigned int ps = 0; unsigned char pa = 0; int pnc = 0;
            pcl = exfat_resolve_path(pp, &ps, &pa, &pnc);
            if (pcl == 0) return 0x80010002;
            parent_nc = pnc;
        }
        int dr = exfat_delete_dir_entry(pcl, cl, parent_nc);
        if (dr != 0) return dr;  // don't orphan clusters if entry wasn't actually removed
        // free clusters (nofatchain files need size-based freeing, not single cluster)
        if (nc) {
            unsigned int nclusters = (sz + (exfat_cluster_size - 1)) / exfat_cluster_size;
            if (nclusters == 0 && cl >= 2) nclusters = 1;
            unsigned int i;
            for (i = 0; i < nclusters; i++)
                exfat_free_cluster(cl + i);
        } else {
            exfat_free_chain(cl);
        }
        return 0;
    }

    unsigned int size = 0;
    unsigned char attr = 0;
    unsigned int cluster = fat32_resolve_path(name, &size, &attr);
    if (cluster == 0) {
        // try es table as fallback
        if (es_initialized && es_try_remove(name) >= 0) return 0;
        return 0x80010002;
    }
    // sony checks attr & 0x19 = readonly|volume_id|directory
    if (attr & 0x19) return 0x8001000D; // eacces right?

    const char *base = name;
    while (*base == '/') base++;
    const char *last = NULL;
    const char *p = base;
    while (*p) { if (*p == '/') last = p; p++; }

    unsigned int parent_cluster = fat32_root_cluster;
    if (last) {
        char pp[256];
        int plen = (int)(last - base);
        if (plen > 255) plen = 255;
        memcpy(pp, base, plen);
        pp[plen] = '\0';
        unsigned int ps = 0; unsigned char pa = 0;
        parent_cluster = fat32_resolve_path(pp, &ps, &pa);
        if (parent_cluster == 0) return 0x80010002;
    }

    // walk parent dir to find and mark the entry as deleted
    unsigned char sec_buf[512] __attribute__((aligned(64)));
    unsigned int dcl = parent_cluster;
    while (dcl >= 2 && dcl < FAT32_EOC) {
        unsigned int sec = cluster_to_sector(dcl);
        unsigned int s;
        for (s = 0; s < fat32_sectors_per_cluster; s++) {
            if (blk_read_sectors(sec + s, sec_buf, 1) < 0) return -1;
            unsigned int e;
            for (e = 0; e < fat32_bytes_per_sector / 32; e++) {
                FatDirEntry *de = (FatDirEntry *)(sec_buf + e * 32);
                if (de->name[0] == 0x00) goto rm_done;
                if (de->name[0] == 0xE5) continue;
                if (de->attr == FAT_ATTR_LFN) continue;
                unsigned int cl = ((unsigned int)de->cluster_hi << 16) | de->cluster_lo;
                if (cl == cluster) {
                    de->name[0] = 0xE5; // mark sfn deleted
                    // also mark preceding lfn entries as deleted
                    // for some reason this has different behavior on different sdcard speeds? race condition?
                    {
                        int le = (int)e - 1;
                        while (le >= 0) {
                            FatDirEntry *ld = (FatDirEntry *)(sec_buf + le * 32);
                            if (ld->attr != FAT_ATTR_LFN) break;
                            ld->name[0] = 0xE5;
                            le--;
                        }
                    }
                    blk_write_sectors(sec + s, sec_buf, 1);
                    blk_wr_cache_push(sec + s, sec_buf);
                    {
                        unsigned int fc = cluster;
                        while (fc >= 2 && fc < FAT32_EOC) {
                            unsigned int next = fat32_next_cluster(fc);
                            fat32_write_fat(fc, 0);
                            fc = next;
                        }
                    }
                    fat2_flush();
                    return 0;
                }
            }
        }
        dcl = fat32_next_cluster(dcl);
    }
rm_done:
    return 0x80010002; // not found in the directory
}

static int exfat_IoMkdir(PspIoDrvFileArg *arg, const char *name, SceMode mode)
{
    (void)arg; (void)mode;
    if (fs_initialized != 2) deferred_fs_init();

    if (g_use_exfat) {
        if (blk_fd < 0) return -1;
        unsigned int es2 = 0; unsigned char ea2 = 0; int nc2 = 0;
        unsigned int ecl = exfat_resolve_path(name, &es2, &ea2, &nc2);
        if (ecl != 0) return (ea2 & 0x10) ? 0 : 0x80010011;
        const char *eb = name; while (*eb == '/') eb++;
        const char *el = NULL; const char *ep2 = eb;
        while (*ep2) { if (*ep2 == '/') el = ep2; ep2++; }
        unsigned int epcl = exfat_root_cluster;
        const char *edname = eb;
        if (el) {
            char pp[256]; int pl = (int)(el - eb); if (pl > 255) pl = 255;
            memcpy(pp, eb, pl); pp[pl] = '\0';
            edname = el + 1;
            unsigned int ps = 0; unsigned char pa = 0; int pnc = 0;
            epcl = exfat_resolve_path(pp, &ps, &pa, &pnc);
            if (epcl == 0) return 0x80010002;
        }
        unsigned int dcl = exfat_alloc_cluster();
        if (dcl == 0) return 0x80010024;
        exfat_zero_cluster_ex(dcl);
        unsigned long long dir_len = (unsigned long long)exfat_cluster_size;
        int wret = exfat_write_dir_entry(epcl, edname,
                                          EXFAT_ATTR_DIRECTORY, dcl, dir_len);
        if (wret < 0) { exfat_free_cluster(dcl); return -1; }
        return 0;
    }

    if (blk_fd < 0) return -1;
    while (*name == '/') name++;
    const char *last = NULL;
    const char *p = name;
    while (*p) { if (*p == '/') last = p; p++; }

    unsigned int parent_cluster = fat32_root_cluster;
    const char *dname = name;
    if (last != NULL) {
        char parent_path[256];
        int plen = (int)(last - name);
        if (plen > 255) plen = 255;
        memcpy(parent_path, name, plen);
        parent_path[plen] = '\0';
        unsigned int ps = 0; unsigned char pa = 0;
        if (g_use_exfat) { int nc=0; parent_cluster = exfat_resolve_path(parent_path, &ps, &pa, &nc); }
        else parent_cluster = fat32_resolve_path(parent_path, &ps, &pa);
        if (parent_cluster == 0) { return 0x80010002; }
        dname = last + 1;
    }

    unsigned int es = 0; unsigned char ea = 0;
    unsigned int check_cl;
    if (g_use_exfat) { int nc=0; check_cl = exfat_resolve_path(name, &es, &ea, &nc); }
    else check_cl = fat32_resolve_path(name, &es, &ea);
    if (check_cl != 0) {
        if (ea & FAT_ATTR_DIRECTORY) return 0;
        return 0x80010011;
    }

    unsigned int cluster = fat32_alloc_cluster();
    if (cluster == 0) { return 0x80010024; }
    fat32_zero_cluster(cluster);

    // write . and .. entries (exfat normally doesn't do this automatically so we gotta injecterino)
    unsigned char sec_buf[512] __attribute__((aligned(64)));
    memset(sec_buf, 0, sizeof(sec_buf));
    FatDirEntry *dot  = (FatDirEntry *)sec_buf;
    FatDirEntry *dot2 = (FatDirEntry *)(sec_buf + 32);
    memset(dot->name,  ' ', 11); dot->name[0]  = '.';
    dot->attr = FAT_ATTR_DIRECTORY;
    dot->cluster_hi = (unsigned short)(cluster >> 16);
    dot->cluster_lo = (unsigned short)(cluster & 0xFFFF);
    memset(dot2->name, ' ', 11); dot2->name[0] = '.'; dot2->name[1] = '.';
    dot2->attr = FAT_ATTR_DIRECTORY;
    dot2->cluster_hi = (unsigned short)(parent_cluster >> 16);
    dot2->cluster_lo = (unsigned short)(parent_cluster & 0xFFFF);
    blk_write_sectors(cluster_to_sector(cluster), sec_buf, 1);

    FatDirEntry de;
    memset(&de, 0, sizeof(de));
    make_sfn(dname, de.name);
    de.attr = FAT_ATTR_DIRECTORY;
    de.cluster_hi = (unsigned short)(cluster >> 16);
    de.cluster_lo = (unsigned short)(cluster & 0xFFFF);
    de.file_size  = 0;

    { unsigned short td, tt;
      fat_current_time(&td, &tt);
      de.create_date = de.modify_date = de.access_date = td;
      de.create_time = de.modify_time = tt; }
    if (fat32_write_dir_entry_lfn(parent_cluster, &de, dname, NULL, NULL) < 0) {
        return -1;
    }
    return 0;
}

static int exfat_IoRmdir(PspIoDrvFileArg *arg, const char *name)
{
    (void)arg;
    if (fs_initialized != 2) deferred_fs_init();

    if (blk_fd < 0) return -1;
    if (g_use_exfat) {
        unsigned int sz = 0; unsigned char at = 0; int nc = 0;
        unsigned int cl = exfat_resolve_path(name, &sz, &at, &nc);
        if (cl == 0) return 0x80010002;
        if (!(at & 0x10)) return 0x80010014;
        {
            unsigned char chk[512] __attribute__((aligned(64)));
            unsigned int ccl = cl;
            while (ccl >= 2 && ccl < FAT32_EOC) {
                unsigned int csec = exfat_cluster_to_sector(ccl);
                unsigned int cs;
                for (cs = 0; cs < exfat_sectors_per_cluster; cs++) {
                    if (exfat_blk_read_sectors(csec + cs, chk, 1) < 0) return -1;
                    unsigned int ce;
                    for (ce = 0; ce < 512; ce += 32) {
                        if (chk[ce] == 0x00) goto exfat_dir_empty;
                        if (chk[ce] & 0x80) return 0x8001000A; // active entry = not empty
                    }
                }
                ccl = exfat_next_cluster(ccl);
            }
        }
    exfat_dir_empty:;
        // find parent and delete entry
        const char *b = name; while (*b == '/') b++;
        const char *l = NULL; const char *p = b;
        while (*p) { if (*p == '/') l = p; p++; }
        unsigned int pcl = exfat_root_cluster;
        int parent_nc = 0;
        if (l) {
            char pp[256]; int pl = (int)(l - b); if (pl > 255) pl = 255;
            memcpy(pp, b, pl); pp[pl] = '\0';
            unsigned int ps = 0; unsigned char pa = 0; int pnc = 0;
            pcl = exfat_resolve_path(pp, &ps, &pa, &pnc);
            if (pcl == 0) return 0x80010002;
            parent_nc = pnc;
        }
        int dr = exfat_delete_dir_entry(pcl, cl, parent_nc);
        if (dr != 0) return dr;  // propagate failure so savedata utility sees it
        if (nc) {
            unsigned int nclusters = (sz + (exfat_cluster_size - 1)) / exfat_cluster_size;
            if (nclusters == 0) nclusters = 1;
            unsigned int fi;
            for (fi = 0; fi < nclusters; fi++)
                exfat_free_cluster(cl + fi);
        } else { exfat_free_chain(cl); }
        return 0;
    }

    unsigned int size = 0; unsigned char attr = 0;
    unsigned int cluster = fat32_resolve_path(name, &size, &attr);
    if (cluster == 0) return 0x80010002;
    if (!(attr & FAT_ATTR_DIRECTORY)) return 0x80010014;

    // check directory is empty (only . and .. allowed)
    {
        unsigned char chk[512] __attribute__((aligned(64)));
        unsigned int ccl = cluster;
        while (ccl >= 2 && ccl < FAT32_EOC) {
            unsigned int csec = cluster_to_sector(ccl);
            unsigned int cs;
            for (cs = 0; cs < fat32_sectors_per_cluster; cs++) {
                if (blk_read_sectors(csec + cs, chk, 1) < 0) return -1;
                unsigned int ce;
                for (ce = 0; ce < fat32_bytes_per_sector / 32; ce++) {
                    FatDirEntry *cde = (FatDirEntry *)(chk + ce * 32);
                    if (cde->name[0] == 0x00) goto dir_empty;
                    if (cde->name[0] == 0xE5) continue;
                    if (cde->attr == FAT_ATTR_LFN) continue;
                    if (cde->name[0] == '.' && cde->name[1] == ' ') continue;
                    if (cde->name[0] == '.' && cde->name[1] == '.') continue;
                    return 0x8001000A; // not empty
                }
            }
            ccl = fat32_next_cluster(ccl);
        }
    }
dir_empty:

    // find parent and mark entry deleted
    const char *base = name;
    while (*base == '/') base++;
    const char *last = NULL;
    const char *p = base;
    while (*p) { if (*p == '/') last = p; p++; }
    unsigned int parent_cluster = fat32_root_cluster;
    if (last) {
        char pp[256];
        int plen = (int)(last - base);
        if (plen > 255) plen = 255;
        memcpy(pp, base, plen);
        pp[plen] = '\0';
        unsigned int ps = 0; unsigned char pa = 0;
        parent_cluster = fat32_resolve_path(pp, &ps, &pa);
        if (parent_cluster == 0) return 0x80010002;
    }

    unsigned char sec_buf[512] __attribute__((aligned(64)));
    unsigned int dcl = parent_cluster;
    while (dcl >= 2 && dcl < FAT32_EOC) {
        unsigned int sec = cluster_to_sector(dcl);
        unsigned int s;
        for (s = 0; s < fat32_sectors_per_cluster; s++) {
            if (blk_read_sectors(sec + s, sec_buf, 1) < 0) return -1;
            unsigned int e;
            for (e = 0; e < fat32_bytes_per_sector / 32; e++) {
                FatDirEntry *de = (FatDirEntry *)(sec_buf + e * 32);
                if (de->name[0] == 0x00) return 0x80010002;
                if (de->name[0] == 0xE5) continue;
                if (de->attr == FAT_ATTR_LFN) continue;
                unsigned int cl = ((unsigned int)de->cluster_hi << 16) | de->cluster_lo;
                if (cl == cluster) {
                    de->name[0] = 0xE5;
                    // mark preceding lfn entries as deleted
                    {
                        int le = (int)e - 1;
                        while (le >= 0) {
                            FatDirEntry *ld = (FatDirEntry *)(sec_buf + le * 32);
                            if (ld->attr != FAT_ATTR_LFN) break;
                            ld->name[0] = 0xE5;
                            le--;
                        }
                    }
                    blk_write_sectors(sec + s, sec_buf, 1);
                    // push to write cache so subsequent reads see deleted state
                    blk_wr_cache_push(sec + s, sec_buf);
                    // free the full cluster chain (not just first cluster)
                    // there must be a better way to do this... i'm sure of it...
                    {
                        unsigned int fc = cluster;
                        while (fc >= 2 && fc < FAT32_EOC) {
                            unsigned int next = fat32_next_cluster(fc);
                            fat32_write_fat(fc, 0);
                            fc = next;
                        }
                    }
                    fat2_flush();
                    return 0;
                }
            }
        }
        dcl = fat32_next_cluster(dcl);
    }
    return 0x80010002;
}

static int exfat_IoDopen(PspIoDrvFileArg *arg, const char *dirname)
{
    if (fs_initialized != 2) deferred_fs_init();
    reopen_blk_fd_if_needed();

    if (blk_fd < 0) {
        return -1;
    }

    unsigned int size = 0;
    unsigned char attr = 0;
    unsigned int cluster;
    int no_chain = 0;

    if (g_use_exfat) { int nc=0; cluster = exfat_resolve_path(dirname, &size, &attr, &nc); no_chain = nc; }
    else cluster = fat32_resolve_path(dirname, &size, &attr);

    if (cluster == 0)
        return 0x80010002;

    if (!(attr & FAT_ATTR_DIRECTORY))
        return 0x80010014;

    int fd = alloc_fd();
    if (fd < 0) {
        return 0x80000020; // emfile - too many open files (sony uses this)!
    }

    memset(&open_files[fd], 0, sizeof(OpenFile));
    open_files[fd].msstor_fd = -1;
    open_files[fd].in_use = 1;
    open_files[fd].is_dir = 1;
    open_files[fd].first_cluster = cluster;
    open_files[fd].dir_cluster = cluster;
    open_files[fd].no_fat_chain = no_chain;
    es_save_dirname(fd, dirname);

    arg->arg = (void *)fd;
    return 0;
}

static int exfat_IoDclose(PspIoDrvFileArg *arg)
{
    int fd = (int)arg->arg;
    if (fd >= 0 && fd < MAX_OPEN_FILES && open_files[fd].in_use) {
        open_files[fd].in_use = 0;
    }
    return 0;
}

static int exfat_IoDread(PspIoDrvFileArg *arg, SceIoDirent *dir)
{
    int fd = (int)arg->arg;
    if (fd < 0 || fd >= MAX_OPEN_FILES || !open_files[fd].in_use)
        return -1;

    OpenFile *f = &open_files[fd];
    if (!f->is_dir)
        return -1;

    // save d_private pointer before memset zeros it
    // the caller allocates pspmsprivatedirent and sets dir->d_private.
    // the io manager may zero it after we return, but the buffer itself persists
    // we write directly to the caller's buffer via this pointer! This is the big one!!
    void *saved_d_private = dir->d_private;

    memset(dir, 0, sizeof(SceIoDirent));

    // restore d_private so caller can still find their buffer
    dir->d_private = saved_d_private;

    if (f->first_cluster == 0)
        return 0;

    if (g_use_exfat) {
        // exfat directory enumeration parses 0x85→0xc0→0xc1 entry sets
        // standard filesystem jazz. uses the shared dir read cache to avoid
        // re-reading the same sector for consecutive 32-byte entries.
        unsigned char *drd_sec_buf = dir_rd_cache_buf;
        while (1) {
            if (f->dir_offset >= exfat_cluster_size) {
                if (f->no_fat_chain)
                    f->dir_cluster = f->dir_cluster + 1;
                else
                    f->dir_cluster = exfat_next_cluster(f->dir_cluster);
                f->dir_offset = 0;
                if (f->dir_cluster < 2 || f->dir_cluster >= FAT32_EOC)
                    return 0;
            }
            unsigned int sec = exfat_data_start_sector +
                (f->dir_cluster - 2) * exfat_sectors_per_cluster +
                (f->dir_offset / exfat_bytes_per_sector);
            unsigned int sec_off = f->dir_offset % exfat_bytes_per_sector;

            if (sec != dir_rd_cache_sec) {
                int ret = exfat_blk_read_sectors(sec, drd_sec_buf, 1);
                if (ret < 0) { dir_rd_cache_invalidate(); return 0; }
                dir_rd_cache_sec = sec;
            }

            unsigned char etype = drd_sec_buf[sec_off];
            f->dir_offset += 32;

            if (etype == 0x00) return 0; // end of directory
            if (!(etype & 0x80)) continue; // deleted/unused mostly
            if (etype != EXFAT_ENTRY_FILE) continue; // skip bitmap, upcase, volume label !

            ExFatFileEntry *fe = (ExFatFileEntry *)(drd_sec_buf + sec_off);
            int sec_count = fe->secondary_count;
            unsigned short file_attr = fe->file_attributes;

            unsigned int first_cl = 0;
            unsigned int fsize = 0;
            unsigned short ucs2_name[256];
            int name_pos = 0;
            memset(ucs2_name, 0, sizeof(ucs2_name));

            int si;
            for (si = 0; si < sec_count; si++) {
                if (f->dir_offset >= exfat_cluster_size) {
                    if (f->no_fat_chain)
                        f->dir_cluster = f->dir_cluster + 1;
                    else
                        f->dir_cluster = exfat_next_cluster(f->dir_cluster);
                    f->dir_offset = 0;
                    if (f->dir_cluster < 2 || f->dir_cluster >= FAT32_EOC) break;
                }
                sec = exfat_data_start_sector +
                    (f->dir_cluster - 2) * exfat_sectors_per_cluster +
                    (f->dir_offset / exfat_bytes_per_sector);
                sec_off = f->dir_offset % exfat_bytes_per_sector;

                if (sec != dir_rd_cache_sec) {
                    int ret2 = exfat_blk_read_sectors(sec, drd_sec_buf, 1);
                    if (ret2 < 0) { dir_rd_cache_invalidate(); break; }
                    dir_rd_cache_sec = sec;
                }

                unsigned char st = drd_sec_buf[sec_off];
                if (st == EXFAT_ENTRY_STREAM) {
                    ExFatStreamEntry *se = (ExFatStreamEntry *)(drd_sec_buf + sec_off);
                    first_cl = se->first_cluster;
                    fsize = (unsigned int)se->data_length;
                } else if (st == EXFAT_ENTRY_NAME) {
                    ExFatNameEntry *ne = (ExFatNameEntry *)(drd_sec_buf + sec_off);
                    int k;
                    for (k = 0; k < 15 && name_pos < 255; k++) {
                        unsigned short ch = ne->name[k];
                        if (ch == 0) break;
                        ucs2_name[name_pos++] = ch;
                    }
                }
                f->dir_offset += 32;
            }
            ucs2_name[name_pos] = 0;

            if (name_pos == 0 || first_cl < 2) continue; // malformed entry / maybe we could validate this without losing too much performance?

            // convert ucs-2 → local encoding via codepage driver (maybe make a version that skips codepage altogether for performance but decreased language support??)
            sceCodepage_driver_907CBFD2(dir->d_name, 256, ucs2_name);

            dir->d_stat.st_size = fsize;
            dir->d_stat.st_attr = (unsigned int)(file_attr & 0xFF);
            if (file_attr & EXFAT_ATTR_DIRECTORY)
                dir->d_stat.st_mode = 0x11FF;
            else
                dir->d_stat.st_mode = 0x21FF;

            // exfat timestamps from fileentry (fix attempot 3, this one was pesky with save data utility)
            {
                unsigned int cts = fe->create_timestamp;
                unsigned int mts = fe->modify_timestamp;
                unsigned int ats = fe->access_timestamp;
                unsigned short cd = (unsigned short)((cts >> 16) & 0xFFFF);
                unsigned short ct = (unsigned short)(cts & 0xFFFF);
                unsigned short md = (unsigned short)((mts >> 16) & 0xFFFF);
                unsigned short mt = (unsigned short)(mts & 0xFFFF);
                unsigned short ad = (unsigned short)((ats >> 16) & 0xFFFF);
                // again... probably don't need to waste cycles doing this. !NOTE: Revisit this as well
                fat_time_to_psp(cd, ct, &dir->d_stat.sce_st_ctime);
                fat_time_to_psp(md, mt, &dir->d_stat.sce_st_atime);
                fat_time_to_psp(ad, 0, &dir->d_stat.sce_st_mtime);
            }

            // d_private: sony fills msprivatedirent when caller provides it!
            // struct: { u32 size=0x414; char s_name[16]; char l_name[1024]; }
            if (saved_d_private) {
                unsigned int *dp = (unsigned int *)saved_d_private;
                if (dp[0] == 0x414) {
                    char *s_name = (char *)dp + 4;
                    char *l_name = (char *)dp + 0x14;
                    // exfat has no 8.3 sfn support so we gotta generate from lfn via codepage
                    sceCodepage_driver_907CBFD2(s_name, 13, ucs2_name);
                    // l_name = codepage-converted long name
                    sceCodepage_driver_907CBFD2(l_name, 0x400, ucs2_name);
                }
            }

            return 1;
        }
        return 0;
    }

    // fat32 directory enum also uses the shared dir read cache
    unsigned char *drd_sec_buf = dir_rd_cache_buf;
    unsigned short ucs2_buf[256]; // use the raw ucs-2 lfn for codepage conversion
    int ucs2_ready = 0;
    while (1) {
        if (f->dir_offset >= fat32_cluster_size) {
            f->dir_cluster = fat32_next_cluster(f->dir_cluster);
            f->dir_offset = 0;
            if (f->dir_cluster < 2 || f->dir_cluster >= FAT32_EOC)
                return 0;
        }

        unsigned int sec = cluster_to_sector(f->dir_cluster) + (f->dir_offset / fat32_bytes_per_sector);
        unsigned int sec_off = f->dir_offset % fat32_bytes_per_sector;

        if (sec != dir_rd_cache_sec) {
            int ret = blk_read_sectors(sec, drd_sec_buf, 1);
            if (ret < 0) { dir_rd_cache_invalidate(); return 0; }
            dir_rd_cache_sec = sec;
        }

        FatDirEntry *entry = (FatDirEntry *)(drd_sec_buf + sec_off);
        f->dir_offset += 32;

        if (entry->name[0] == 0x00)
            return 0;

        if (entry->name[0] == 0xE5) {
            memset(f->lfn_buf, 0, sizeof(f->lfn_buf));
            memset(ucs2_buf, 0, sizeof(ucs2_buf));
            f->lfn_ready = 0;
            ucs2_ready = 0;
            continue;
        }

        if (entry->attr == FAT_ATTR_LFN) {
            FatLfnEntry *lfn = (FatLfnEntry *)entry;
            int seq = lfn->order & 0x3F;
            if (lfn->order & 0x40) {
                memset(f->lfn_buf, 0, sizeof(f->lfn_buf));
                memset(ucs2_buf, 0, sizeof(ucs2_buf));
            }
            fat_extract_lfn_part(lfn, f->lfn_buf, seq);
            fat_extract_lfn_part_ucs2(lfn, ucs2_buf, seq);
            if (seq == 1) {
                f->lfn_ready = 1;
                ucs2_ready = 1;
            }
            continue;
        }

        if (entry->attr & FAT_ATTR_VOLUME_ID) {
            f->lfn_ready = 0;
            ucs2_ready = 0;
            continue;
        }

        // skip dot entries (sony's fatms skips "." and "..") .. yes i know we padded it then avoid it
        if (entry->name[0] == '.' &&
            (entry->name[1] == ' ' ||
             (entry->name[1] == '.' && entry->name[2] == ' '))) {
            f->lfn_ready = 0;
            ucs2_ready = 0;
            continue;
        }

        // d_name -> use codepage conversion from ucs-2 if lfn available
        if (f->lfn_ready && ucs2_ready && ucs2_buf[0]) {
            // null-terminate the ucs-2 buffer
            int ui;
            for (ui = 0; ui < 255 && ucs2_buf[ui] && ucs2_buf[ui] != 0xFFFF; ui++);
            ucs2_buf[ui] = 0;
            // convert ucs-2 -> local encoding via codepage driver
            sceCodepage_driver_907CBFD2(dir->d_name, 256, ucs2_buf);
        } else if (f->lfn_ready && f->lfn_buf[0]) {
            // fallback to ascii lfn (this actually works raw, but not when using sonys utilities)
            int i;
            for (i = 0; i < 255 && f->lfn_buf[i]; i++)
                dir->d_name[i] = f->lfn_buf[i];
            dir->d_name[i] = '\0';
        } else {
            fat_extract_shortname(entry, dir->d_name);
        }

        dir->d_stat.st_size = entry->file_size;
        if (entry->attr & FAT_ATTR_DIRECTORY)
            dir->d_stat.st_mode = 0x11FF;
        else
            dir->d_stat.st_mode = 0x21FF;
        // raw fat attribute byte, sony passes entry->attr directly
        dir->d_stat.st_attr = entry->attr;
        fat_fill_stat_times(entry, &dir->d_stat);

        // fill our privates
        if (dir->d_private) {
            unsigned int *dp = (unsigned int *)dir->d_private;
            if (dp[0] == 0x414) {
                char *s_name = (char *)dp + 4;
                char *l_name = (char *)dp + 0x14;
                fat_extract_shortname(entry, s_name);
                if (ucs2_ready && ucs2_buf[0]) {
                    sceCodepage_driver_907CBFD2(l_name, 0x400, ucs2_buf);
                } else {
                    int li;
                    for (li = 0; li < 0x3FF && dir->d_name[li]; li++)
                        l_name[li] = dir->d_name[li];
                    l_name[li] = '\0';
                }
            }
        }

        memset(f->lfn_buf, 0, sizeof(f->lfn_buf));
        f->lfn_ready = 0;
        return 1;
    }
}

static int exfat_IoGetstat(PspIoDrvFileArg *arg, const char *file, SceIoStat *stat)
{
    (void)arg;
    if (fs_initialized != 2) deferred_fs_init();
    reopen_blk_fd_if_needed();

    if (blk_fd < 0)
        return -1;

    if (es_initialized) {
        int r = es_try_getstat(file, stat);
        if (r >= 0) return r;

        // try to support GCLite's behavior
        const char *fp = file;
        while (*fp == '/') fp++;
        if (*fp && es_check_dir_prefix(fp)) {
            memset(stat, 0, sizeof(SceIoStat));
            stat->st_mode = 0x11FF;
            stat->st_attr = 0x10;
            stat->sce_st_ctime.year = 1992; stat->sce_st_ctime.month = 3; stat->sce_st_ctime.day = 27;
            stat->sce_st_mtime.year = 1992; stat->sce_st_mtime.month = 3; stat->sce_st_mtime.day = 27;
            stat->sce_st_atime.year = 1992; stat->sce_st_atime.month = 3; stat->sce_st_atime.day = 27;
            return 0;
        }
    }

    unsigned int size = 0;
    unsigned char attr = 0;
    unsigned int cluster;

    if (g_use_exfat) { int nc=0; cluster = exfat_resolve_path(file, &size, &attr, &nc); }
    else cluster = fat32_resolve_path(file, &size, &attr);

    if (cluster == 0 && size == 0 && attr == 0) {
        return 0x80010002;
    }

    memset(stat, 0, sizeof(SceIoStat));
    stat->st_size = size;
    if (attr & FAT_ATTR_DIRECTORY)
        stat->st_mode = 0x11FF;
    else
        stat->st_mode = 0x21FF;
    stat->st_attr = attr;

    if (g_use_exfat) {
        // walk parent dir to read actual ExFatFileEntry timestamps
        const char *fb = file; while (*fb == '/') fb++;
        const char *fl = NULL;
        { const char *fp = fb; while (*fp) { if (*fp == '/') fl = fp; fp++; } }
        unsigned int pcl = exfat_root_cluster;
        int parent_nc = 0;
        if (fl) {
            char pp[256]; int pl = (int)(fl - fb);
            if (pl > 255) pl = 255;
            memcpy(pp, fb, pl); pp[pl] = '\0';
            unsigned int ps=0; unsigned char pa=0; int pnc=0;
            pcl = exfat_resolve_path(pp, &ps, &pa, &pnc);
            if (pcl == 0) pcl = exfat_root_cluster;
            parent_nc = pnc;
        }
        unsigned char sb[512] __attribute__((aligned(64)));
        unsigned int dcl = pcl;
        while (dcl >= 2 && dcl < FAT32_EOC) {
            unsigned int sec = exfat_cluster_to_sector(dcl);
            unsigned int s;
            for (s = 0; s < exfat_sectors_per_cluster; s++) {
                if (exfat_blk_read_sectors(sec + s, sb, 1) >= 0) {
                    unsigned int off;
                    for (off = 0; off < 512; off += 32) {
                        if (sb[off] == 0x00) goto exfat_ts_done;
                        if (sb[off] != 0x85) continue;
                        if (off + 32 < 512 && sb[off + 32] == 0xC0) {
                            unsigned int scl = 0;
                            memcpy(&scl, sb + off + 32 + 20, 4);
                            if (scl == cluster) {
                                ExFatFileEntry *fe = (ExFatFileEntry *)(sb + off);
                                unsigned int cts = fe->create_timestamp;
                                unsigned int mts = fe->modify_timestamp;
                                unsigned int ats = fe->access_timestamp;
                                // !NOTE: Revisit this as well (relevance to GCLite?)
                                fat_time_to_psp((unsigned short)(cts >> 16), (unsigned short)(cts & 0xFFFF), &stat->sce_st_ctime);
                                fat_time_to_psp((unsigned short)(mts >> 16), (unsigned short)(mts & 0xFFFF), &stat->sce_st_atime);
                                fat_time_to_psp((unsigned short)(ats >> 16), 0, &stat->sce_st_mtime);
                                goto exfat_ts_done;
                            }
                        }
                    }
                }
            }
            if (parent_nc) dcl = dcl + 1;
            else dcl = exfat_next_cluster(dcl);
        }
    exfat_ts_done: ;
    } else if (cluster >= 2) {
        const char *fb = file; while (*fb == '/') fb++;
        const char *fl = NULL;
        { const char *fp = fb; while (*fp) { if (*fp == '/') fl = fp; fp++; } }
        unsigned int pcl = fat32_root_cluster;
        if (fl) {
            char pp[256]; int pl = (int)(fl - fb);
            if (pl > 255) pl = 255;
            memcpy(pp, fb, pl); pp[pl] = '\0';
            unsigned int ps=0; unsigned char pa=0;
            pcl = fat32_resolve_path(pp, &ps, &pa);
            if (pcl == 0) pcl = fat32_root_cluster;
        }
        unsigned char sb[512] __attribute__((aligned(64)));
        unsigned int dcl = pcl;
        while (dcl >= 2 && dcl < FAT32_EOC) {
            unsigned int sec = cluster_to_sector(dcl);
            unsigned int s;
            for (s = 0; s < fat32_sectors_per_cluster; s++) {
                if (blk_read_sectors(sec + s, sb, 1) >= 0) {
                    unsigned int e;
                    for (e = 0; e < fat32_bytes_per_sector / 32; e++) {
                        FatDirEntry *de = (FatDirEntry *)(sb + e * 32);
                        if (de->name[0] == 0x00) goto ts_done;
                        if (de->name[0] == 0xE5 || de->attr == FAT_ATTR_LFN) continue;
                        unsigned int cl = ((unsigned int)de->cluster_hi << 16) | de->cluster_lo;
                        if (cl == cluster) {
                            fat_fill_stat_times(de, stat);
                            goto ts_done;
                        }
                    }
                }
            }
            dcl = fat32_next_cluster(dcl);
        }
    }
ts_done:
    return 0;
}

static int exfat_IoChstat(PspIoDrvFileArg *arg, const char *file, SceIoStat *stat, int bits)
{
    (void)arg;
    if (fs_initialized != 2) deferred_fs_init();
    if (blk_fd < 0) return -1;
    if (g_use_exfat) {
        unsigned int fsz = 0; unsigned char fat2 = 0; int fnc = 0;
        unsigned int fcl = exfat_resolve_path(file, &fsz, &fat2, &fnc);
        if (fcl == 0) return 0x80010002;
        const char *fb2 = file; while (*fb2 == '/') fb2++;
        const char *fl2 = NULL; const char *fp2 = fb2;
        while (*fp2) { if (*fp2 == '/') fl2 = fp2; fp2++; }
        unsigned int pcl2 = exfat_root_cluster;
        if (fl2) {
            char pp[256]; int pl = (int)(fl2 - fb2); if (pl > 255) pl = 255;
            memcpy(pp, fb2, pl); pp[pl] = '\0';
            unsigned int ps = 0; unsigned char pa = 0; int pnc = 0;
            pcl2 = exfat_resolve_path(pp, &ps, &pa, &pnc);
            if (pcl2 == 0) return 0x80010002;
        }
        // walk parent dir to find matching file entry and update attributes
        unsigned char sb2[512] __attribute__((aligned(64)));
        unsigned int dcl2 = pcl2;
        while (dcl2 >= 2 && dcl2 < FAT32_EOC) {
            unsigned int sec2 = exfat_cluster_to_sector(dcl2);
            unsigned int s2;
            for (s2 = 0; s2 < exfat_sectors_per_cluster; s2++) {
                if (exfat_blk_read_sectors(sec2 + s2, sb2, 1) < 0) return -1;
                unsigned int off2;
                for (off2 = 0; off2 < 512; off2 += 32) {
                    if (sb2[off2] == 0x00) return 0x80010002;
                    if (sb2[off2] != 0x85) continue;
                    if (off2 + 32 < 512 && sb2[off2 + 32] == 0xC0) {
                        unsigned int scl = 0;
                        memcpy(&scl, sb2 + off2 + 32 + 20, 4);
                        if (scl == fcl) {
                            // update file_attributes
                            if (bits & 0x01) {
                                unsigned short fa = 0;
                                memcpy(&fa, sb2 + off2 + 4, 2);
                                fa = (fa & 0x10) | (stat->st_attr & ~0x10);
                                memcpy(sb2 + off2 + 4, &fa, 2);
                            }
                            // try to match sony so thestat bytes round-trip through chstat exactly like stock fatms
                            { unsigned short td, tt;
                            ExFatFileEntry *efe = (ExFatFileEntry *)(sb2 + off2);
                            if (bits & 0x04) {
                                psp_to_fat_time(&stat->sce_st_atime, &td, &tt);
                                efe->modify_timestamp = ((unsigned int)td << 16) | tt;
                            }
                            if (bits & 0x08) {
                                psp_to_fat_time(&stat->sce_st_ctime, &td, &tt);
                                efe->create_timestamp = ((unsigned int)td << 16) | tt;
                            }
                            if (bits & 0x10) {
                                psp_to_fat_time(&stat->sce_st_mtime, &td, &tt);
                                efe->access_timestamp = ((unsigned int)td << 16) | tt;
                            } }
                            exfat_blk_write_sectors(sec2 + s2, sb2, 1);
                            return 0;
                        }
                    }
                }
            }
            dcl2 = exfat_next_cluster(dcl2);
        }
        return 0x80010002;
    }

    unsigned int fsize = 0; unsigned char fattr = 0;
    unsigned int cluster = fat32_resolve_path(file, &fsize, &fattr);
    if (cluster == 0 && fsize == 0 && fattr == 0) return 0x80010002;

    const char *fb = file; while (*fb == '/') fb++;
    const char *fl = NULL;
    { const char *fp = fb; while (*fp) { if (*fp == '/') fl = fp; fp++; } }
    unsigned int pcl = fat32_root_cluster;
    if (fl) {
        char pp[256]; int pl = (int)(fl - fb);
        if (pl > 255) pl = 255;
        memcpy(pp, fb, pl); pp[pl] = '\0';
        unsigned int ps=0; unsigned char pa=0;
        pcl = fat32_resolve_path(pp, &ps, &pa);
        if (pcl == 0) return 0x80010002;
    }

    // walk the dir, find entry by cluster, update fields
    // i know for sure we can optimize this...
    unsigned char sb[512] __attribute__((aligned(64)));
    unsigned int dcl = pcl;
    while (dcl >= 2 && dcl < FAT32_EOC) {
        unsigned int sec = cluster_to_sector(dcl);
        unsigned int s;
        for (s = 0; s < fat32_sectors_per_cluster; s++) {
            if (blk_read_sectors(sec + s, sb, 1) < 0) return -1;
            unsigned int e;
            for (e = 0; e < fat32_bytes_per_sector / 32; e++) {
                FatDirEntry *de = (FatDirEntry *)(sb + e * 32);
                if (de->name[0] == 0x00) return 0x80010002;
                if (de->name[0] == 0xE5 || de->attr == FAT_ATTR_LFN) continue;
                unsigned int cl = ((unsigned int)de->cluster_hi << 16) | de->cluster_lo;
                if (cl == cluster) {
                    // update attributes if requested (bit 0 of bits)
                    if (bits & 0x01) {
                        de->attr = (de->attr & FAT_ATTR_DIRECTORY) |
                                   (stat->st_attr & ~FAT_ATTR_DIRECTORY);
                    }
                    // sony byte layout: modify@0x20 (pspsdk sce_st_atime),
                    // access@0x30 (pspsdk sce_st_mtime)
                    { unsigned short td, tt;
                    if (bits & 0x04) {
                        psp_to_fat_time(&stat->sce_st_atime, &td, &tt);
                        de->modify_date = td; de->modify_time = tt;
                    }
                    if (bits & 0x08) {
                        psp_to_fat_time(&stat->sce_st_ctime, &td, &tt);
                        de->create_date = td; de->create_time = tt;
                    }
                    if (bits & 0x10) {
                        psp_to_fat_time(&stat->sce_st_mtime, &td, &tt);
                        de->access_date = td;
                    } }
                    blk_write_sectors(sec + s, sb, 1);
                    return 0;
                }
            }
        }
        dcl = fat32_next_cluster(dcl);
    }
    return 0x80010002;
}

static int exfat_IoRename(PspIoDrvFileArg *arg, const char *oldname, const char *newname)
{
    (void)arg;
    if (fs_initialized != 2) deferred_fs_init();

    if (blk_fd < 0) return -1;
    if (g_use_exfat) {
        // exfat rename: delete old entry, create new entry in ioopen o_creat style
        // for now: delete old dir entry, let the caller re-create via ioopen
        // this matches the _delete_ rename flow (rename = delete old + create new name)
        // we have to do this manually because exfat is new
        unsigned int osz = 0; unsigned char oat = 0; int onc = 0;
        unsigned int ocl = exfat_resolve_path(oldname, &osz, &oat, &onc);
        if (ocl == 0) return 0x80010002;
        // check dest doesn't exist - sony returns eexist
        unsigned int dsz = 0; unsigned char dat = 0; int dnc = 0;
        unsigned int dcl = exfat_resolve_path(newname, &dsz, &dat, &dnc);
        if (dcl != 0) return 0x80010011; // eexist it
        const char *ob = oldname; while (*ob == '/') ob++;
        const char *ol = NULL; const char *op = ob;
        while (*op) { if (*op == '/') ol = op; op++; }
        unsigned int opcl = exfat_root_cluster;
        int old_parent_nc = 0;
        if (ol) {
            char pp[256]; int pl = (int)(ol - ob); if (pl > 255) pl = 255;
            memcpy(pp, ob, pl); pp[pl] = '\0';
            unsigned int ps = 0; unsigned char pa = 0; int pnc = 0;
            opcl = exfat_resolve_path(pp, &ps, &pa, &pnc);
            if (opcl == 0) return 0x80010002;
            old_parent_nc = pnc;
        }
        // delete old entry (keeps data clusters intact - just removes dir entry)
        exfat_delete_dir_entry(opcl, ocl, old_parent_nc);
        const char *nb = newname; while (*nb == '/') nb++;
        const char *nl = NULL; const char *np2 = nb;
        while (*np2) { if (*np2 == '/') nl = np2; np2++; }
        unsigned int npcl = opcl;
        const char *nfname = nb;
        if (nl) {
            char pp[256]; int pl = (int)(nl - nb); if (pl > 255) pl = 255;
            memcpy(pp, nb, pl); pp[pl] = '\0';
            nfname = nl + 1;
            unsigned int ps = 0; unsigned char pa = 0; int pnc = 0;
            npcl = exfat_resolve_path(pp, &ps, &pa, &pnc);
            if (npcl == 0) return 0x80010002;
        }
        unsigned short new_attrs = (oat & 0x10) ? EXFAT_ATTR_DIRECTORY : EXFAT_ATTR_ARCHIVE;
        unsigned long long new_dlen = (unsigned long long)osz;
        int wret = exfat_write_dir_entry(npcl, nfname, new_attrs, ocl, new_dlen);
        return wret;
    }

    unsigned int old_size = 0; unsigned char old_attr = 0;
    unsigned int old_cluster = fat32_resolve_path(oldname, &old_size, &old_attr);
    if (old_cluster == 0) return 0x80010002;
    // sony only checks readonly (0x01) for rename - allows directories
    if (old_attr & 0x01) return 0x8001000D; // eacces - hmmm...

    unsigned int dst_size = 0; unsigned char dst_attr = 0;
    unsigned int dst_cluster = fat32_resolve_path(newname, &dst_size, &dst_attr);
    if (dst_cluster != 0) return 0x80010011;

    const char *ob = oldname; while (*ob == '/') ob++;
    const char *ol = NULL;
    { const char *p = ob; while (*p) { if (*p == '/') ol = p; p++; } }
    unsigned int old_parent = fat32_root_cluster;
    if (ol) {
        char pp[256]; int pl = (int)(ol - ob); if (pl > 255) pl = 255;
        memcpy(pp, ob, pl); pp[pl] = '\0';
        unsigned int ps=0; unsigned char pa=0;
        old_parent = fat32_resolve_path(pp, &ps, &pa);
        if (old_parent == 0) return 0x80010002;
    }

    const char *nb = newname; while (*nb == '/') nb++;
    const char *nl = NULL;
    { const char *p = nb; while (*p) { if (*p == '/') nl = p; p++; } }
    unsigned int new_parent = old_parent;
    const char *new_fname = nb;
    if (nl) {
        char pp[256]; int pl = (int)(nl - nb); if (pl > 255) pl = 255;
        memcpy(pp, nb, pl); pp[pl] = '\0';
        new_fname = nl + 1;
        unsigned int ps=0; unsigned char pa=0;
        new_parent = fat32_resolve_path(pp, &ps, &pa);
        if (new_parent == 0) return 0x80010002;
    }

    if (old_parent == new_parent) {
        // same directory - just rename the entry in place
        unsigned char sec_buf[512] __attribute__((aligned(64)));
        unsigned int dcl = old_parent;
        while (dcl >= 2 && dcl < FAT32_EOC) {
            unsigned int sec = cluster_to_sector(dcl);
            unsigned int s;
            for (s = 0; s < fat32_sectors_per_cluster; s++) {
                if (blk_read_sectors(sec + s, sec_buf, 1) < 0) return -1;
                unsigned int e;
                for (e = 0; e < fat32_bytes_per_sector / 32; e++) {
                    FatDirEntry *de = (FatDirEntry *)(sec_buf + e * 32);
                    if (de->name[0] == 0x00) return 0x80010002;
                    if (de->name[0] == 0xE5 || de->attr == FAT_ATTR_LFN) continue;
                    unsigned int cl = ((unsigned int)de->cluster_hi << 16) | de->cluster_lo;
                    if (cl == old_cluster) {
                        { int le = (int)e - 1;
                          while (le >= 0) {
                              FatDirEntry *ld = (FatDirEntry *)(sec_buf + le * 32);
                              if (ld->attr != FAT_ATTR_LFN) break;
                              ld->name[0] = 0xE5; le--;
                          }
                        }
                        // delete old sfn
                        de->name[0] = 0xE5; 
                        blk_write_sectors(sec + s, sec_buf, 1);
                        blk_wr_cache_push(sec + s, sec_buf);
                        // create new entry with lfn in same parent
                        FatDirEntry nde;
                        memcpy(&nde, de, 32); // copy the attrs/cluster/size
                        nde.name[0] = 0; // clear for make_sfn
                        make_sfn(new_fname, nde.name);
                        // sony's iorename is a pure rename - no _delete_ cleanup
                        // save utility calls ioremove + iormdir separately afterward from whatt I've seen
                        return fat32_write_dir_entry_lfn(old_parent, &nde, new_fname, NULL, NULL);
                    }
                }
            }
            dcl = fat32_next_cluster(dcl);
        }
        return 0x80010002;
    } else {
        // cross-directory: find old entry, copy it, delete old, create in new parent
        unsigned char sec_buf[512] __attribute__((aligned(64)));
        FatDirEntry saved_de;
        int found = 0;
        unsigned int dcl = old_parent;
        while (dcl >= 2 && dcl < FAT32_EOC && !found) {
            unsigned int sec = cluster_to_sector(dcl);
            unsigned int s;
            for (s = 0; s < fat32_sectors_per_cluster && !found; s++) {
                if (blk_read_sectors(sec + s, sec_buf, 1) < 0) return -1;
                unsigned int e;
                for (e = 0; e < fat32_bytes_per_sector / 32; e++) {
                    FatDirEntry *de = (FatDirEntry *)(sec_buf + e * 32);
                    if (de->name[0] == 0x00) break;
                    if (de->name[0] == 0xE5 || de->attr == FAT_ATTR_LFN) continue;
                    unsigned int cl = ((unsigned int)de->cluster_hi << 16) | de->cluster_lo;
                    if (cl == old_cluster) {
                        memcpy(&saved_de, de, 32);
                        // delete old entry + lfn
                        { int le = (int)e - 1;
                          while (le >= 0) {
                              FatDirEntry *ld = (FatDirEntry *)(sec_buf + le * 32);
                              if (ld->attr != FAT_ATTR_LFN) break;
                              ld->name[0] = 0xE5; le--;
                          }
                        }
                        de->name[0] = 0xE5;
                        blk_write_sectors(sec + s, sec_buf, 1);
                        blk_wr_cache_push(sec + s, sec_buf);
                        found = 1;
                        break;
                    }
                }
            }
            dcl = fat32_next_cluster(dcl);
        }
        if (!found) return 0x80010002;
        make_sfn(new_fname, saved_de.name);
        return fat32_write_dir_entry_lfn(new_parent, &saved_de, new_fname, NULL, NULL);
    }
}

static int exfat_IoChdir(PspIoDrvFileArg *arg, const char *dir)
{
    (void)arg; (void)dir;
    return 0;
}

// from my findingsthe actual kernel signature has 6 params (pspsdk declares 1, which is wrong)
// df_mount(iob, fs_name, blockdev, mode, unk1, unk2)
// we declare extra params to match the real abi, the cast is in the function table below.
static int exfat_IoMount(PspIoDrvFileArg *arg, const char *fs_name,
                          const char *blockdev, int mode, void *unk1, int unk2)
{
    (void)arg; (void)fs_name; (void)mode; (void)unk1; (void)unk2;

    // sony's sub_0ba10, it calls block device devctls to initialize
    if (blockdev && blk_fd >= 0) {
        k_sceIoDevctl(blockdev, 0x0211D814, NULL, 0, NULL, 0);
        k_sceIoDevctl(blockdev, 0x0210D816, NULL, 0, NULL, 0);
    }

    mount_state = 0x1020;
    media_flags = 0x2000; // media present flag

    // notify all registered callbacks when card inserted
    notify_all_callbacks(1);

    // signal the media thread as poer usual
    if (event_flag_id >= 0)
        sceKernelSetEventFlag(event_flag_id, EVT_INSERT);

    return 0;
}

// the actual kernel sig has 2 params
static int exfat_IoUmount(PspIoDrvFileArg *arg, const char *devname)
{
    (void)arg; (void)devname;

    // sony's sub_0ba94 will close all open files/dirs on unmount
    // flush dirty writable files before clearing in_use
    {
        int i;
        for (i = 0; i < MAX_OPEN_FILES; i++) {
            if (open_files[i].in_use) {
                OpenFile *f = &open_files[i];
                if (f->is_dirty && f->writable && !f->is_dir) {
                    if (g_use_exfat) {
                        exfat_update_dir_entry(f->parent_cluster, f->first_cluster, f->first_cluster, f->file_size, f->parent_no_fat_chain);
                    } else if (f->sfn_sector != 0) {
                        unsigned char ubuf[512] __attribute__((aligned(64)));
                        if (blk_read_sectors(f->sfn_sector, ubuf, 1) >= 0) {
                            FatDirEntry *de = (FatDirEntry *)(ubuf + f->sfn_index);
                            de->file_size = f->file_size;
                            de->cluster_hi = (unsigned short)(f->first_cluster >> 16);
                            de->cluster_lo = (unsigned short)(f->first_cluster & 0xFFFF);
                            de->attr |= FAT_ATTR_ARCHIVE;
                            blk_write_sectors(f->sfn_sector, ubuf, 1);
                            blk_wr_cache_push(f->sfn_sector, ubuf);
                        }
                    }
                    fat2_flush();
                }
                open_files[i].in_use = 0;
            }
        }
    }

    mount_state = 0x1022;
    media_flags = 0x4000; // media removed

    // notify all registered callbacks when card removed
    notify_all_callbacks(2);

    if (event_flag_id >= 0)
        sceKernelSetEventFlag(event_flag_id, EVT_REMOVE);

    return 0;
}

static int exfat_IoDevctl(PspIoDrvFileArg *arg, const char *devname,
                          unsigned int cmd, void *indata, int inlen,
                          void *outdata, int outlen)
{
    (void)arg; (void)devname;
    if (fs_initialized != 2) deferred_fs_init();
    reopen_blk_fd_if_needed();

    switch (cmd) {

    // xmb registers a callback uid to be notified on card insert/remove
    // if thje card is already mounted, immediately notify with arg=1 (insert)
    case 0x02415821:
        if (!indata || inlen < 4)
            return 0x80010016;
        {
            SceUID cbid = *(SceUID *)indata;
            if (cb_count < MAX_CALLBACKS) {
                cb_uids[cb_count++] = cbid;
            }
            // notify only inline! do not also set evt_insert, that causes
            // a double notification which crashes xmb's card state machine (trust me i learned this the hard way)
            if (mount_state == 0x1020) {
                sceKernelNotifyCallback(cbid, 1);
            }
        }
        return 0;

    // this is the unregister callback
    case 0x02415822:
        if (!indata || inlen < 4)
            return 0x80010016;
        {
            SceUID cbid = *(SceUID *)indata;
            int j;
            for (j = 0; j < cb_count; j++) {
                if (cb_uids[j] == cbid) {
                    cb_uids[j] = cb_uids[--cb_count];
                    break;
                }
            }
        }
        return 0;

    // this gets devicesize
    // devicesize struct passed via p2p through indata
    // some callers also pass it via outdata so we shoudl handle both.
    case 0x02425818:
        if (mount_state != 0x1020)
            return 0x80020321; // sce_error_errno_device_not_found (this is important!)
        {
            unsigned int *ds = NULL;
            if (indata && inlen >= 4)
                ds = *(unsigned int **)indata;
            if (!ds && outdata && outlen >= 20)
                ds = (unsigned int *)outdata;
            if (!ds)
                return 0x80010016;
            // this struct is from ark-4's stargate/io_patch devicesize struct:
            // ds[0] = maxclusters
            // ds[1] = freeclusters
            // ds[2] = maxsectors
            // ds[3] = sectorsize (bytes per sector)
            // ds[4] = sectorcount (sectors per cluster)
            if (g_use_exfat) {
                ds[0] = exfat_cluster_count;
                ds[1] = exfat_free_clusters;
                ds[2] = exfat_cluster_count * exfat_sectors_per_cluster;
                ds[3] = exfat_bytes_per_sector;
                ds[4] = exfat_sectors_per_cluster;
            } else {
                ds[0] = fat32_total_clusters;
                ds[1] = fat32_free_clusters;
                ds[2] = fat32_total_clusters * fat32_sectors_per_cluster;
                ds[3] = fat32_bytes_per_sector;
                ds[4] = fat32_sectors_per_cluster;
            }
        }
        return 0;

    // query the mount state
    // returns 1 if mounted (0x1020), 0 otherwise.
    case 0x02425823:
        if (outdata && outlen >= 4)
            *(int *)outdata = (mount_state == 0x1020) ? 1 : 0;
        return 0;

    // check if the device is readyy
    // on real ms pro, this forwards ioctl 0x02125009. on sd adapters, ioctl may fail
    // so fall back to "ready" since we're clearly reading from it.
    case 0x02425824:
        if (outdata && outlen >= 4) {
            if (blk_fd >= 0) {
                int tmp = 0;
                int r = k_sceIoIoctl(blk_fd, 0x02125009, NULL, 0, &tmp, 4);
                if (r >= 0) {
                    *(int *)outdata = (tmp == 1) ? 1 : 0;
                } else {
                    *(int *)outdata = 1; // sd adapter ioctl unsupported, but device still works !?
                }
            } else {
                *(int *)outdata = 0;
            }
            return 0;
        }
        return 0x80010016;

    // get the sector count
    case 0x02425825:
        if (outdata && outlen >= 4 && blk_fd >= 0) {
            int r = k_sceIoIoctl(blk_fd, 0x02125008, NULL, 0, outdata, outlen);
            if (r < 0) {
                // sd adapter fallback to returning the total sectors from bpb
                *(int *)outdata = (int)(fat32_total_clusters * fat32_sectors_per_cluster);
                return 0;
            }
            return r;
        }
        return 0x80010016;

    // test getting the insertion state
    case 0x02015804:
        if (outdata && outlen >= 4)
            *(int *)outdata = (mount_state == 0x1020) ? 1 : 0;
        return 0;

    // this is the ms controller status
    // xmb polls this to detect the card state, use mount_state which isupdated 
    // by sysevent handler on eject (0x00100000) and ioumount
    case 0x02025801:
        if (outdata && outlen >= 4)
            *(int *)outdata = (mount_state == 0x1020) ? 4 : 0; // 4 means ready, 0 meansremoved
        return 0;

    // the card type changed
    // no hot swap support sadly so the physical card type never changes
    case 0x02025806:
        if (outdata && outlen >= 4)
            *(int *)outdata = 0;
        return 0;

    // register the controller callback
    // accept but don't forward this, we handle card events via our own eventflag
    case 0x0203D802:
        return 0;

    // unregister the controller callback but again, don't forward
    case 0x0201D803:
        return 0;

    // forcing remount
    // called by system after usb mode exits. sony's media thread processes
    // evt_remove+evt_insert but our media thread is empty
    // let's handle inline and close the stale fd, reset init state, notify removal then insertion
    case 0x0240D81E:
        if (blk_fd >= 0) {
            k_sceIoClose(blk_fd);
            blk_fd = -1; blk_fd_pos = 0xFFFFFFFFFFFFFFFFULL;
        }
        blk_fd_validated = 0;
        fat_cached_sector = 0xFFFFFFFF;
        exfat_fat_cached_sector = 0xFFFFFFFF;
        blk_wr_cache_sector[0] = 0xFFFFFFFF;
        blk_wr_cache_sector[1] = 0xFFFFFFFF;
        dir_rd_cache_invalidate();
        // force the full reinit on the next vfs call
        fs_initialized = 0;
        mount_state = 0x1022;
        media_flags = 0x4000;
        notify_all_callbacks(2);
        // then reinit like a card insertion
        deferred_fs_init();
        reopen_blk_fd_if_needed();
        if (blk_fd >= 0) {
            mount_state = 0x1020;
            media_flags = 0x2000;
            notify_all_callbacks(1);
        }
        return 0;

    // formatting
    // format the card as fat32 or exfat (and maybe fat16 in future?)
    // if indata is a 4-byte int: 0=fat32, 1=exfat. otherwise uses the current type.
    // what we should do is detect the card typpe and format accordingly but if no type is detected
    // just default to fat32
    case 0x0241D813:
    case ES_DEVCTL_FORMAT: {
        if (blk_fd < 0) return -1;
        int fmt_exfat = g_use_exfat; // default: keep current type
        if (indata && inlen >= 4) {
            int requested = *(int *)indata;
            if (requested == 0) fmt_exfat = 0;      // force fat32
            else if (requested == 1) fmt_exfat = 1;  // force exfat
        }
        unsigned char fbuf[512] __attribute__((aligned(64)));

        // get card size, scan all mbr entries to find the furthest extent
        unsigned int phys_sectors = 0;
        {
            unsigned char mbr_scan[512] __attribute__((aligned(64)));
            k_sceIoLseek64k(blk_fd, 0, 0, 0, PSP_SEEK_SET);
            if (k_sceIoRead(blk_fd, mbr_scan, 512) == 512
                && mbr_scan[510] == 0x55 && mbr_scan[511] == 0xAA) {
                int pi;
                for (pi = 0; pi < 4; pi++) {
                    unsigned char *e = mbr_scan + 0x1BE + pi * 16;
                    if (e[4] == 0) continue;
                    unsigned int lba = e[8]|(e[9]<<8)|(e[10]<<16)|(e[11]<<24);
                    unsigned int ns  = e[12]|(e[13]<<8)|(e[14]<<16)|(e[15]<<24);
                    unsigned int end = lba + ns;
                    if (end > phys_sectors) phys_sectors = end;
                }
            }
            blk_fd_pos = 0xFFFFFFFFFFFFFFFFULL;
        }
        if (phys_sectors < 2048 && partition_start_sector > 0 && fat32_total_sectors > 0)
            phys_sectors = partition_start_sector + fat32_total_sectors;
        if (phys_sectors < 2048) return -1;

        // detect existing es partition before formatting
        // save the es mbr entry so we can restore it after formatting
        unsigned char es_entry[16];
        int es_slot = -1;
        unsigned int es_lba_saved = 0;
        {
            unsigned char mbr_check[512] __attribute__((aligned(64)));
            k_sceIoLseek64k(blk_fd, 0, 0, 0, PSP_SEEK_SET);
            if (k_sceIoRead(blk_fd, mbr_check, 512) == 512
                && mbr_check[510] == 0x55 && mbr_check[511] == 0xAA) {
                int pi;
                for (pi = 0; pi < 4; pi++) {
                    unsigned char *e = mbr_check + 0x1BE + pi * 16;
                    if (e[4] == ES_PARTITION_TYPE) {
                        memcpy(es_entry, e, 16);
                        es_slot = pi;
                        es_lba_saved = e[8]|(e[9]<<8)|(e[10]<<16)|(e[11]<<24);
                        break;
                    }
                }
            }
            blk_fd_pos = 0xFFFFFFFFFFFFFFFFULL;
        }

        // determine partition start and sectors_per_cluster
        unsigned int p_start = 2048; // standard 1mb alignment / this should be good enough for DCARK / TM right?
        unsigned int p_sectors = phys_sectors - p_start;
        // if es partition exists, shrink the fs partition to end before es area
        if (es_lba_saved > p_start)
            p_sectors = es_lba_saved - p_start;
        unsigned int spc;
        if (fmt_exfat) {
            spc = 256; // 128kb clusters for exfat (optimal for large files)
        } else {
            spc = 64;  // 32kb clusters for fat32 (crapioca)
        }

        // write the mbr at absolute sector 0
        memset(fbuf, 0, 512);
        // partition entry 0 at offset 446 is the fs partition
        fbuf[446] = 0x80;  
        fbuf[447] = 0xFE; fbuf[448] = 0xFF; fbuf[449] = 0xFF;
        fbuf[450] = fmt_exfat ? 0x07 : 0x0C;
        fbuf[451] = 0xFE; fbuf[452] = 0xFF; fbuf[453] = 0xFF;
        fbuf[454] = (unsigned char)(p_start);
        fbuf[455] = (unsigned char)(p_start >> 8);
        fbuf[456] = (unsigned char)(p_start >> 16);
        fbuf[457] = (unsigned char)(p_start >> 24);
        fbuf[458] = (unsigned char)(p_sectors);
        fbuf[459] = (unsigned char)(p_sectors >> 8);
        fbuf[460] = (unsigned char)(p_sectors >> 16);
        fbuf[461] = (unsigned char)(p_sectors >> 24);
        // restore the es partition entry if it existed
        if (es_slot >= 0) {
            memcpy(fbuf + 0x1BE + es_slot * 16, es_entry, 16);
        }
        fbuf[510] = 0x55; fbuf[511] = 0xAA;
        // raw write to absolute sector 0
        k_sceIoLseek64k(blk_fd, 0, 0, 0, PSP_SEEK_SET);
        k_sceIoWrite(blk_fd, fbuf, 512);
        // invalidate cached position
        blk_fd_pos = 0xFFFFFFFFFFFFFFFFULL;

        // update the partition_start_sector for blk_write_sectors
        partition_start_sector = p_start;

        if (fmt_exfat) {
            // exFAT Format
            unsigned int spc_shift = 0;
            { unsigned int t = spc; while (t > 1) { spc_shift++; t >>= 1; } }
            // match real exfat layout with fat at 1mb offset, heap at cluster-aligned boundary
            unsigned int fat_offset = 2048; // 1mb into partition (matches the real exfat layout)
            unsigned int fat_entries = (p_sectors - fat_offset) / spc;
            unsigned int fat_length = (fat_entries * 4 + 511) / 512;
            fat_length = (fat_length + spc - 1) & ~(spc - 1);
            unsigned int heap_offset = fat_offset + fat_length;
            { unsigned int align = 32768; // 16mb alignment matching real exfat
              if (heap_offset < align) heap_offset = align;
              else heap_offset = (heap_offset + align - 1) & ~(align - 1); }
            unsigned int cluster_count = (p_sectors - heap_offset) / spc;
            unsigned int bitmap_bytes = (cluster_count + 7) / 8;
            unsigned int bitmap_sectors = (bitmap_bytes + 511) / 512;
            unsigned int bitmap_clusters = (bitmap_sectors + spc - 1) / spc;
            // upcase table ~6kb = 1 cluster (fits in 32kb or 128kb)
            unsigned int upcase_clusters = 1;
            unsigned int root_cluster = 2 + bitmap_clusters + upcase_clusters;

            memset(fbuf, 0, 512);
            fbuf[0] = 0xEB; fbuf[1] = 0x76; fbuf[2] = 0x90; // jmp + nop
            memcpy(fbuf + 3, "EXFAT   ", 8);
            // bytes 11-63: must be zero (legacy bpb area)
            *(unsigned long long *)(fbuf + 64) = (unsigned long long)p_start;
            *(unsigned long long *)(fbuf + 72) = (unsigned long long)p_sectors;
            *(unsigned int *)(fbuf + 80) = fat_offset;
            *(unsigned int *)(fbuf + 84) = fat_length;
            *(unsigned int *)(fbuf + 88) = heap_offset;
            *(unsigned int *)(fbuf + 92) = cluster_count;
            *(unsigned int *)(fbuf + 96) = root_cluster;
            *(unsigned int *)(fbuf + 100) = 0x12345678;
            *(unsigned short *)(fbuf + 104) = 0x0100;
            // bytespersectorshift
            fbuf[108] = 9; // 512 bytes
            fbuf[109] = (unsigned char)spc_shift;
            fbuf[110] = 1;
            // driveselect
            fbuf[111] = 0x80;
            // percentinuse
            fbuf[112] = 0x00;
            // boot sig
            fbuf[510] = 0x55; fbuf[511] = 0xAA;
            blk_write_sectors(0, fbuf, 1);

            // backup vbr at sector 12
            blk_write_sectors(12, fbuf, 1);

            // extended boot sectors 1-8 has signature 0xaa550000 at offset 508 (we're not gonna deviate much)
            memset(fbuf, 0, 512);
            *(unsigned int *)(fbuf + 508) = 0xAA550000;
            { unsigned int zs; for (zs = 1; zs <= 8; zs++) blk_write_sectors(zs, fbuf, 1); }
            // sectors 9-10 are oem parameters + reserved (all zeros typiucaslly)
            memset(fbuf, 0, 512);
            blk_write_sectors(9, fbuf, 1);
            blk_write_sectors(10, fbuf, 1);
            {
                unsigned int cksum = 0;
                unsigned int sec;
                for (sec = 0; sec <= 10; sec++) {
                    unsigned char cbuf[512] __attribute__((aligned(64)));
                    blk_read_sectors(sec, cbuf, 1);
                    unsigned int bi;
                    for (bi = 0; bi < 512; bi++) {
                        if (sec == 0 && (bi == 106 || bi == 107 || bi == 112))
                            continue;
                        cksum = ((cksum >> 1) | (cksum << 31)) + (unsigned int)cbuf[bi];
                    }
                }
                // write checksum sector 11 (uint32 repeated 128 times)
                unsigned int *cs = (unsigned int *)fbuf;
                { unsigned int ci; for (ci = 0; ci < 128; ci++) cs[ci] = cksum; }
                blk_write_sectors(11, fbuf, 1);
            }

            memset(fbuf, 0, 512);
            *(unsigned int *)(fbuf + 508) = 0xAA550000;
            { unsigned int zs; for (zs = 13; zs <= 20; zs++) blk_write_sectors(zs, fbuf, 1); }
            memset(fbuf, 0, 512);
            blk_write_sectors(21, fbuf, 1);
            blk_write_sectors(22, fbuf, 1);

            {
                unsigned int cksum = 0;
                unsigned int sec;
                for (sec = 12; sec <= 22; sec++) {
                    unsigned char cbuf[512] __attribute__((aligned(64)));
                    blk_read_sectors(sec, cbuf, 1);
                    unsigned int bi;
                    for (bi = 0; bi < 512; bi++) {
                        if (sec == 12 && (bi == 106 || bi == 107 || bi == 112))
                            continue;
                        cksum = ((cksum >> 1) | (cksum << 31)) + (unsigned int)cbuf[bi];
                    }
                }
                unsigned int *cs = (unsigned int *)fbuf;
                { unsigned int ci; for (ci = 0; ci < 128; ci++) cs[ci] = cksum; }
                blk_write_sectors(23, fbuf, 1);
            }

            memset(fbuf, 0, 512);
            *(unsigned int *)(fbuf + 0) = 0xFFFFFFF8; // fat[0] media type
            *(unsigned int *)(fbuf + 4) = 0xFFFFFFFF; // fat[1] reserved
            { unsigned int fc;
              for (fc = 2; fc < 2 + bitmap_clusters - 1; fc++)
                *(unsigned int *)(fbuf + fc * 4) = fc + 1; // chain to next
              *(unsigned int *)(fbuf + fc * 4) = 0xFFFFFFFF; // last bitmap = eoc
            }
            // upcase cluster(s) and root cluster = eoc
            { unsigned int fc;
              for (fc = 2 + bitmap_clusters; fc <= root_cluster; fc++)
                *(unsigned int *)(fbuf + fc * 4) = 0xFFFFFFFF; }
            blk_write_sectors(fat_offset, fbuf, 1);
            // zero only a few more fat sectors (not all 30k+ since that hangs the psp)
            // only the first sector has real entries, zero a handful more for safety
            memset(fbuf, 0, 512);
            { unsigned int fs; for (fs = 1; fs < 8 && fs < fat_length; fs++)
                blk_write_sectors(fat_offset + fs, fbuf, 1); }

            memset(fbuf, 0, 512);
            { unsigned int used = root_cluster - 2 + 1;
              unsigned int bi; for (bi = 0; bi < used; bi++) fbuf[bi/8] |= (1 << (bi%8)); }
            blk_write_sectors(heap_offset, fbuf, 1);
            // zero remaining bitmap sectors
            memset(fbuf, 0, 512);
            { unsigned int bs; for (bs = 1; bs < bitmap_sectors && bs < spc; bs++)
                blk_write_sectors(heap_offset + bs, fbuf, 1); }

            {
                unsigned int uc_cluster = 2 + bitmap_clusters;
                unsigned int uc_sec_base = heap_offset + (uc_cluster - 2) * spc;
                unsigned int uc_byte_count = sizeof(exfat_upcase_table);
                unsigned int uc_checksum = 0xE619D30D;

                // write upcase table sector by sector
                unsigned int uc_written = 0;
                unsigned int uc_sec;
                for (uc_sec = 0; uc_sec < spc; uc_sec++) {
                    memset(fbuf, 0, 512);
                    if (uc_written < uc_byte_count) {
                        unsigned int chunk = uc_byte_count - uc_written;
                        if (chunk > 512) chunk = 512;
                        memcpy(fbuf, exfat_upcase_table + uc_written, chunk);
                        uc_written += chunk;
                    }
                    blk_write_sectors(uc_sec_base + uc_sec, fbuf, 1);
                }

            unsigned int root_sec_base = heap_offset + (root_cluster - 2) * spc;
            memset(fbuf, 0, 512);
            fbuf[0] = 0x83; fbuf[1] = 8; // volume label
            fbuf[2]='M'; fbuf[4]='E'; fbuf[6]='M'; fbuf[8]='S';
            fbuf[10]='T'; fbuf[12]='I'; fbuf[14]='C'; fbuf[16]='K';

            fbuf[32] = 0x81; // allocation bitmap
            *(unsigned int *)(fbuf + 32 + 20) = 2;
            *(unsigned long long *)(fbuf + 32 + 24) = (unsigned long long)bitmap_bytes;

            fbuf[64] = 0x82; // the std up case table
            *(unsigned int *)(fbuf + 64 + 4) = uc_checksum;
            *(unsigned int *)(fbuf + 64 + 20) = uc_cluster;
            *(unsigned long long *)(fbuf + 64 + 24) = (unsigned long long)uc_byte_count;

            blk_write_sectors(root_sec_base, fbuf, 1);
            memset(fbuf, 0, 512);
            { unsigned int rs; for (rs = 1; rs < spc; rs++)
                blk_write_sectors(root_sec_base + rs, fbuf, 1); }
            }
            // zero the remaining root dir sectors
            memset(fbuf, 0, 512);
            { unsigned int rs; for (rs = 1; rs < spc; rs++)
                blk_write_sectors(heap_offset + 2 * spc + rs, fbuf, 1); }

            goto format_reinit;
        }

        // fat32 format (matches sony's psp layout exactly)
        {
            unsigned int reserved = 36; // sony uses 36, not 32 (why???)
            unsigned int num_fats = 2;
            unsigned int clusters = (p_sectors - reserved) / spc;
            unsigned int fat_entries = clusters + 2;
            unsigned int fat_sectors = (fat_entries * 4 + 511) / 512;
            unsigned int data_start = reserved + fat_sectors * num_fats;
            unsigned int data_sectors = p_sectors - data_start;
            clusters = data_sectors / spc;
            unsigned int free_clusters = clusters - 1;

            // vbr at partition sector 0 to match sony's hex dump exactly
            memset(fbuf, 0, 512);
            fbuf[0] = 0xEB; fbuf[1] = 0x58; fbuf[2] = 0x90; // jmp + nop
            memcpy(fbuf + 3, "        ", 8); // oem name: 8 spaces (sony uses spaces on my 1k test device)
            fbuf[11] = 0x00; fbuf[12] = 0x02; // bytes_per_sector = 512
            fbuf[13] = (unsigned char)spc;
            fbuf[14] = (unsigned char)(reserved); fbuf[15] = (unsigned char)(reserved >> 8);
            fbuf[16] = (unsigned char)num_fats;
            fbuf[21] = 0xF8; // media type
            fbuf[24] = 0x3F; fbuf[25] = 0x00;
            fbuf[26] = 0x10; fbuf[27] = 0x00; // num_heads is 16 (sony uses 16)
            // hidden_sectors is the partition start (sony uses 63 for chs-aligned)
            fbuf[28] = (unsigned char)(p_start);
            fbuf[29] = (unsigned char)(p_start >> 8);
            fbuf[30] = (unsigned char)(p_start >> 16);
            fbuf[31] = (unsigned char)(p_start >> 24);
            memcpy(fbuf + 32, &p_sectors, 4);
            memcpy(fbuf + 36, &fat_sectors, 4);
            fbuf[44] = 2;
            fbuf[48] = 1; // fs_info_sector = 1
            fbuf[50] = 6;
            fbuf[66] = 0x29; // boot_sig = 0x29 (sony puts at offset 66, not 70)
            memcpy(fbuf + 71, "MEMSTICK   ", 11); // volume_label
            memcpy(fbuf + 82, "FAT32   ", 8); // fs_type
            fbuf[510] = 0x55; fbuf[511] = 0xAA;
            blk_write_sectors(0, fbuf, 1);

            // fsinfo sector at partition sector 1 - sony format
            memset(fbuf, 0, 512);
            fbuf[0] = 0x52; fbuf[1] = 0x52; fbuf[2] = 0x61; fbuf[3] = 0x41; // "rraa"
            fbuf[484] = 0x72; fbuf[485] = 0x72; fbuf[486] = 0x41; fbuf[487] = 0x61; // "rraa"
            // free_cluster_count at offset 488
            memcpy(fbuf + 488, &free_clusters, 4);
            // and next_free_cluster at offset 492
            { unsigned int nf = 3; memcpy(fbuf + 492, &nf, 4); }
            fbuf[510] = 0x55; fbuf[511] = 0xAA;
            blk_write_sectors(1, fbuf, 1);

            // sector 2: boot trail signature (sony writes 55 aa at end i'm sure they had a good reason for this)
            memset(fbuf, 0, 512);
            fbuf[510] = 0x55; fbuf[511] = 0xAA;
            blk_write_sectors(2, fbuf, 1);

            // backup vbr at sector 6 annd7 and 8
            blk_read_sectors(0, fbuf, 1);
            blk_write_sectors(6, fbuf, 1);
            blk_read_sectors(1, fbuf, 1);
            blk_write_sectors(7, fbuf, 1);
            memset(fbuf, 0, 512);
            fbuf[510] = 0x55; fbuf[511] = 0xAA;
            blk_write_sectors(8, fbuf, 1);

            // standard fat1 first sector
            memset(fbuf, 0, 512);
            fbuf[0] = 0xF8; fbuf[1] = 0xFF; fbuf[2] = 0xFF; fbuf[3] = 0x0F;
            fbuf[4] = 0xFF; fbuf[5] = 0xFF; fbuf[6] = 0xFF; fbuf[7] = 0xFF;
            fbuf[8] = 0xFF; fbuf[9] = 0xFF; fbuf[10] = 0xFF; fbuf[11] = 0x0F;
            blk_write_sectors(reserved, fbuf, 1);

            // sony does not zero all fat sectors as it relies on sd being pre-erased.
            // only write fat1/fat2 first sector (entries 0-2) and root dir entry
            // this matches sony's ~2 second format time (there's no quicker + more efficient way to do this without wrecking something)

            // fat2 first sector - copy from fat1
            blk_read_sectors(reserved, fbuf, 1);
            blk_write_sectors(reserved + fat_sectors, fbuf, 1);

            // write volume label entry in root directory
            memset(fbuf, 0, 512);
            memcpy(fbuf, "MEMSTICK   ", 11); // volume label (8.3 format)
            fbuf[11] = 0x08; // attribute is volume_id
            blk_write_sectors(data_start, fbuf, 1);
        }

    format_reinit:
        // sony's format handler fires remove+insert event flags after writing the filesystem
        // the scefatmsmedia thread wakes up and handles the remount (sceioassign + notify_callbacks)
        // the xmb waits for the callback and it does not self rescan
        // without the callback, xmb shows "Format Failed." (sometimes it would skip to Memory Stick Not Inserted?? how is that possible... probably an anomaly)
        fat_cached_sector = 0xFFFFFFFF;
        if (blk_fd >= 0) {
            k_sceIoClose(blk_fd);
            blk_fd = -1; blk_fd_pos = 0xFFFFFFFFFFFFFFFFULL;
        }
        blk_fd_validated = 0;
        fs_initialized = 0;
        // signal media thread: remove then insert (sony fires both)
        if (event_flag_id >= 0) {
            sceKernelSetEventFlag(event_flag_id, EVT_REMOVE);
            sceKernelSetEventFlag(event_flag_id, EVT_INSERT);
        }
        return 0;
    }

    // this is the flush cmd, always return success
    case 0x00005001:
        return 0;

    // set the char
    case 0x02425856:
        if (!indata || inlen != 4) return 0x80010016;
        {
            int val;
            memcpy(&val, indata, sizeof(val));
            int r = sceCodepage_driver_B0AE63AA(val);
            return r < 0 ? r : 0;
        }

    // now elts get the char
    case 0x02425858:
        if (outdata && outlen >= 4)
            *(int *)outdata = 1;
        return 0;

    // check if formatted
    case 0x02415857:
        if (outdata && outlen >= 4)
            *(int *)outdata = 1; // 1 means formatted
        return 0;

    // get the max sector count
    case 0x02415864:
        if (outdata && outlen >= 4)
            *(int *)outdata = fat32_sectors_per_cluster;
        return 0;

    // get the serial number (when sony uses this it's usually a dummy)
    case 0x02415862:
        if (outdata && outlen >= 4)
            *(unsigned int *)outdata = 0x12345678; // feed dummy serial for now
        return 0;

    // get the fs typee
    case 0x0242585A:
        if (outdata && outlen >= 4)
            *(int *)outdata = 1; // always say fat as sony has no convept of exFAT
        return 0;

    // partition info
    case 0x02415859:
        if (outdata && outlen >= 4)
            *(int *)outdata = 1;
        return 0;

    // format checking, just return what sony expects here
    case 0x0241D814:
        if (outdata && outlen >= 4)
            *(int *)outdata = 0;
        return 0;

    // alt get freespace avail (actually just the same as 0x02425818)
    case 0x0241D819: 
        if (outdata && outlen >= 20) {
            unsigned int *ds = (unsigned int *)outdata;
            if (g_use_exfat) {
                ds[0] = exfat_cluster_count;
                ds[1] = exfat_free_clusters;
                ds[2] = exfat_cluster_count * exfat_sectors_per_cluster;
                ds[3] = exfat_bytes_per_sector;
                ds[4] = exfat_sectors_per_cluster;
            } else {
                ds[0] = fat32_total_clusters;
                ds[1] = fat32_free_clusters;
                ds[2] = fat32_total_clusters * fat32_sectors_per_cluster;
                ds[3] = fat32_bytes_per_sector;
                ds[4] = fat32_sectors_per_cluster;
            }
        }
        return 0;

    case 0x0240D81B: {
        // sony's ioremove, the kernel opens the file first, then sends this devctl
        int fd = (int)arg->arg;
        if (fd < 0 || fd >= MAX_OPEN_FILES || !open_files[fd].in_use)
            return 0x80010016;
        OpenFile *f = &open_files[fd];
        if (g_use_exfat) {
            // mark exfat dir entry set as deleted (clear bit 7)
            int dr = exfat_delete_dir_entry(f->parent_cluster, f->first_cluster, f->parent_no_fat_chain);
            if (dr != 0) return dr;
            if (f->no_fat_chain && f->file_size > 0) {
                unsigned int nc = (f->file_size + (exfat_cluster_size - 1)) / exfat_cluster_size;
                unsigned int i;
                for (i = 0; i < nc; i++)
                    exfat_free_cluster(f->first_cluster + i);
            } else if (f->first_cluster >= 2) {
                exfat_free_chain(f->first_cluster);
            }
        } else {
            // fat32: mark sfn entry as 0xe5, free chain
            if (f->sfn_sector != 0) {
                unsigned char sbuf[512] __attribute__((aligned(64)));
                if (blk_read_sectors(f->sfn_sector, sbuf, 1) >= 0) {
                    sbuf[f->sfn_index] = 0xE5;
                    // mark preceding lfn entries as deleted within same sector
                    if (f->sfn_index >= 32) {
                        int le = (int)f->sfn_index - 32;
                        while (le >= 0) {
                            FatDirEntry *ld = (FatDirEntry *)(sbuf + le);
                            if (ld->attr != FAT_ATTR_LFN) break;
                            ld->name[0] = 0xE5;
                            le -= 32;
                        }
                    }
                    blk_write_sectors(f->sfn_sector, sbuf, 1);
                    blk_wr_cache_push(f->sfn_sector, sbuf);
                }
                // free cluster chain as usual
                if (f->first_cluster >= 2) {
                    unsigned int fc = f->first_cluster;
                    while (fc >= 2 && fc < FAT32_EOC) {
                        unsigned int next = fat32_next_cluster(fc);
                        fat32_write_fat(fc, 0);
                        fc = next;
                    }
                    fat2_flush();
                }
            }
        }
        return 0;
    }

    // set and get attrib / rename through devctl
    case 0x0242D81A: 
    case 0x0242D81C:
    case 0x02415830:
        return 0;
    // get partition table and query format (do as sony does!)
    case 0x02415870:
    case 0x0241D81D:
        return 0;

    case ES_DEVCTL_READ_SECTOR:  return es_devctl_read_sector(indata, inlen);
    case ES_DEVCTL_WRITE_SECTOR: return es_devctl_write_sector(indata, inlen);
    case ES_DEVCTL_GET_INFO:     return es_devctl_get_info(outdata, outlen);
    case ES_DEVCTL_RESCAN:       return es_devctl_rescan();
    case ES_DEVCTL_SYNC_FD:      es_sync_blk_fd(); return 0;

    case ES_DEVCTL_GET_VERSION:
        if (outdata && outlen >= 4) {
            *(unsigned int *)outdata = DRATINIFS_VERSION;
            return 0;
        }
        return -1;

    default:
         // sony returns 0 for unknown devctls; errors cause xmb crashes, deadlocks, race condition fails .. just a mess
         // just return 0 for now until we can document everything in more detail.... i'm still missing some things..
        return 0;
    }
}

static int exfat_IoUnk21(PspIoDrvFileArg *arg)
{
    (void)arg;
    return 0;
}

// vfs wrappers (copying sony's 661 fatms pattern)
// sony checks scekernelcheckthreadkernelstack() < 0x1000 before extending.
// uses 0x1000 (4kb) extension

struct _io_open_args { PspIoDrvFileArg *arg; char *file; int flags; SceMode mode; int ret; };
static void _wrap_IoOpen(void *p) { struct _io_open_args *a = p; a->ret = exfat_IoOpen(a->arg, a->file, a->flags, a->mode); }
static int wrap_IoOpen(PspIoDrvFileArg *arg, char *file, int flags, SceMode mode) {
    if (fs_mutex >= 0) sceKernelLockMutex(fs_mutex, 1, 0);
    int ret;
    if (sceKernelCheckThreadKernelStack() < 0x1000) {
        struct _io_open_args a = {arg, file, flags, mode, -1};
        sceKernelExtendKernelStack(0x1000, _wrap_IoOpen, &a);
        ret = a.ret;
    } else {
        ret = exfat_IoOpen(arg, file, flags, mode);
    }
    if (fs_mutex >= 0) sceKernelUnlockMutex(fs_mutex, 1);
    return ret;
}

struct _io_read_args { PspIoDrvFileArg *arg; char *data; int len; int ret; };
static void _wrap_IoRead(void *p) { struct _io_read_args *a = p; a->ret = exfat_IoRead(a->arg, a->data, a->len); }
static int wrap_IoRead(PspIoDrvFileArg *arg, char *data, int len) {
    if (fs_mutex >= 0) sceKernelLockMutex(fs_mutex, 1, 0);
    int ret;
    if (sceKernelCheckThreadKernelStack() < 0x1000) {
        struct _io_read_args a = {arg, data, len, -1};
        sceKernelExtendKernelStack(0x1000, _wrap_IoRead, &a);
        ret = a.ret;
    } else {
        ret = exfat_IoRead(arg, data, len);
    }
    if (fs_mutex >= 0) sceKernelUnlockMutex(fs_mutex, 1);
    return ret;
}

struct _io_dopen_args { PspIoDrvFileArg *arg; const char *dirname; int ret; };
static void _wrap_IoDopen(void *p) { struct _io_dopen_args *a = p; a->ret = exfat_IoDopen(a->arg, a->dirname); }
static int wrap_IoDopen(PspIoDrvFileArg *arg, const char *dirname) {
    if (fs_mutex >= 0) sceKernelLockMutex(fs_mutex, 1, 0);
    dir_rd_cache_invalidate();
    int ret;
    if (sceKernelCheckThreadKernelStack() < 0x1000) {
        struct _io_dopen_args a = {arg, dirname, -1};
        sceKernelExtendKernelStack(0x1000, _wrap_IoDopen, &a);
        ret = a.ret;
    } else {
        ret = exfat_IoDopen(arg, dirname);
    }

    // try to support GCLite's behavior here as well
    if (ret == (int)0x80010002 && es_initialized) {
        const char *dn = dirname;
        while (*dn == '/') dn++;
        if (*dn && es_check_dir_prefix(dn)) {
            int fd = alloc_fd();
            if (fd < 0) {
                ret = 0x80000020;
            } else {
                memset(&open_files[fd], 0, sizeof(OpenFile));
                open_files[fd].msstor_fd = -1;
                open_files[fd].in_use = 1;
                open_files[fd].is_dir = 1;
                open_files[fd].first_cluster = 0;  // tjis should signal the virtual directory
                open_files[fd].dir_cluster = 0;
                es_save_dirname(fd, dirname);
                arg->arg = (void *)fd;
                ret = 0;
            }
        }
    }

    if (fs_mutex >= 0) sceKernelUnlockMutex(fs_mutex, 1);
    return ret;
}

struct _io_dread_args { PspIoDrvFileArg *arg; SceIoDirent *dir; int ret; };
static void _wrap_IoDread(void *p) { struct _io_dread_args *a = p; a->ret = exfat_IoDread(a->arg, a->dir); }
static int wrap_IoDread(PspIoDrvFileArg *arg, SceIoDirent *dir) {
    if (fs_mutex >= 0) sceKernelLockMutex(fs_mutex, 1, 0);
    int ret;
    // once the overlay has taken over, skip the exfat_IoDread entirely. otherwise every damn extra dread call after fat exhaust would
    // re-read the directory's end-marker sector from the stick
    int fd_probe = (int)arg->arg;
    int overlay_active = (es_initialized && fd_probe >= 0 && fd_probe < MAX_OPEN_FILES
                          && open_files[fd_probe].sfn_sector != 0);
    if (overlay_active) {
        ret = 0;
    } else if (sceKernelCheckThreadKernelStack() < 0x1000) {
        struct _io_dread_args a = {arg, dir, -1};
        sceKernelExtendKernelStack(0x1000, _wrap_IoDread, &a);
        ret = a.ret;
    } else {
        ret = exfat_IoDread(arg, dir);
    }
    if (ret == 0 && es_initialized) {
        int fd = (int)arg->arg;
        if (fd >= 0 && fd < MAX_OPEN_FILES) {
            int iter = (int)open_files[fd].sfn_sector;
            ret = es_dread_overlay(fd, dir, &iter);
            open_files[fd].sfn_sector = (unsigned int)iter;
        }
    }
    if (fs_mutex >= 0) sceKernelUnlockMutex(fs_mutex, 1);
    return ret;
}

struct _io_getstat_args { PspIoDrvFileArg *arg; const char *file; SceIoStat *stat; int ret; };
static void _wrap_IoGetstat(void *p) { struct _io_getstat_args *a = p; a->ret = exfat_IoGetstat(a->arg, a->file, a->stat); }
static int wrap_IoGetstat(PspIoDrvFileArg *arg, const char *file, SceIoStat *stat) {
    if (fs_mutex >= 0) sceKernelLockMutex(fs_mutex, 1, 0);
    int ret;
    if (sceKernelCheckThreadKernelStack() < 0x1000) {
        struct _io_getstat_args a = {arg, file, stat, -1};
        sceKernelExtendKernelStack(0x1000, _wrap_IoGetstat, &a);
        ret = a.ret;
    } else {
        ret = exfat_IoGetstat(arg, file, stat);
    }
    if (fs_mutex >= 0) sceKernelUnlockMutex(fs_mutex, 1);
    return ret;
}

struct _io_write_args { PspIoDrvFileArg *arg; const char *data; int len; int ret; };
static void _wrap_IoWrite(void *p) { struct _io_write_args *a = p; a->ret = exfat_IoWrite(a->arg, a->data, a->len); }
static int wrap_IoWrite(PspIoDrvFileArg *arg, const char *data, int len) {
    if (fs_mutex >= 0) sceKernelLockMutex(fs_mutex, 1, 0);
    int ret;
    if (sceKernelCheckThreadKernelStack() < 0x1000) {
        struct _io_write_args a = {arg, data, len, -1};
        sceKernelExtendKernelStack(0x1000, _wrap_IoWrite, &a);
        ret = a.ret;
    } else {
        ret = exfat_IoWrite(arg, data, len);
    }
    if (fs_mutex >= 0) sceKernelUnlockMutex(fs_mutex, 1);
    return ret;
}

struct _io_close_args { PspIoDrvFileArg *arg; int ret; };
static void _wrap_IoClose(void *p) { struct _io_close_args *a = p; a->ret = exfat_IoClose(a->arg); }
static int wrap_IoClose(PspIoDrvFileArg *arg) {
    if (fs_mutex >= 0) sceKernelLockMutex(fs_mutex, 1, 0);
    int ret;
    if (sceKernelCheckThreadKernelStack() < 0x1000) {
        struct _io_close_args a = {arg, -1};
        sceKernelExtendKernelStack(0x1000, _wrap_IoClose, &a);
        ret = a.ret;
    } else {
        ret = exfat_IoClose(arg);
    }
    if (fs_mutex >= 0) sceKernelUnlockMutex(fs_mutex, 1);
    return ret;
}

struct _io_lseek_args { PspIoDrvFileArg *arg; SceOff ofs; int whence; SceOff ret; };
static void _wrap_IoLseek(void *p) { struct _io_lseek_args *a = p; a->ret = exfat_IoLseek(a->arg, a->ofs, a->whence); }
static SceOff wrap_IoLseek(PspIoDrvFileArg *arg, SceOff ofs, int whence) {
    if (fs_mutex >= 0) sceKernelLockMutex(fs_mutex, 1, 0);
    SceOff ret;
    if (sceKernelCheckThreadKernelStack() < 0x1000) {
        struct _io_lseek_args a = {arg, ofs, whence, -1};
        sceKernelExtendKernelStack(0x1000, _wrap_IoLseek, &a);
        ret = a.ret;
    } else {
        ret = exfat_IoLseek(arg, ofs, whence);
    }
    if (fs_mutex >= 0) sceKernelUnlockMutex(fs_mutex, 1);
    return ret;
}

struct _io_ioctl_args { PspIoDrvFileArg *arg; unsigned int cmd; void *indata; int inlen; void *outdata; int outlen; int ret; };
static void _wrap_IoIoctl(void *p) { struct _io_ioctl_args *a = p; a->ret = exfat_IoIoctl(a->arg, a->cmd, a->indata, a->inlen, a->outdata, a->outlen); }
static int wrap_IoIoctl(PspIoDrvFileArg *arg, unsigned int cmd, void *indata, int inlen, void *outdata, int outlen) {
    if (fs_mutex >= 0) sceKernelLockMutex(fs_mutex, 1, 0);
    int ret;
    if (sceKernelCheckThreadKernelStack() < 0x1000) {
        struct _io_ioctl_args a = {arg, cmd, indata, inlen, outdata, outlen, -1};
        sceKernelExtendKernelStack(0x1000, _wrap_IoIoctl, &a);
        ret = a.ret;
    } else {
        ret = exfat_IoIoctl(arg, cmd, indata, inlen, outdata, outlen);
    }
    if (fs_mutex >= 0) sceKernelUnlockMutex(fs_mutex, 1);
    return ret;
}

struct _io_remove_args { PspIoDrvFileArg *arg; const char *name; int ret; };
static void _wrap_IoRemove(void *p) { struct _io_remove_args *a = p; a->ret = exfat_IoRemove(a->arg, a->name); }
static int wrap_IoRemove(PspIoDrvFileArg *arg, const char *name) {
    if (fs_mutex >= 0) sceKernelLockMutex(fs_mutex, 1, 0);
    int ret;
    if (sceKernelCheckThreadKernelStack() < 0x1000) {
        struct _io_remove_args a = {arg, name, -1};
        sceKernelExtendKernelStack(0x1000, _wrap_IoRemove, &a);
        ret = a.ret;
    } else {
        ret = exfat_IoRemove(arg, name);
    }
    if (fs_mutex >= 0) sceKernelUnlockMutex(fs_mutex, 1);
    return ret;
}

struct _io_mkdir_args { PspIoDrvFileArg *arg; const char *name; SceMode mode; int ret; };
static void _wrap_IoMkdir(void *p) { struct _io_mkdir_args *a = p; a->ret = exfat_IoMkdir(a->arg, a->name, a->mode); }
static int wrap_IoMkdir(PspIoDrvFileArg *arg, const char *name, SceMode mode) {
    if (fs_mutex >= 0) sceKernelLockMutex(fs_mutex, 1, 0);
    int ret;
    if (sceKernelCheckThreadKernelStack() < 0x1000) {
        struct _io_mkdir_args a = {arg, name, mode, -1};
        sceKernelExtendKernelStack(0x1000, _wrap_IoMkdir, &a);
        ret = a.ret;
    } else {
        ret = exfat_IoMkdir(arg, name, mode);
    }
    if (fs_mutex >= 0) sceKernelUnlockMutex(fs_mutex, 1);
    return ret;
}

struct _io_rmdir_args { PspIoDrvFileArg *arg; const char *name; int ret; };
static void _wrap_IoRmdir(void *p) { struct _io_rmdir_args *a = p; a->ret = exfat_IoRmdir(a->arg, a->name); }
static int wrap_IoRmdir(PspIoDrvFileArg *arg, const char *name) {
    if (fs_mutex >= 0) sceKernelLockMutex(fs_mutex, 1, 0);
    int ret;
    if (sceKernelCheckThreadKernelStack() < 0x1000) {
        struct _io_rmdir_args a = {arg, name, -1};
        sceKernelExtendKernelStack(0x1000, _wrap_IoRmdir, &a);
        ret = a.ret;
    } else {
        ret = exfat_IoRmdir(arg, name);
    }
    if (fs_mutex >= 0) sceKernelUnlockMutex(fs_mutex, 1);
    return ret;
}

struct _io_dclose_args { PspIoDrvFileArg *arg; int ret; };
static void _wrap_IoDclose(void *p) { struct _io_dclose_args *a = p; a->ret = exfat_IoDclose(a->arg); }
static int wrap_IoDclose(PspIoDrvFileArg *arg) {
    if (fs_mutex >= 0) sceKernelLockMutex(fs_mutex, 1, 0);
    int ret;
    if (sceKernelCheckThreadKernelStack() < 0x1000) {
        struct _io_dclose_args a = {arg, -1};
        sceKernelExtendKernelStack(0x1000, _wrap_IoDclose, &a);
        ret = a.ret;
    } else {
        ret = exfat_IoDclose(arg);
    }
    if (fs_mutex >= 0) sceKernelUnlockMutex(fs_mutex, 1);
    return ret;
}

struct _io_chstat_args { PspIoDrvFileArg *arg; const char *file; SceIoStat *stat; int bits; int ret; };
static void _wrap_IoChstat(void *p) { struct _io_chstat_args *a = p; a->ret = exfat_IoChstat(a->arg, a->file, a->stat, a->bits); }
static int wrap_IoChstat(PspIoDrvFileArg *arg, const char *file, SceIoStat *stat, int bits) {
    if (fs_mutex >= 0) sceKernelLockMutex(fs_mutex, 1, 0);
    int ret;
    if (sceKernelCheckThreadKernelStack() < 0x1000) {
        struct _io_chstat_args a = {arg, file, stat, bits, -1};
        sceKernelExtendKernelStack(0x1000, _wrap_IoChstat, &a);
        ret = a.ret;
    } else {
        ret = exfat_IoChstat(arg, file, stat, bits);
    }
    if (fs_mutex >= 0) sceKernelUnlockMutex(fs_mutex, 1);
    return ret;
}

struct _io_rename_args { PspIoDrvFileArg *arg; const char *oldname; const char *newname; int ret; };
static void _wrap_IoRename(void *p) { struct _io_rename_args *a = p; a->ret = exfat_IoRename(a->arg, a->oldname, a->newname); }
static int wrap_IoRename(PspIoDrvFileArg *arg, const char *oldname, const char *newname) {
    if (fs_mutex >= 0) sceKernelLockMutex(fs_mutex, 1, 0);
    int ret;
    if (sceKernelCheckThreadKernelStack() < 0x1000) {
        struct _io_rename_args a = {arg, oldname, newname, -1};
        sceKernelExtendKernelStack(0x1000, _wrap_IoRename, &a);
        ret = a.ret;
    } else {
        ret = exfat_IoRename(arg, oldname, newname);
    }
    if (fs_mutex >= 0) sceKernelUnlockMutex(fs_mutex, 1);
    return ret;
}

struct _io_chdir_args { PspIoDrvFileArg *arg; const char *dir; int ret; };
static void _wrap_IoChdir(void *p) { struct _io_chdir_args *a = p; a->ret = exfat_IoChdir(a->arg, a->dir); }
static int wrap_IoChdir(PspIoDrvFileArg *arg, const char *dir) {
    if (fs_mutex >= 0) sceKernelLockMutex(fs_mutex, 1, 0);
    int ret;
    if (sceKernelCheckThreadKernelStack() < 0x1000) {
        struct _io_chdir_args a = {arg, dir, -1};
        sceKernelExtendKernelStack(0x1000, _wrap_IoChdir, &a);
        ret = a.ret;
    } else {
        ret = exfat_IoChdir(arg, dir);
    }
    if (fs_mutex >= 0) sceKernelUnlockMutex(fs_mutex, 1);
    return ret;
}

// driver structure (let's try to keep it safge and copy sony's pattern but we still need to deviate while maintaining efficiency)

struct _io_devctl_args { PspIoDrvFileArg *arg; const char *devname; unsigned int cmd; void *indata; int inlen; void *outdata; int outlen; int ret; };
static void _wrap_IoDevctl(void *p) { struct _io_devctl_args *a = p; a->ret = exfat_IoDevctl(a->arg, a->devname, a->cmd, a->indata, a->inlen, a->outdata, a->outlen); }
static int wrap_IoDevctl(PspIoDrvFileArg *arg, const char *devname, unsigned int cmd, void *indata, int inlen, void *outdata, int outlen) {
    if (fs_mutex >= 0) sceKernelLockMutex(fs_mutex, 1, 0);
    int ret;
    if (sceKernelCheckThreadKernelStack() < 0x1000) {
        struct _io_devctl_args a = {arg, devname, cmd, indata, inlen, outdata, outlen, -1};
        sceKernelExtendKernelStack(0x1000, _wrap_IoDevctl, &a);
        ret = a.ret;
    } else {
        ret = exfat_IoDevctl(arg, devname, cmd, indata, inlen, outdata, outlen);
    }
    if (fs_mutex >= 0) sceKernelUnlockMutex(fs_mutex, 1);
    return ret;
}

struct _io_mount_args { PspIoDrvFileArg *arg; const char *fs_name; const char *blockdev; int mode; void *unk1; int unk2; int ret; };
static void _wrap_IoMount(void *p) { struct _io_mount_args *a = p; a->ret = exfat_IoMount(a->arg, a->fs_name, a->blockdev, a->mode, a->unk1, a->unk2); }
static int wrap_IoMount(PspIoDrvFileArg *arg, const char *fs_name, const char *blockdev, int mode, void *unk1, int unk2) {
    if (fs_mutex >= 0) sceKernelLockMutex(fs_mutex, 1, 0);
    int ret;
    if (sceKernelCheckThreadKernelStack() < 0x1000) {
        struct _io_mount_args a = {arg, fs_name, blockdev, mode, unk1, unk2, -1};
        sceKernelExtendKernelStack(0x1000, _wrap_IoMount, &a);
        ret = a.ret;
    } else {
        ret = exfat_IoMount(arg, fs_name, blockdev, mode, unk1, unk2);
    }
    if (fs_mutex >= 0) sceKernelUnlockMutex(fs_mutex, 1);
    return ret;
}

struct _io_umount_args { PspIoDrvFileArg *arg; const char *devname; int ret; };
static void _wrap_IoUmount(void *p) { struct _io_umount_args *a = p; a->ret = exfat_IoUmount(a->arg, a->devname); }
static int wrap_IoUmount(PspIoDrvFileArg *arg, const char *devname) {
    if (fs_mutex >= 0) sceKernelLockMutex(fs_mutex, 1, 0);
    int ret;
    if (sceKernelCheckThreadKernelStack() < 0x1000) {
        struct _io_umount_args a = {arg, devname, -1};
        sceKernelExtendKernelStack(0x1000, _wrap_IoUmount, &a);
        ret = a.ret;
    } else {
        ret = exfat_IoUmount(arg, devname);
    }
    if (fs_mutex >= 0) sceKernelUnlockMutex(fs_mutex, 1);
    return ret;
}

static PspIoDrvFuncs exfat_funcs = {
    exfat_IoInit,
    exfat_IoExit,
    wrap_IoOpen,
    wrap_IoClose,
    wrap_IoRead,
    wrap_IoWrite,
    wrap_IoLseek,
    wrap_IoIoctl,
    wrap_IoRemove,
    wrap_IoMkdir,
    wrap_IoRmdir,
    wrap_IoDopen,
    wrap_IoDclose,
    wrap_IoDread,
    wrap_IoGetstat,
    wrap_IoChstat,
    wrap_IoRename,
    wrap_IoChdir,
    (void *)wrap_IoMount,    // the real abi has 6 params, pspsdk declares 1 so we need to cast it
    (void *)wrap_IoUmount,   // ditto for umount but with 2 params
    wrap_IoDevctl,
    exfat_IoUnk21
};

static PspIoDrv exfat_driver = {
    "fatms",
    0x000A0010,
    0x800,
    "MS",
    &exfat_funcs
};

// fat32 initski

static int __attribute__((noinline)) fat32_init(void)
{
    unsigned char bpb_buf[512] __attribute__((aligned(64)));
    fat32_bytes_per_sector = 512;

    int ret = blk_read_sectors(0, bpb_buf, 1);
    if (ret < 0)
        return ret;

    Fat32BPB *bpb = (Fat32BPB *)bpb_buf;

    if (bpb->jmp[0] != 0xEB && bpb->jmp[0] != 0xE9) {
        return -1;
    }

    fat32_bytes_per_sector = bpb->bytes_per_sector;
    fat32_sectors_per_cluster = bpb->sectors_per_cluster;
    if (fat32_sectors_per_cluster == 0) {
        return -3;
    }
    fat32_cluster_size = fat32_bytes_per_sector * fat32_sectors_per_cluster;
    fat32_fat_start_sector = bpb->reserved_sectors;
    fat32_fat_size = bpb->fat_size_32;
    fat32_root_cluster = bpb->root_cluster;

    fat32_data_start_sector = bpb->reserved_sectors + (bpb->num_fats * bpb->fat_size_32);
    fat32_total_sectors = bpb->total_sectors_32 ? bpb->total_sectors_32 : bpb->total_sectors_16;
    fat32_total_clusters = (fat32_total_sectors - fat32_data_start_sector) / fat32_sectors_per_cluster;

    fat_cached_sector = 0xFFFFFFFF;

    // read fsinfo for free cluster count (sony's is instant, no fat scan)
    fat32_free_clusters = 0;
    if (bpb->fs_info >= 1 && bpb->fs_info < bpb->reserved_sectors) {
        unsigned char fsi_buf[512] __attribute__((aligned(64)));
        if (blk_read_sectors(bpb->fs_info, fsi_buf, 1) >= 0) {
            unsigned int sig1, sig2, free_count;
            memcpy(&sig1, fsi_buf + 0, 4);
            memcpy(&sig2, fsi_buf + 484, 4);
            memcpy(&free_count, fsi_buf + 488, 4);
            if (sig1 == 0x41615252 && sig2 == 0x61417272
                && free_count != 0xFFFFFFFF) {
                // accept fsinfo even if stale (sony does this but I don't want to do this in the future...)
                // clamp to total_clusters to avoid reporting more free than exists
                if (free_count > fat32_total_clusters)
                    free_count = fat32_total_clusters;
                fat32_free_clusters = free_count;
            }
        }
    }

    return 0;
}

// exFAT vbr parser 
// fills the same fat32_* globals and exfat fat entries are an identical 32-bit format so all cluster walk logic works unchanged!...mostly lol
// exfat vbr offsets are [80] fatoffset, [88] clusterheapoffset, [96] rootcluster, [108] bytespersectorshift, [109] sectorsperclustershift
// exfat_init: reads vbr and populates exfat_* globals only
// I don't touch any fat32_* or partition_start_sector globals
static int __attribute__((used)) exfat_init(void)
{
    unsigned char vbr[512] __attribute__((aligned(64)));
    exfat_bytes_per_sector = 512; // needed!
    int ret = exfat_blk_read_sectors(0, vbr, 1);
    if (ret < 0) return ret;
    if (memcmp(vbr + 3, "EXFAT   ", 8) != 0) return -1;
    unsigned char bps_shift = vbr[108];
    unsigned char spc_shift = vbr[109];
    if (bps_shift < 9 || bps_shift > 12 || spc_shift > 25) return -2;
    exfat_bytes_per_sector    = 1u << bps_shift;
    exfat_sectors_per_cluster = 1u << spc_shift;
    exfat_cluster_size        = exfat_bytes_per_sector * exfat_sectors_per_cluster;
    exfat_fat_start_sector    = *(unsigned int *)(vbr + 80);
    exfat_fat_size            = *(unsigned int *)(vbr + 84);
    exfat_data_start_sector   = *(unsigned int *)(vbr + 88);
    exfat_root_cluster        = *(unsigned int *)(vbr + 96);
    exfat_cluster_count       = *(unsigned int *)(vbr + 92);
    exfat_volume_sectors      = *(unsigned int *)(vbr + 72);
    exfat_fat_cached_sector = 0xFFFFFFFF;

    // scan root directory for allocation bitmap entry
    {
        unsigned int cl = exfat_root_cluster;
        unsigned char rbuf[512] __attribute__((aligned(64)));
        int found_bitmap = 0;
        while (cl >= 2 && cl < 0x0FFFFFF8 && !found_bitmap) {
            unsigned int sb = exfat_data_start_sector + (cl - 2) * exfat_sectors_per_cluster;
            unsigned int si;
            for (si = 0; si < exfat_sectors_per_cluster && !found_bitmap; si++) {
                if (exfat_blk_read_sectors(sb + si, rbuf, 1) < 0) break;
                unsigned int eo;
                for (eo = 0; eo < 512; eo += 32) {
                    if (rbuf[eo] == 0x00) { found_bitmap = -1; break; }
                    if (rbuf[eo] == EXFAT_ENTRY_BITMAP) {
                        // bitmap entry is byte 20-23 = firstcluster and byte 24-27 = datalength (low)
                        unsigned int bc, bs;
                        memcpy(&bc, rbuf + eo + 20, 4);
                        memcpy(&bs, rbuf + eo + 24, 4);
                        exfat_bitmap_cluster = bc;
                        exfat_bitmap_size = bs;
                        found_bitmap = 1;
                        break;
                    }
                }
            }
            // follow root dir chain (fat entry for root cluster)
            if (!found_bitmap && found_bitmap != -1) {
                unsigned int fat_off = cl * 4;
                unsigned int fs = exfat_fat_start_sector + (fat_off / exfat_bytes_per_sector);
                unsigned int fe = fat_off % exfat_bytes_per_sector;
                unsigned char fb[512] __attribute__((aligned(64)));
                if (exfat_blk_read_sectors(fs, fb, 1) < 0) break;
                memcpy(&cl, fb + fe, 4);
            } else break;
        }
    }

    return 0;
}

static SceUID assign_thid = -1;

// MBR partition table entry shoukld be 16 bytes
typedef struct {
    unsigned char status;
    unsigned char chs_first[3];
    unsigned char type;
    unsigned char chs_last[3];
    unsigned int lba_start;
    unsigned int num_sectors;
} __attribute__((packed)) MBRPartEntry;

// parse mbr to find the first fat32 partition's start lba
// returns the lba start sector, or 0 if not found
static unsigned int mbr_find_fat32_partition(const unsigned char *mbr_buf)
{
    if (mbr_buf[510] != 0x55 || mbr_buf[511] != 0xAA)
        return 0;
    const MBRPartEntry *entries = (const MBRPartEntry *)(mbr_buf + 0x1BE);
    int i;
    for (i = 0; i < 4; i++) {
        if ((entries[i].type == 0x0B || entries[i].type == 0x0C) && entries[i].lba_start > 0)
            return entries[i].lba_start;
    }
    // let'sd fallback to first non-empty partition (but not exfat 0x07 or extremespeed 0xda)
    for (i = 0; i < 4; i++) {
        if (entries[i].type != 0x00 && entries[i].type != 0x07
            && entries[i].type != 0xDA && entries[i].lba_start > 0)
            return entries[i].lba_start;
    }
    return 0;
}

// parse the MBR to find the exFAT partition (type 0x07) start lba (07 technically covers NTFS too but... well.. yeah)
// returns the lba start sector, or 0 if not found.
static unsigned int mbr_find_exfat_partition(const unsigned char *mbr_buf, unsigned int *out_num_sectors)
{
    if (mbr_buf[510] != 0x55 || mbr_buf[511] != 0xAA)
        return 0;
    const MBRPartEntry *entries = (const MBRPartEntry *)(mbr_buf + 0x1BE);
    int i;
    for (i = 0; i < 4; i++) {
        if (entries[i].type == 0x07 && entries[i].lba_start > 0) {
            if (out_num_sectors) *out_num_sectors = entries[i].num_sectors;
            return entries[i].lba_start;
        }
    }
    return 0;
}

// deferred init is called from iodevctl/ioopen when msstor is probably ready
// retries if msstor0 not yet available, can be called multiple times safely
// THIS is probably the cause of the race condition issues plaguing this driver relating to DATAINSTALL games...

static void deferred_fs_init(void)
{
    if (fs_initialized == 2) return; // fully done
    if (fs_initialized == 0) {
        fs_initialized = 1; // and in progress
    }

    // open raw whole-disk device (sony does not do this, and we have to, because of exfat/ES support)
    // always use "msstor0:", we manage partition offsets ourselves because sony's pattern is broken for the scope of this
    // "msstor0p1:" auto-offsets to partition start, but media thread reopens as "msstor0:" causing an offset mismatch
    if (blk_fd < 0) {
        int blk_retries;
        for (blk_retries = 0; blk_retries < 30; blk_retries++) {
            blk_fd = k_sceIoOpen("msstor0:", 0x04000003, 0);
            if (blk_fd >= 0) break;
            sceKernelDelayThread(50000); // 50ms.. lets wait for msstor after reboot (this is consistent but.... it's not right...)
        }
        if (blk_fd < 0) {
            return;
        }
        // sony calls card init ioctl after opening block device so lets do eet
        k_sceIoIoctl(blk_fd, 0x02125803, NULL, 0, NULL, 0);
    }

    // patch the msstor capacity to the real disk size (from Leftovers breakthrough!)
    {
        static const char *ms_names[] = {
            "sceMSstor_Driver", "sceMSstor", "sceMSStorDriver",
            "sceMSStor_Driver", "sceMSstorDriver", NULL
        };
        SceModule *ms_mod = NULL;
        int ni;
        for (ni = 0; ms_names[ni]; ni++) {
            ms_mod = sceKernelFindModuleByName(ms_names[ni]);
            if (ms_mod) break;
        }
        if (ms_mod) {
            unsigned int text_addr = (unsigned int)ms_mod->text_addr;
            if (text_addr == 0) text_addr = (unsigned int)ms_mod->segmentaddr[0];
            unsigned int text_size = ms_mod->segmentsize[0];
            unsigned int seg1_addr = (ms_mod->nsegment >= 2 && ms_mod->segmentaddr[1])
                ? (unsigned int)ms_mod->segmentaddr[1]
                : text_addr + text_size;

            // this is the dev stuct i foudn
            #define MSSTOR_SEG1_VA  0xF300
            #define MSSTOR_DEV_PTR  0x14394
            unsigned int dev_ptr_addr = (MSSTOR_DEV_PTR >= MSSTOR_SEG1_VA)
                ? seg1_addr + (MSSTOR_DEV_PTR - MSSTOR_SEG1_VA)
                : text_addr + MSSTOR_DEV_PTR;

            unsigned int dev_struct = *(volatile unsigned int *)dev_ptr_addr;
            if (dev_struct >= 0x88000000 && dev_struct <= 0x8BFFFFFF) {
                // get the highest end cap
                unsigned char mbr_cap[512] __attribute__((aligned(64)));
                k_sceIoLseek64k(blk_fd, 0, 0, 0, PSP_SEEK_SET);
                if (k_sceIoRead(blk_fd, mbr_cap, 512) == 512
                    && mbr_cap[510] == 0x55 && mbr_cap[511] == 0xAA) {
                    unsigned int max_end = 0;
                    int pi;
                    for (pi = 0; pi < 4; pi++) {
                        unsigned char *e = mbr_cap + 0x1BE + pi * 16;
                        if (e[4] == 0) continue;
                        unsigned int lba = e[8]|(e[9]<<8)|(e[10]<<16)|(e[11]<<24);
                        unsigned int ns  = e[12]|(e[13]<<8)|(e[14]<<16)|(e[15]<<24);
                        unsigned int end = lba + ns;
                        if (end > max_end) max_end = end;
                    }
                    if (max_end > 0)
                        ((volatile unsigned int *)dev_struct)[5016] = max_end;
                }
                blk_fd_pos = 0xFFFFFFFFFFFFFFFFULL;
            }
        }
    }

    // let's find the partition!
    fat32_bytes_per_sector = 512;
    partition_start_sector = 0;
    int use_exfat = 0;

    {
        unsigned char mbr_buf[512] __attribute__((aligned(64)));
        int r = blk_read_sectors(0, mbr_buf, 1);
        if (r < 0) { return; }
        if (mbr_buf[0] == 0xEB || mbr_buf[0] == 0xE9) {
            partition_start_sector = 0;
            if (memcmp(mbr_buf + 3, "EXFAT   ", 8) == 0) {
                use_exfat = 1;
            } else {
            }
        } else {
            partition_start_sector = mbr_find_fat32_partition(mbr_buf);
            if (partition_start_sector > 0) {
            } else {
                partition_start_sector = mbr_find_exfat_partition(mbr_buf, &exfat_partition_num_sectors);
                if (partition_start_sector > 0) {
                    use_exfat = 1;
                } else {
                    return;
                }
            }

            // detect es partition from same mbr buffer (no extra i/o wasted!)
            if (mbr_buf[510] == 0x55 && mbr_buf[511] == 0xAA) {
                unsigned int es_ns = 0;
                unsigned int es_lba = es_mbr_find(mbr_buf, &es_ns);
                if (es_lba > 0) es_init_partition(es_lba);
            }
        }
    }

    // init the FS finally!
    int ret;
    if (use_exfat) {
        exfat_partition_start_sector = partition_start_sector;
        ret = exfat_init();
        if (ret < 0) { return; }
        g_use_exfat = 1;
        fat32_bytes_per_sector    = exfat_bytes_per_sector;
        fat32_sectors_per_cluster = exfat_sectors_per_cluster;
        fat32_cluster_size        = exfat_cluster_size;
        fat32_fat_start_sector    = exfat_fat_start_sector;
        fat32_fat_size            = exfat_fat_size;
        fat32_root_cluster        = exfat_root_cluster;
        fat32_data_start_sector   = exfat_data_start_sector;
        fat32_total_clusters      = exfat_cluster_count;
        fat32_total_sectors       = exfat_volume_sectors;
        fat_cached_sector         = 0xFFFFFFFF;
        // cache exfat free cluster count from bitmap at init
        // if we don't do this it breaks free space reporting...
        // I dunno... mayvbe there's a faster way?... I need more coffee...
        if (exfat_bitmap_cluster >= 2 && exfat_bitmap_size > 0) {
            unsigned int bmp_base = exfat_data_start_sector +
                (exfat_bitmap_cluster - 2) * exfat_sectors_per_cluster;
            unsigned char bmp[512] __attribute__((aligned(64)));
            unsigned int free_count = 0;
            unsigned int total_bytes = exfat_bitmap_size;
            unsigned int ab = 0;
            while (ab < total_bytes) {
                unsigned int sec = bmp_base + ab / 512;
                if (exfat_blk_read_sectors(sec, bmp, 1) < 0) break;
                unsigned int start = ab % 512;
                unsigned int end = total_bytes - (ab / 512) * 512;
                if (end > 512) end = 512;
                unsigned int off;
                for (off = start; off < end; off++) {
                    if (bmp[off] == 0xFF) continue;
                    int bit;
                    for (bit = 0; bit < 8; bit++) {
                        if (!(bmp[off] & (1 << bit))) free_count++;
                    }
                }
                ab = (ab / 512 + 1) * 512;
            }
            exfat_free_clusters = free_count;
        }
        // mirror to fat32 globals to satisfy those code paths that love 'em so much
        fat32_free_clusters = exfat_free_clusters;
    } else {
        g_use_exfat = 0;
        ret = fat32_init();
        if (ret < 0) { return; }
        // fat32_free_clusters already set by fat32_init via fsinfo
    }

    // let's now assign the ms0!
    int assigned = 0;
    int i2;
    for (i2 = 0; i2 < 20; i2++) {
        // sony uses flag | 0x80000000 which bit 31 is critical for kernel mount handling
        ret = k_sceIoAssign("ms0:", "msstor0:", "fatms0:", (int)0x80000001, NULL, 0);
        if (ret >= 0) { assigned = 1; break; }
        sceKernelDelayThread(50000); // errrr.....
    }
    if (!assigned) {
        if (mount_state == 0x1020) {
            // already mounted so just continue
        } else {
            return;
        }
    } else {
        mount_state = 0x1020;
    }
    media_flags = 0x2000; // media present

    // signal media thread that the card is inserted and mounted
    if (event_flag_id >= 0)
        sceKernelSetEventFlag(event_flag_id, EVT_INSERT);

    blk_fd_validated = 1; // fd was just opened lets skip the validation in reopen_blk_fd_if_needed
    fs_initialized = 2; // donezo

    // sony never closes blk_fd during normal operation....
    // it's technically more performant on the bit level to keep it open but there's some edge cases...
}

static int assign_thread(SceSize args, void *argp)
{
    (void)args; (void)argp;

    // wait 3s for mediasync to finish initial scan, then reopen the blk_fd
    sceKernelDelayThread(3000000); // .....yeaaaa let's not do this...
    if (blk_fd < 0 && mount_state == 0x1020) {
        if (fs_mutex >= 0) sceKernelLockMutex(fs_mutex, 1, 0);
        blk_fd = k_sceIoOpen("msstor0:", 0x04000003, 0);
        if (fs_mutex >= 0) sceKernelUnlockMutex(fs_mutex, 1);
        // fire insert callback so the xmb rescans
        if (blk_fd >= 0) {
            notify_all_callbacks(1);
        }
    }
    // register with mscmhc0 for hardware card events
    // passes pointer to eventflag sceuid as indata, receives reg handle in outdata
    static int mscmhc_reg_handle = 0;
    if (event_flag_id >= 0) {
        k_sceIoDevctl("mscmhc0:", 0x0203D802,
                       &event_flag_id, 4, &mscmhc_reg_handle, 4);
    }

    while (1) {
        if (event_flag_id >= 0) {
            u32 pattern = 0;
            sceKernelWaitEventFlag(event_flag_id,
                            EVT_INSERT | EVT_REMOVE,
                            PSP_EVENT_WAITOR | PSP_EVENT_WAITCLEAR,
                            &pattern, NULL);

            if (pattern & EVT_REMOVE) {
                // must hold mutex because vfs handlers on other threads share blk_fd
                if (fs_mutex >= 0) sceKernelLockMutex(fs_mutex, 1, 0);
                if (blk_fd >= 0) {
                    k_sceIoClose(blk_fd);
                    blk_fd = -1; blk_fd_pos = 0xFFFFFFFFFFFFFFFFULL;
                }
                blk_fd_validated = 0;
                k_sceIoUnassign("ms0:");
                mount_state = 0x1022;
                media_flags = 0x4000;
                if (fs_mutex >= 0) sceKernelUnlockMutex(fs_mutex, 1);
                notify_all_callbacks(2);
            }
            if (pattern & EVT_INSERT) {
                // only reinit if not already mounted
                if (mount_state != 0x1020) {
                    if (fs_mutex >= 0) sceKernelLockMutex(fs_mutex, 1, 0);
                    fs_initialized = 0;
                    blk_fd_validated = 0;
                    deferred_fs_init();
                    if (fs_mutex >= 0) sceKernelUnlockMutex(fs_mutex, 1);
                    if (blk_fd >= 0) {
                        notify_all_callbacks(1);
                    }
                }
            }
        }
    }

    sceKernelExitDeleteThread(0);
    return 0;
}

int module_reboot_phase(int phase)
{
    (void)phase;
    return 0;
}

// module stuffs

int module_start(SceSize args, void *argp)
{
    (void)args;
    (void)argp;

    memset(open_files, 0, sizeof(open_files));

    // handler-level mutex to serialize all vfs ops (match sony's fatms as best I can)
    fs_mutex = sceKernelCreateMutex("FatmsMtx", 0x301, 0, 0);

    event_flag_id = sceKernelCreateEventFlag("SceFatmsMedia", 1, 0, NULL);

    // codepage init
    // music_parser.prx and content_browser depend on codepage for metadata parsing
    {
        int cp_sizes[4] = {0};
        if (sceCodepage_driver_EE932176() >= 0) {
            if (sceCodepage_driver_1D0DE569(&cp_sizes[0], &cp_sizes[1],
                                             &cp_sizes[2], &cp_sizes[3]) == 0
                && cp_sizes[0] == 0 && cp_sizes[1] == 0) {
                int s2 = (cp_sizes[2] + 0x3F) & ~0x3F;
                int s3 = (cp_sizes[3] + 0x3F) & ~0x3F;
                int total = s2 + s3;
                if (total > 0) {
                    SceUID cp_fpl = sceKernelCreateFpl("SceFatfsCPTable", 1,
                                        0x0101, total, 1, NULL);
                    if (cp_fpl >= 0) {
                        void *cp_buf = NULL;
                        if (sceKernelTryAllocateFpl(cp_fpl, &cp_buf) >= 0 && cp_buf) {
                            int base = (int)cp_buf;
                            // from what i can find sony's 661 039BF9E9 takes 4 (buf, size) pairs.
                            // cp_sizes[0] and cp_sizes[1] are gated to 0 by our check above, so first two pairs have buf=base and size=0 (zero-length, same ptr)
                            // the third is buf=base (since aligned(0)+aligned(0)=0), size=cp_sizes[2]
                            // then it MUST be buf=base+aligned(cp_sizes[2]), size=cp_sizes[3] otherwise chunk data can get corrupted (even in local thanks for the report @nuclearkommando).
                            sceCodepage_driver_039BF9E9(
                                base, cp_sizes[0],
                                base, cp_sizes[1],
                                base, cp_sizes[2],
                                base + s2, cp_sizes[3]);
                        }
                    }
                }
            }
        }
    }

    // fpl pools created before sceioadddrv
    {
        SceUID fpl = sceKernelCreateFpl("SceFatfsFdBuf", 1, 0x0101, 0x4000, 1, NULL);
        if (fpl >= 0) {
            void *fpl_buf = NULL;
            sceKernelTryAllocateFpl(fpl, &fpl_buf);
        }
    }
    {
        SceUID fpl2 = sceKernelCreateFpl("SceFatfsDirentClear", 1, 0x0101, 0x1000, 1, NULL);
        if (fpl2 >= 0) {
            void *fpl_buf2 = NULL;
            sceKernelTryAllocateFpl(fpl2, &fpl_buf2);
        }
    }

    // register sysevent handler (sony does this for suspend/resume)
    sceKernelRegisterSysEventHandler(&fatms_sysevent);

    // sony's original fatms does not call sceiodeldrv or msstorcacheinit
    // we are basically the replacement so just register directly like sony does.
    int ret = k_sceIoAddDrv(&exfat_driver);
    if (ret < 0)
        return 0;

    // sony's media/mount thread, it should match SceFatmsMedia name + priority.
    // priority 0x10, attr 0x100001 (vfpu + kernel)
    assign_thid = sceKernelCreateThread("SceFatmsMedia", assign_thread,
                                        0x10, 0x4000, 0x00100001, NULL);
    if (assign_thid >= 0)
        sceKernelStartThread(assign_thid, 0, NULL);

    return 0;
}

int module_stop(SceSize args, void *argp)
{
    (void)args;
    (void)argp;

    sceKernelUnregisterSysEventHandler(&fatms_sysevent);

    k_sceIoUnassign("ms0:");

    if (blk_fd >= 0) {
        // do not send destructive devctls 0x0211d814/0x0210d816 on sd adapters lets keep it clean
        k_sceIoClose(blk_fd);
        blk_fd = -1; blk_fd_pos = 0xFFFFFFFFFFFFFFFFULL;
    }

    if (event_flag_id >= 0) {
        sceKernelDeleteEventFlag(event_flag_id);
        event_flag_id = -1;
    }

    k_sceIoDelDrv("fatms");

    return 0;
}
