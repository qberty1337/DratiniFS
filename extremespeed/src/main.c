// extremespeed manager for managing the raw iso partition.
// we'll also comms with dratinifs via sceidevctl for raw sector access... We should be good to manuipulate the whole diskk
// and escape partition table boundary scope

#include <pspkernel.h>
#include <pspdisplay.h>
#include <pspctrl.h>
#include <psppower.h>
#include <pspiofilemgr.h>
#include <pspdebug.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <pspsdk.h>

#include "extremespeed_format.h"

PSP_MODULE_INFO("DratiniFS ExtremeSpeed", 0x800, 1, 0);
PSP_MAIN_THREAD_ATTR(PSP_THREAD_ATTR_VSH | PSP_THREAD_ATTR_VFPU);
PSP_HEAP_SIZE_KB(-1024);

#define printf pspDebugScreenPrintf

static int exit_requested = 0;
static int prx_found = 0, backup_found = 0, flash_state = 0;

static int exit_callback(int arg1, int arg2, void *common)
{
    (void)arg1; (void)arg2; (void)common;
    exit_requested = 1;
    return 0;
}

static int callback_thread(SceSize args, void *argp)
{
    (void)args; (void)argp;
    int cbid = sceKernelCreateCallback("ExitCB", exit_callback, NULL);
    sceKernelRegisterExitCallback(cbid);
    sceKernelSleepThreadCB();
    return 0;
}

static void setup_callbacks(void)
{
    int thid = sceKernelCreateThread("cb", callback_thread, 0x11, 0x1000, 0, NULL);
    if (thid >= 0) sceKernelStartThread(thid, 0, NULL);
}

// helpers n stuff

static void wait_release(unsigned int btn)
{
    SceCtrlData pad;
    do { sceCtrlReadBufferPositive(&pad, 1); } while (pad.Buttons & btn);
}

static void wait_press(unsigned int btn)
{
    SceCtrlData pad;
    do { sceCtrlReadBufferPositive(&pad, 1); } while (!(pad.Buttons & btn));
}

static unsigned int wait_any_button(void)
{
    SceCtrlData pad;
    do { sceCtrlReadBufferPositive(&pad, 1); } while (!pad.Buttons);
    return pad.Buttons;
}

static void wait_cross(void)
{
    wait_release(PSP_CTRL_CROSS);
    wait_press(PSP_CTRL_CROSS);
    wait_release(PSP_CTRL_CROSS);
}

// i/o

static unsigned char sector_buf[512] __attribute__((aligned(64)));

static int devctl_read_sectors(unsigned int sector, void *buf, unsigned int count)
{
    ExtremeSpeedDevCtlArg arg;
    arg.sector = sector;
    arg.count = count;
    arg.buffer = buf;
    return sceIoDevctl("fatms0:", ES_DEVCTL_READ_SECTOR, &arg, sizeof(arg), NULL, 0);
}

static int devctl_write_sectors(unsigned int sector, const void *buf, unsigned int count)
{
    sceKernelDcacheWritebackInvalidateAll();
    ExtremeSpeedDevCtlArg arg;
    arg.sector = sector;
    arg.count = count;
    arg.buffer = (void *)buf;
    return sceIoDevctl("fatms0:", ES_DEVCTL_WRITE_SECTOR, &arg, sizeof(arg), NULL, 0);
}

static int devctl_write_sectors_checked(unsigned int sector, const void *buf, unsigned int count)
{
    int r = devctl_write_sectors(sector, buf, count);
    if (r < 0) {
        pspDebugScreenSetTextColor(0xFF0000FF);
        printf("  WRITE FAILED at sector %u: 0x%08X\n", sector, r);
        pspDebugScreenSetTextColor(0xFFFFFFFF);
    }
    return r;
}

static int devctl_get_info(ExtremeSpeedInfo *info)
{
    return sceIoDevctl("fatms0:", ES_DEVCTL_GET_INFO, NULL, 0, info, sizeof(*info));
}

static int devctl_rescan(void)
{
    return sceIoDevctl("fatms0:", ES_DEVCTL_RESCAN, NULL, 0, NULL, 0);
}

static void devctl_sync_fd(void)
{
    sceIoDevctl("fatms0:", ES_DEVCTL_SYNC_FD, NULL, 0, NULL, 0);
}

// superblock integrity

static unsigned int es_crc32(const void *data, unsigned int len)
{
    const unsigned char *p = (const unsigned char *)data;
    unsigned int crc = 0xFFFFFFFF;
    unsigned int i, j;
    for (i = 0; i < len; i++) {
        crc ^= p[i];
        for (j = 0; j < 8; j++)
            crc = (crc >> 1) ^ (0xEDB88320 & -(crc & 1));
    }
    return ~crc;
}

static int __attribute__((unused)) validate_mbr(const unsigned char *buf)
{
    return (buf[510] == 0x55 && buf[511] == 0xAA);
}

static int __attribute__((unused)) validate_superblock(const unsigned char *buf)
{
    const ExtremeSpeedSuperblock *sb = (const ExtremeSpeedSuperblock *)buf;
    return (sb->magic == ES_MAGIC && sb->version == ES_VERSION);
}

static int write_mbr_verified(unsigned char *buf)
{
    sceKernelDcacheWritebackInvalidateAll();
    int r = devctl_write_sectors(0, buf, 1);
    if (r < 0) return r;
    // read back and compare
    unsigned char verify[512] __attribute__((aligned(64)));
    r = devctl_read_sectors(0, verify, 1);
    if (r < 0) return r;
    if (memcmp(buf, verify, 512) != 0) {
        pspDebugScreenSetTextColor(0xFF0000FF);
        printf("  MBR VERIFY FAILED — data mismatch!\n");
        pspDebugScreenSetTextColor(0xFFFFFFFF);
        return -1;
    }
    return 0;
}

// write superblock + backup at sector 200
static int write_superblock_with_backup(unsigned int partition_start, unsigned char *sb_buf)
{
    ExtremeSpeedSuperblock *sb = (ExtremeSpeedSuperblock *)sb_buf;
    // compute crc32 over superblock (excluding checksum field itself)
    sb->checksum = 0;
    sb->checksum = es_crc32(sb_buf, 512);

    if (devctl_write_sectors_checked(partition_start, sb_buf, 1) < 0)
        return -1;
    if (devctl_write_sectors_checked(partition_start + ES_BACKUP_SB_SECTOR, sb_buf, 1) < 0)
        return -1;
    return 0;
}

// in RAM cache of the name pool
static unsigned char g_pool[ES_NAME_POOL_BYTES] __attribute__((aligned(64)));

// load the entire name pool from disk
static int pool_refresh(unsigned int partition_start)
{
    int s;
    for (s = 0; s < ES_NAME_POOL_SECTORS; s++) {
        if (devctl_read_sectors(partition_start + ES_NAME_POOL_START_SECTOR + s,
                                g_pool + s * ES_SECTOR_SIZE, 1) < 0)
            return -1;
    }
    g_pool[ES_NAME_POOL_BYTES - 1] = 0;  // safety NUL at the very end
    return 0;
}

static const char *pool_get(const ExtremeSpeedEntry *ent)
{
    if (!ent || ent->name_len == 0) return "";
    if (ent->name_off == ES_NAME_INVALID_OFF) return "";
    if ((unsigned int)ent->name_off + (unsigned int)ent->name_len >= ES_NAME_POOL_BYTES)
        return "";
    return (const char *)&g_pool[ent->name_off];
}

static unsigned int pool_append_name(unsigned int partition_start,
                                     unsigned int pool_used,
                                     const char *name,
                                     int name_len)
{
    if (name_len <= 0 || name_len >= ES_NAME_MAX_LEN) return 0xFFFFFFFFu;
    // need name_len bytes + 1 NUL
    if ((unsigned int)pool_used + (unsigned int)name_len + 1 > ES_NAME_POOL_BYTES)
        return 0xFFFFFFFFu;

    unsigned int sec_idx = pool_used / ES_SECTOR_SIZE;
    unsigned int off_in_sec = pool_used % ES_SECTOR_SIZE;
    unsigned int abs_sec = partition_start + ES_NAME_POOL_START_SECTOR + sec_idx;

    // spans 2 sectors if (off_in_sec + name_len + 1) > ES_SECTOR_SIZE
    unsigned char buf[ES_SECTOR_SIZE * 2] __attribute__((aligned(64)));
    int spans_two = (off_in_sec + (unsigned int)name_len + 1) > ES_SECTOR_SIZE;

    if (devctl_read_sectors(abs_sec, buf, 1) < 0) return 0xFFFFFFFFu;
    if (spans_two) {
        if (devctl_read_sectors(abs_sec + 1, buf + ES_SECTOR_SIZE, 1) < 0)
            return 0xFFFFFFFFu;
    }
    memcpy(buf + off_in_sec, name, name_len);
    buf[off_in_sec + name_len] = '\0';

    if (devctl_write_sectors(abs_sec, buf, 1) < 0) return 0xFFFFFFFFu;
    if (spans_two) {
        if (devctl_write_sectors(abs_sec + 1, buf + ES_SECTOR_SIZE, 1) < 0)
            return 0xFFFFFFFFu;
    }
    return pool_used;
}


// basic game id extraction... is there a better way to do this?
static int iso_extract_game_id(SceUID fd, char *out_id, int id_size)
{
    unsigned char buf[2048] __attribute__((aligned(64)));
    memset(out_id, 0, id_size);

    // read primary volume descriptor at iso sector 16
    sceIoLseek(fd, 32768, PSP_SEEK_SET);
    if (sceIoRead(fd, buf, 2048) < 2048) return -1;
    if (buf[0] != 1) return -2; // not a pvd

    // root directory record is at pvd offset 156
    unsigned int root_lba = buf[156 + 2] | (buf[156 + 3] << 8)
                          | (buf[156 + 4] << 16) | (buf[156 + 5] << 24);
    unsigned int root_size = buf[156 + 10] | (buf[156 + 11] << 8)
                           | (buf[156 + 12] << 16) | (buf[156 + 13] << 24);
    if (root_lba == 0 || root_size == 0) return -3;

    // scan root directory for "psp_game"
    unsigned int psp_game_lba = 0, psp_game_size = 0;
    unsigned int off = 0;
    sceIoLseek(fd, (long long)root_lba * 2048, PSP_SEEK_SET);
    while (off < root_size) {
        if (off % 2048 == 0) {
            if (sceIoRead(fd, buf, 2048) < 2048) return -4;
        }
        unsigned int pos = off % 2048;
        unsigned char rec_len = buf[pos];
        if (rec_len == 0) { off = (off / 2048 + 1) * 2048; continue; }
        unsigned char name_len = buf[pos + 32];
        if (name_len >= 8) {
            if (memcmp(buf + pos + 33, "PSP_GAME", 8) == 0) {
                psp_game_lba = buf[pos+2] | (buf[pos+3]<<8)
                             | (buf[pos+4]<<16) | (buf[pos+5]<<24);
                psp_game_size = buf[pos+10] | (buf[pos+11]<<8)
                              | (buf[pos+12]<<16) | (buf[pos+13]<<24);
                break;
            }
        }
        off += rec_len;
    }
    if (psp_game_lba == 0) return -5;

    unsigned int sfo_lba = 0, sfo_size = 0;
    off = 0;
    sceIoLseek(fd, (long long)psp_game_lba * 2048, PSP_SEEK_SET);
    while (off < psp_game_size) {
        if (off % 2048 == 0) {
            if (sceIoRead(fd, buf, 2048) < 2048) return -6;
        }
        unsigned int pos = off % 2048;
        unsigned char rec_len = buf[pos];
        if (rec_len == 0) { off = (off / 2048 + 1) * 2048; continue; }
        unsigned char name_len = buf[pos + 32];
        if (name_len >= 9) {
            if (memcmp(buf + pos + 33, "PARAM.SFO", 9) == 0) {
                sfo_lba = buf[pos+2] | (buf[pos+3]<<8)
                        | (buf[pos+4]<<16) | (buf[pos+5]<<24);
                sfo_size = buf[pos+10] | (buf[pos+11]<<8)
                         | (buf[pos+12]<<16) | (buf[pos+13]<<24);
                break;
            }
        }
        off += rec_len;
    }
    if (sfo_lba == 0 || sfo_size == 0 || sfo_size > 2048) return -7;

    // read the damn sfo
    sceIoLseek(fd, (long long)sfo_lba * 2048, PSP_SEEK_SET);
    if (sceIoRead(fd, buf, sfo_size > 2048 ? 2048 : sfo_size) < (int)sfo_size) return -8;

    unsigned int sfo_magic;
    memcpy(&sfo_magic, buf, 4);
    if (sfo_magic != 0x46535000) return -9;

    unsigned int key_table_off, data_table_off, entry_count;
    memcpy(&key_table_off, buf + 8, 4);
    memcpy(&data_table_off, buf + 12, 4);
    memcpy(&entry_count, buf + 16, 4);

    unsigned int i;
    for (i = 0; i < entry_count; i++) {
        unsigned int idx_off = 20 + i * 16;
        if (idx_off + 16 > sfo_size) break;
        unsigned short key_off;
        unsigned int val_size, data_off;
        memcpy(&key_off, buf + idx_off, 2);
        memcpy(&val_size, buf + idx_off + 4, 4);
        memcpy(&data_off, buf + idx_off + 12, 4);

        unsigned int kpos = key_table_off + key_off;
        unsigned int vpos = data_table_off + data_off;
        if (kpos + 8 > sfo_size || vpos >= sfo_size) continue;

        if (memcmp(buf + kpos, "DISC_ID", 7) == 0 && buf[kpos + 7] == 0) {
            unsigned int copy = val_size;
            if (copy > (unsigned int)(id_size - 1)) copy = id_size - 1;
            if (vpos + copy > sfo_size) copy = sfo_size - vpos;
            memcpy(out_id, buf + vpos, copy);
            out_id[copy] = '\0';
            return 0;
        }
    }
    //can't find it. ripski
    return -10;
}

// some types to reuse

typedef struct {
    unsigned char status;
    unsigned char chs_first[3];
    unsigned char type;
    unsigned char chs_last[3];
    unsigned int lba_start;
    unsigned int num_sectors;
} __attribute__((packed)) MBRPartEntry;

static const char *part_type_name(unsigned char type)
{
    switch (type) {
    case 0x00: return "Empty";
    case 0x05: return "Extended";
    case 0x06: return "FAT16";
    case 0x07: return "exFAT/NTFS";
    case 0x0B: return "FAT32";
    case 0x0C: return "FAT32 LBA";
    case 0x0F: return "Ext LBA";
    case ES_PARTITION_TYPE: return "ExtremeSpeed";
    default:   return "Unknown";
    }
}

static void print_size(unsigned int sectors)
{
    unsigned int mb = sectors / 2048;
    if (mb >= 1024)
        printf("%u.%u GB", mb / 1024, (mb % 1024) * 10 / 1024);
    else
        printf("%u MB", mb);
}

// menu

static void show_partition_info(void)
{
    pspDebugScreenClear();
    pspDebugScreenSetTextColor(0xFF00FFFF); // ye we goin' with yellow
    printf("=== Partition Info ===\n\n");
    pspDebugScreenSetTextColor(0xFFFFFFFF);

    int r = devctl_read_sectors(0, sector_buf, 1);
    if (r < 0) {
        pspDebugScreenSetTextColor(0xFF0000FF);
        printf("Error reading MBR: 0x%08X\n", r);
        pspDebugScreenSetTextColor(0xFFFFFFFF);
        printf("\nPress X to return.\n");
        wait_cross();
        return;
    }

    if (sector_buf[510] != 0x55 || sector_buf[511] != 0xAA) {
        printf("No valid MBR signature found.\n");
        printf("\nPress X to return.\n");
        wait_cross();
        return;
    }

    const MBRPartEntry *entries = (const MBRPartEntry *)(sector_buf + 0x1BE);
    int i;

    // let's do a cute ascii bar
    {
        unsigned int disk_end = 0;
        for (i = 0; i < 4; i++) {
            if (entries[i].type == 0x00) continue;
            unsigned int end = entries[i].lba_start + entries[i].num_sectors;
            if (end > disk_end) disk_end = end;
        }
        unsigned int total_disk = disk_end;
        {
            unsigned char pb[512] __attribute__((aligned(64)));
            if (devctl_read_sectors(disk_end, pb, 1) > 0) {
                unsigned int lo = disk_end, step = 131072;
                unsigned int probe = disk_end + step;
                while (devctl_read_sectors(probe, pb, 1) > 0) {
                    lo = probe; step *= 2; probe = disk_end + step;
                    if (step > 1024UL * 1024 * 2048UL) break;
                }
                unsigned int hi = probe;
                while (lo + 1 < hi) {
                    unsigned int mid = lo + (hi - lo) / 2;
                    if (devctl_read_sectors(mid, pb, 1) > 0) lo = mid; else hi = mid;
                }
                total_disk = lo + 1;
            }
        }

        #define INFO_BAR_W 63
        printf("  +");
        { int j; for (j = 0; j < INFO_BAR_W; j++) printf("-"); }
        printf("+\n  |");

        // draw each partition as a colored segment
        int chars_used = 0;
        for (i = 0; i < 4; i++) {
            if (entries[i].type == 0x00) continue;
            int pw = (int)((unsigned long long)entries[i].num_sectors * INFO_BAR_W / total_disk);
            if (pw < 1) pw = 1;
            if (chars_used + pw > INFO_BAR_W) pw = INFO_BAR_W - chars_used;
            unsigned char pt = entries[i].type;
            if (pt == ES_PARTITION_TYPE)
                pspDebugScreenSetTextColor(0xFF0080FF);
            else
                pspDebugScreenSetTextColor(0xFF00FF00); //vert
            { int j; for (j = 0; j < pw; j++) printf(pt == ES_PARTITION_TYPE ? "#" : "="); }
            chars_used += pw;
        }
        // unallocated remainder
        if (chars_used < INFO_BAR_W) {
            pspDebugScreenSetTextColor(0xFF808080);
            { int j; for (j = chars_used; j < INFO_BAR_W; j++) printf("."); }
        }
        pspDebugScreenSetTextColor(0xFFFFFFFF);
        printf("|\n  +");
        { int j; for (j = 0; j < INFO_BAR_W; j++) printf("-"); }
        printf("+\n");

        printf("  ");
        for (i = 0; i < 4; i++) {
            if (entries[i].type == 0x00) continue;
            unsigned char pt = entries[i].type;
            if (pt == ES_PARTITION_TYPE)
                pspDebugScreenSetTextColor(0xFF0080FF);
            else
                pspDebugScreenSetTextColor(0xFF00FF00);
            const char *name = part_type_name(pt);
            unsigned int mb = entries[i].num_sectors / 2048;
            if (mb >= 1024) printf("%s:%u.%uGB  ", name, mb/1024, (mb%1024)*10/1024);
            else printf("%s:%uMB  ", name, mb);
        }
        if (total_disk > disk_end) {
            unsigned int fmb = (total_disk - disk_end) / 2048;
            pspDebugScreenSetTextColor(0xFF808080);
            if (fmb >= 1024) printf("Free:%u.%uGB", fmb/1024, (fmb%1024)*10/1024);
            else if (fmb > 0) printf("Free:%uMB", fmb);
        }
        pspDebugScreenSetTextColor(0xFFFFFFFF);
        printf("\n\n");
    }
    // cheesy legend from the rust app
    printf("  # | Type          | Start LBA  | Sectors    | Size\n");
    printf("  --+---------------+------------+------------+--------\n");
    for (i = 0; i < 4; i++) {
        if (entries[i].type == 0x00) continue;
        printf("  %d | 0x%02X %-8s | %10u | %10u | ",
               i, entries[i].type, part_type_name(entries[i].type),
               entries[i].lba_start, entries[i].num_sectors);
        print_size(entries[i].num_sectors);
        printf("\n");
    }

    // es partition info
    printf("\n");
    ExtremeSpeedInfo es_info;
    if (devctl_get_info(&es_info) >= 0 && es_info.initialized) {
        pspDebugScreenSetTextColor(0xFF00FF00);
        printf("ExtremeSpeed: Active\n");
        pspDebugScreenSetTextColor(0xFFFFFFFF);
        printf("  ISOs: %lu\n", es_info.iso_count);
        printf("  Data start: sector %lu\n", es_info.data_start_sector);
        printf("  Next free:  sector %lu\n", es_info.free_sector);
        if (es_info.total_sectors > 0) {
            printf("  Total size: "); print_size(es_info.total_sectors); printf("\n");
            unsigned int used = es_info.free_sector - es_info.data_start_sector;
            unsigned int free_s = es_info.total_sectors - es_info.free_sector;
            printf("  Used: "); print_size(used);
            printf("  Free: "); print_size(free_s); printf("\n");
        }
    } else {
        pspDebugScreenSetTextColor(0xFF0000FF);
        printf("ExtremeSpeed: Not detected\n");
        pspDebugScreenSetTextColor(0xFFFFFFFF);
    }

    printf("\nPress X to return.\n");
    wait_cross();
}

static void create_partition(void)
{
    pspDebugScreenClear();
    pspDebugScreenSetTextColor(0xFF00FFFF);
    printf("=== Create ExtremeSpeed Partition ===\n\n");

    int r = devctl_read_sectors(0, sector_buf, 1);
    if (r < 0 || sector_buf[510] != 0x55 || sector_buf[511] != 0xAA) {
        pspDebugScreenSetTextColor(0xFF0000FF);
        printf("Cannot read MBR.\n");
        printf("\nPress X to return.\n");
        wait_cross();
        return;
    }

    MBRPartEntry *entries = (MBRPartEntry *)(sector_buf + 0x1BE);

    int i;
    for (i = 0; i < 4; i++) {
        if (entries[i].type == ES_PARTITION_TYPE) {
            pspDebugScreenSetTextColor(0xFF00FF00);
            printf("ExtremeSpeed partition already exists (slot %d).\n", i);
            printf("\nPress X to return.\n");
            wait_cross();
            return;
        }
    }

    // find the last fs partition and the last empty slot. es must go in the last empty slot so it doesn't precede the filesystem partitions
    int last_part = -1;
    int empty_slot = -1;
    for (i = 0; i < 4; i++) {
        if (entries[i].type != 0x00 && entries[i].lba_start > 0)
            last_part = i;
        if (entries[i].type == 0x00)
            empty_slot = i; // keep overwriting here it's gonna end up as last empty
    }

    if (last_part < 0 || empty_slot < 0) {
        pspDebugScreenSetTextColor(0xFF0000FF);
        printf("No suitable partition layout found.\n");
        printf("Need: at least one FS partition and one empty MBR slot.\n");
        printf("\nPress X to return.\n");
        wait_cross();
        return;
    }

    unsigned int part_end = entries[last_part].lba_start + entries[last_part].num_sectors;
    unsigned int part_size = entries[last_part].num_sectors;

    pspDebugScreenSetTextColor(0xFFFFFFFF);
    printf("Last partition: slot %d, type 0x%02X\n", last_part, entries[last_part].type);
    printf("  Start: %u, Size: ", entries[last_part].lba_start);
    print_size(part_size);
    printf("\n\n");

    // dynamic options from 2 gb up to all available space, should I just procedurally generate these based on disk portions? or usage patterns?
    // hmm... we'll do some heuristics later if it's worth it
    unsigned int size_options[8];
    char size_labels[8][32];
    int num_options = 0;

    // fixed tiers for now 2, 4, 8, 16, 32, 64 gb
    unsigned int tiers[] = { 2, 4, 8, 16, 32, 64 };
    int ti;
    for (ti = 0; ti < 6 && num_options < 7; ti++) {
        unsigned int sectors = tiers[ti] * 1024UL * 2048UL;
        if (sectors >= part_size) break; // leave some spacde for their fs
        size_options[num_options] = sectors;
        snprintf(size_labels[num_options], 32, "%u GB", tiers[ti]);
        num_options++;
    }

    // "all available" option - shrink fs to used+100mb, give rest to es. query actual free space via getdevicesize.. or try to
    {
        unsigned int ds[5] = {0};
        unsigned int *dsp = ds;
        int dr = sceIoDevctl("fatms0:", 0x02425818, &dsp, 4, NULL, 0);
        if (dr >= 0 && ds[0] > 0 && ds[4] > 0) {
            unsigned int used_clusters = ds[0] - ds[1];
            unsigned int spc = ds[4];
            unsigned int used_sectors = used_clusters * spc;
            unsigned int fs_keep = used_sectors + 204800;
            fs_keep += part_size / 100;
            if (spc > 0) fs_keep = ((fs_keep + spc - 1) / spc) * spc;
            if (fs_keep < part_size) {
                unsigned int all_sectors = part_size - fs_keep;
                if (num_options == 0 || all_sectors > size_options[num_options - 1]) {
                    size_options[num_options] = all_sectors;
                    unsigned int gb = all_sectors / (1024UL * 2048UL);
                    unsigned int mb_rem = (all_sectors / 2048) % 1024;
                    if (gb > 0 && mb_rem > 0)
                        snprintf(size_labels[num_options], 32, "Shrink~%u.%uGB", gb, mb_rem / 103);
                    else if (gb > 0)
                        snprintf(size_labels[num_options], 32, "Shrink ~%uGB", gb);
                    else
                        snprintf(size_labels[num_options], 32, "Shrink~%uMB", all_sectors / 2048);
                    num_options++;
                }
            }
        }
    }

    int unallocated_idx = -1;
    unsigned int disk_sectors = 0;
    {
        // bin search might be quickest here... though I'll need to check !NOTE: Revisit this
        unsigned char probe_buf[512] __attribute__((aligned(64)));
        if (devctl_read_sectors(part_end, probe_buf, 1) > 0) {
            unsigned int lo = part_end;
            unsigned int hi = part_end + 512 * 1024 * 2048UL;
            unsigned int step = 131072;
            unsigned int probe = part_end + step;
            while (devctl_read_sectors(probe, probe_buf, 1) > 0) {
                lo = probe;
                step *= 2;
                probe = part_end + step;
                if (step > 1024UL * 1024 * 2048UL) break;
            }
            hi = probe;
            while (lo + 1 < hi) {
                unsigned int mid = lo + (hi - lo) / 2;
                if (devctl_read_sectors(mid, probe_buf, 1) > 0)
                    lo = mid;
                else
                    hi = mid;
            }
            disk_sectors = lo + 1;
        }
        if (disk_sectors > part_end) {
            unsigned int unalloc = disk_sectors - part_end;
            if (unalloc >= 131072) {
                // offer 25%, 50%, 100% of unallocated space
                int pcts[] = { 25, 50, 100 };
                int pi;
                unallocated_idx = num_options;
                for (pi = 0; pi < 3 && num_options < 8; pi++) {
                    unsigned int sec = (unsigned int)((unsigned long long)unalloc * pcts[pi] / 100);
                    // we should align down to 8192 sectors
                    sec &= ~8191u;
                    if (sec < 131072) continue;
                    size_options[num_options] = sec;
                    unsigned int mb = sec / 2048;
                    unsigned int gb_whole = mb / 1024;
                    unsigned int gb_frac = (mb % 1024) * 100 / 1024;
                    if (gb_whole > 0)
                        snprintf(size_labels[num_options], 32, "%d%% Unallocated %u.%02u GB", pcts[pi], gb_whole, gb_frac);
                    else
                        snprintf(size_labels[num_options], 32, "%d%% Unallocated %u MB", pcts[pi], mb);
                    num_options++;
                }
            }
        }
    }

    int sel = 0;

    if (num_options == 0) {
        pspDebugScreenSetTextColor(0xFF0000FF);
        printf("Partition too small for ExtremeSpeed.\n");
        printf("\nPress X to return.\n");
        wait_cross();
        return;
    }

    printf("Select ExtremeSpeed partition size:\n");
    printf("  (Use UP/DOWN, then X to confirm, O to cancel)\n\n");

    // total disk extent for bar chart
    unsigned int total_disk = disk_sectors > 0 ? disk_sectors : part_end;

    while (!exit_requested) {
        pspDebugScreenSetXY(0, 5);

        // more bar!
        {
            unsigned int es_sec = size_options[sel];
            unsigned int fs_sec, es_start_sec, unalloc_sec;
            int is_unalloc = (unallocated_idx >= 0 && sel >= unallocated_idx);

            if (is_unalloc) {
                fs_sec = part_size;
                es_start_sec = part_end;
                // remaining unallocated after this es size.. this might leave a stray dot visually
                // since we can't use all of it technically. this is gonna bug me.
                unsigned int total_unalloc = total_disk - part_end;
                unalloc_sec = total_unalloc > es_sec ? total_unalloc - es_sec : 0;
            } else {
                fs_sec = part_size - es_sec;
                es_start_sec = entries[last_part].lba_start + fs_sec;
                unalloc_sec = total_disk > (es_start_sec + es_sec) ? total_disk - es_start_sec - es_sec : 0;
            }

            #define BAR_W 63
            int fs_w = (int)((unsigned long long)fs_sec * BAR_W / total_disk);
            int es_w = (int)((unsigned long long)es_sec * BAR_W / total_disk);
            int un_w = BAR_W - fs_w - es_w;
            if (fs_w < 1) fs_w = 1;
            if (es_w < 1) es_w = 1;
            if (un_w < 0) un_w = 0;
            if (un_w <= 0 || (unalloc_sec > 0 && unalloc_sec < total_disk / BAR_W)) { un_w = 0; es_w = BAR_W - fs_w; }
            // adjust to fit exactly bar_w
            while (fs_w + es_w + un_w > BAR_W) { if (un_w > 0) un_w--; else fs_w--; }
            while (fs_w + es_w + un_w < BAR_W) { if (unalloc_sec > 0) un_w++; else es_w++; }

            printf("  +");
            { int j; for (j = 0; j < BAR_W; j++) printf("-"); }
            printf("+\n");

            printf("  |");
            pspDebugScreenSetTextColor(0xFF00FF00);
            { int j; for (j = 0; j < fs_w; j++) printf("="); }
            pspDebugScreenSetTextColor(0xFF0080FF);
            { int j; for (j = 0; j < es_w; j++) printf("#"); }
            if (un_w > 0) {
                pspDebugScreenSetTextColor(0xFF808080); // going with gray for unallocated
                { int j; for (j = 0; j < un_w; j++) printf("."); }
            }
            pspDebugScreenSetTextColor(0xFFFFFFFF);
            printf("|\n");

            printf("  +");
            { int j; for (j = 0; j < BAR_W; j++) printf("-"); }
            printf("+\n");

            pspDebugScreenSetTextColor(0xFF00FF00);
            printf("  ");
            { const char *fsname = "FS";
              unsigned char pt = entries[last_part].type;
              if (pt == 0x0B || pt == 0x0C) fsname = "FAT32";
              else if (pt == 0x07) fsname = "exFAT";
              else if (pt == 0x06 || pt == 0x0E) fsname = "FAT16";
              unsigned int mb = fs_sec / 2048;
              if (mb >= 1024) printf("%s:%u.%uGB", fsname, mb/1024, (mb%1024)*10/1024);
              else printf("%s:%uMB", fsname, mb); }
            pspDebugScreenSetTextColor(0xFF0080FF);
            { unsigned int mb = es_sec / 2048;
              if (mb >= 1024) printf("  ES:%u.%uGB", mb/1024, (mb%1024)*10/1024);
              else printf("  ES:%uMB", mb); }
            if (un_w > 0 && unalloc_sec > 0) {
                unsigned int umb = (total_disk - es_start_sec - es_sec) / 2048;
                pspDebugScreenSetTextColor(0xFF808080);
                if (umb >= 1024) printf("  Free:%u.%uGB", umb/1024, (umb%1024)*10/1024);
                else if (umb > 0) printf("  Free:%uMB", umb);
            }
            pspDebugScreenSetTextColor(0xFFFFFFFF);
            printf("\n\n");
        }

        printf("Select ExtremeSpeed partition size:\n\n");

        for (i = 0; i < num_options; i++) {
            if (i == sel)
                pspDebugScreenSetTextColor(0xFF00FF00);
            else
                pspDebugScreenSetTextColor(0xFFFFFFFF);
            printf("  %s %s\n", (i == sel) ? ">" : " ", size_labels[i]);
        }
        // clear leftover lines from previous longer option list
        printf("                                \n");
        pspDebugScreenSetTextColor(0xFFFFFFFF);

        SceCtrlData pad;
        sceCtrlReadBufferPositive(&pad, 1);
        if (pad.Buttons & PSP_CTRL_UP) { if (sel > 0) sel--; wait_release(PSP_CTRL_UP); }
        if (pad.Buttons & PSP_CTRL_DOWN) { if (sel < num_options - 1) sel++; wait_release(PSP_CTRL_DOWN); }
        if (pad.Buttons & PSP_CTRL_CIRCLE) return;
        if (pad.Buttons & PSP_CTRL_CROSS) { wait_release(PSP_CTRL_CROSS); break; }
        sceDisplayWaitVblankStart();
    }

    unsigned int es_sectors = size_options[sel];
    int use_unallocated = (unallocated_idx >= 0 && sel >= unallocated_idx);

    pspDebugScreenClear();
    pspDebugScreenSetTextColor(0xFF0000FF);
    printf("!!! WARNING !!!\n\n");
    pspDebugScreenSetTextColor(0xFFFFFFFF);
    if (use_unallocated) {
        printf("This will create an ExtremeSpeed partition (");
        print_size(es_sectors);
        printf(")\nin UNALLOCATED space after the filesystem.\n\n");
        printf("The main partition will NOT be modified.\n\n");
    } else {
        printf("This will SHRINK partition %d by ", last_part);
        print_size(es_sectors);
        printf("\nand create an ExtremeSpeed partition in the freed space.\n\n");
        printf("Data beyond the new partition boundary WILL BE LOST.\n");
        printf("Back up important files first!\n\n");
    }
    pspDebugScreenSetTextColor(0xFF00FFFF);
    printf("Press START to confirm, or O to cancel.\n");
    pspDebugScreenSetTextColor(0xFFFFFFFF);

    while (!exit_requested) {
        SceCtrlData pad;
        sceCtrlReadBufferPositive(&pad, 1);
        if (pad.Buttons & PSP_CTRL_CIRCLE) return;
        if (pad.Buttons & PSP_CTRL_START) { wait_release(PSP_CTRL_START); break; }
        sceDisplayWaitVblankStart();
    }

    // let's perform the operation
    pspDebugScreenClear();
    printf("Creating ExtremeSpeed partition...\n\n");

    unsigned int new_fs_sectors;
    unsigned int es_lba_start;
    if (use_unallocated) {
        es_lba_start = part_end;
        es_lba_start = (es_lba_start + 8191) & ~8191u; // 4 meggers
        es_sectors = size_options[sel] - (es_lba_start - part_end); // adjust for alignment
        new_fs_sectors = part_size;
    } else {
        // reduce fs partition, place es in freed space
        new_fs_sectors = part_size - es_sectors;
        es_lba_start = entries[last_part].lba_start + new_fs_sectors;
        es_lba_start = (es_lba_start + 8191) & ~8191u;
        new_fs_sectors = es_lba_start - entries[last_part].lba_start;
        es_sectors = part_end - es_lba_start;
    }

    // no-op when its unalloc
    entries[last_part].num_sectors = new_fs_sectors;

    // add es partition in empty slot
    memset(&entries[empty_slot], 0, sizeof(MBRPartEntry));
    entries[empty_slot].type = ES_PARTITION_TYPE;
    entries[empty_slot].lba_start = es_lba_start;
    entries[empty_slot].num_sectors = es_sectors;

    // update filesystem metadata to match new partition size, skip for unallocated mode
    if (!use_unallocated) {
        unsigned int fs_start = entries[last_part].lba_start;
        unsigned char vbr[512] __attribute__((aligned(64)));
        if (devctl_read_sectors(fs_start, vbr, 1) >= 0) {
            if (memcmp(vbr + 3, "EXFAT   ", 8) == 0) {
                // let's try and follow the exfat spec but we can't match it exactly

                // update volumelength
                unsigned long long vol_len = (unsigned long long)new_fs_sectors;
                memcpy(vbr + 72, &vol_len, 8);

                // recalculate clustercount
                unsigned int heap_offset = 0;
                memcpy(&heap_offset, vbr + 88, 4);
                unsigned int spc_shift = vbr[109];
                unsigned int spc = 1u << spc_shift;
                unsigned int new_cluster_count = 0;
                if (new_fs_sectors > heap_offset && spc > 0)
                    new_cluster_count = (new_fs_sectors - heap_offset) / spc;
                memcpy(vbr + 92, &new_cluster_count, 4);

                // write updated vbr (sector 0)
                sceKernelDcacheWritebackInvalidateAll();
                devctl_write_sectors(fs_start, vbr, 1);

                // copy entire boot region (sectors 0-10) to backup (12-22). read each sector, write to backup offset.
                {
                    unsigned char bsec[512] __attribute__((aligned(64)));
                    unsigned int bs;
                    for (bs = 0; bs < 11; bs++) {
                        if (bs == 0) {
                            devctl_write_sectors(fs_start + 12 + bs, vbr, 1);
                        } else {
                            if (devctl_read_sectors(fs_start + bs, bsec, 1) >= 0)
                                devctl_write_sectors(fs_start + 12 + bs, bsec, 1);
                        }
                    }
                }

                // recalc
                {
                    unsigned int cksum = 0;
                    unsigned char csec[512] __attribute__((aligned(64)));
                    unsigned int bs;
                    for (bs = 0; bs < 11; bs++) {
                        if (bs == 0) {
                            memcpy(csec, vbr, 512);
                        } else {
                            devctl_read_sectors(fs_start + bs, csec, 1);
                        }
                        unsigned int bi;
                        for (bi = 0; bi < 512; bi++) {
                            if (bs == 0 && (bi == 106 || bi == 107 || bi == 112))
                                continue;
                            cksum = ((cksum >> 1) | (cksum << 31)) + (unsigned int)csec[bi];
                        }
                    }
                    unsigned int ci;
                    unsigned char cksec[512] __attribute__((aligned(64)));
                    for (ci = 0; ci < 128; ci++)
                        memcpy(cksec + ci * 4, &cksum, 4);
                    sceKernelDcacheWritebackInvalidateAll();
                    devctl_write_sectors(fs_start + 11, cksec, 1);
                    devctl_write_sectors(fs_start + 23, cksec, 1);
                }

                printf("  exFAT: VolumeLength=%u, ClusterCount=%u, checksum updated.\n",
                       new_fs_sectors, new_cluster_count);
            } else {
                // std fat32 bpb resixze
                memcpy(vbr + 32, &new_fs_sectors, 4);
                // also zero the 16-bit totalsectors16...if its set
                vbr[19] = 0; vbr[20] = 0;
                sceKernelDcacheWritebackInvalidateAll();
                devctl_write_sectors(fs_start, vbr, 1);
                devctl_write_sectors(fs_start + 6, vbr, 1);
                {
                    unsigned short fsinfo_sec = *(unsigned short *)(vbr + 48);
                    if (fsinfo_sec >= 1 && fsinfo_sec < 32) {
                        unsigned char fsi[512] __attribute__((aligned(64)));
                        if (devctl_read_sectors(fs_start + fsinfo_sec, fsi, 1) == 0) {
                            unsigned int unknown = 0xFFFFFFFF;
                            memcpy(fsi + 488, &unknown, 4); // free_count = unknown
                            sceKernelDcacheWritebackInvalidateAll();
                            devctl_write_sectors(fs_start + fsinfo_sec, fsi, 1);
                        }
                    }
                }
                printf("  FAT32: TotalSectors=%u, BPB+backup+FSInfo updated.\n", new_fs_sectors);
            }
        }
    }

    // write extremespeed superblock with backup
    unsigned char sb_buf[512] __attribute__((aligned(64)));
    memset(sb_buf, 0, 512);
    ExtremeSpeedSuperblock *sb = (ExtremeSpeedSuperblock *)sb_buf;
    sb->magic = ES_MAGIC;
    sb->version = ES_VERSION;
    sb->iso_count = 0;
    sb->data_start_sector = ES_DATA_START_DEFAULT;
    sb->total_sectors = es_sectors;
    sb->flags = use_unallocated ? ES_FLAG_FROM_UNALLOCATED : 0;

    if (write_superblock_with_backup(es_lba_start, sb_buf) < 0) {
        printf("Error writing superblock.\n");
        printf("\nPress X to return.\n");
        wait_cross();
        return;
    }
    printf("  Superblock + backup written at LBA %u.\n", es_lba_start);

    // zero the iso table sectors
    memset(sb_buf, 0, 512);
    unsigned int s;
    for (s = 1; s < ES_DATA_START_DEFAULT; s++) {
        devctl_write_sectors(es_lba_start + s, sb_buf, 1);
    }
    printf("  ISO table zeroed.\n");

    r = write_mbr_verified(sector_buf);
    if (r < 0) {
        pspDebugScreenSetTextColor(0xFF0000FF);
        printf("Error writing MBR!\n");
        printf("\nPress X to return.\n");
        wait_cross();
        return;
    }
    printf("  MBR updated (verified).\n");

    // rescan
    devctl_rescan();

    pspDebugScreenSetTextColor(0xFF00FF00);
    printf("\nExtremeSpeed partition created! (");
    print_size(es_sectors);
    printf(")\n");
    pspDebugScreenSetTextColor(0xFFFFFFFF);
    printf("\nReboot recommended for filesystem to recognize new size.\n");
    printf("Press X to return.\n");
    wait_cross();
}

static void delete_es_partition(void)
{
    pspDebugScreenClear();
    pspDebugScreenSetTextColor(0xFF00FFFF);
    printf("=== Delete ExtremeSpeed Partition ===\n\n");
    pspDebugScreenSetTextColor(0xFFFFFFFF);

    int r = devctl_read_sectors(0, sector_buf, 1);
    if (r < 0 || sector_buf[510] != 0x55 || sector_buf[511] != 0xAA) {
        pspDebugScreenSetTextColor(0xFF0000FF);
        printf("Cannot read MBR.\n");
        printf("\nPress X to return.\n");
        wait_cross();
        return;
    }

    MBRPartEntry *entries = (MBRPartEntry *)(sector_buf + 0x1BE);
    int es_slot = -1;
    int i;
    for (i = 0; i < 4; i++) {
        if (entries[i].type == ES_PARTITION_TYPE) { es_slot = i; break; }
    }
    if (es_slot < 0) {
        printf("No ExtremeSpeed partition found.\n");
        printf("\nPress X to return.\n");
        wait_cross();
        return;
    }

    printf("ES partition in MBR slot %d: ", es_slot);
    print_size(entries[es_slot].num_sectors);
    printf("\n\n");

    pspDebugScreenSetTextColor(0xFF0000FF);
    printf("!!! WARNING !!!\n\n");
    pspDebugScreenSetTextColor(0xFFFFFFFF);
    // read es superblock to check creation method
    unsigned char es_sb_buf[512] __attribute__((aligned(64)));
    int from_unallocated = 0;
    if (devctl_read_sectors(entries[es_slot].lba_start, es_sb_buf, 1) > 0) {
        ExtremeSpeedSuperblock *esb = (ExtremeSpeedSuperblock *)es_sb_buf;
        if (esb->magic == ES_MAGIC && (esb->flags & ES_FLAG_FROM_UNALLOCATED))
            from_unallocated = 1;
    }

    printf("All ISOs on the ExtremeSpeed partition will be LOST.\n");
    if (from_unallocated)
        printf("The space will become unallocated (filesystem unchanged).\n\n");
    else
        printf("The freed space will be added back to the filesystem.\n\n");
    pspDebugScreenSetTextColor(0xFF00FFFF);
    printf("Press START to confirm, or O to cancel.\n");
    pspDebugScreenSetTextColor(0xFFFFFFFF);

    while (!exit_requested) {
        SceCtrlData pad;
        sceCtrlReadBufferPositive(&pad, 1);
        if (pad.Buttons & PSP_CTRL_CIRCLE) return;
        if (pad.Buttons & PSP_CTRL_START) { wait_release(PSP_CTRL_START); break; }
        sceDisplayWaitVblankStart();
    }

    // find the filesystem partition to expand
    unsigned int es_lba = entries[es_slot].lba_start;
    unsigned int es_nsec = entries[es_slot].num_sectors;
    int fs_slot = -1;
    if (!from_unallocated) {
        for (i = 0; i < 4; i++) {
            if (i == es_slot) continue;
            if (entries[i].type != 0x00 && entries[i].lba_start > 0) {
                // pick the fs partition that ends right before the es partition
                unsigned int end = entries[i].lba_start + entries[i].num_sectors;
                if (end <= es_lba) {
                    if (fs_slot < 0 || entries[i].lba_start > entries[fs_slot].lba_start)
                        fs_slot = i;
                }
            }
        }
    }

    // expand fs partition to reclaim es space (skip for unallocated)
    unsigned int reclaimed = es_nsec;
    if (!from_unallocated && fs_slot >= 0) {
        unsigned int new_fs_sectors = (es_lba + es_nsec) - entries[fs_slot].lba_start;
        entries[fs_slot].num_sectors = new_fs_sectors;
        printf("  Expanded partition %d to ", fs_slot);
        print_size(new_fs_sectors);
        printf("\n");

        unsigned int fs_start = entries[fs_slot].lba_start;
        unsigned char vbr[512] __attribute__((aligned(64)));
        if (devctl_read_sectors(fs_start, vbr, 1) >= 0) {
            if (memcmp(vbr + 3, "EXFAT   ", 8) == 0) {
                // exfat is update volumelength + clustercount + checksum + backup
                unsigned long long vol_len = (unsigned long long)new_fs_sectors;
                memcpy(vbr + 72, &vol_len, 8);
                unsigned int heap_offset = 0;
                memcpy(&heap_offset, vbr + 88, 4);
                unsigned int spc_shift = vbr[109];
                unsigned int spc_val = 1u << spc_shift;
                unsigned int new_cc = 0;
                if (new_fs_sectors > heap_offset && spc_val > 0)
                    new_cc = (new_fs_sectors - heap_offset) / spc_val;
                memcpy(vbr + 92, &new_cc, 4);
                sceKernelDcacheWritebackInvalidateAll();
                devctl_write_sectors(fs_start, vbr, 1);
                { unsigned char bsec[512] __attribute__((aligned(64))); unsigned int bs;
                  for (bs = 0; bs < 11; bs++) {
                      if (bs == 0) devctl_write_sectors(fs_start+12, vbr, 1);
                      else if (devctl_read_sectors(fs_start+bs, bsec, 1) >= 0)
                          devctl_write_sectors(fs_start+12+bs, bsec, 1);
                  }
                }
                // recalculate boot region checksum
                { unsigned int cksum = 0; unsigned char csec[512] __attribute__((aligned(64)));
                  unsigned int bs, bi;
                  for (bs = 0; bs < 11; bs++) {
                      if (bs == 0) memcpy(csec, vbr, 512);
                      else devctl_read_sectors(fs_start+bs, csec, 1);
                      for (bi = 0; bi < 512; bi++) {
                          if (bs == 0 && (bi == 106 || bi == 107 || bi == 112)) continue;
                          cksum = ((cksum >> 1) | (cksum << 31)) + (unsigned int)csec[bi];
                      }
                  }
                  unsigned char cksec[512] __attribute__((aligned(64)));
                  unsigned int ci;
                  for (ci = 0; ci < 128; ci++) memcpy(cksec + ci*4, &cksum, 4);
                  sceKernelDcacheWritebackInvalidateAll();
                  devctl_write_sectors(fs_start+11, cksec, 1);
                  devctl_write_sectors(fs_start+23, cksec, 1);
                }
                printf("  exFAT: ClusterCount=%u, checksum updated.\n", new_cc);
            } else {
                // fat32 is gonna be just  totalsectors32 + backup
                memcpy(vbr + 32, &new_fs_sectors, 4);
                vbr[19] = 0; vbr[20] = 0;
                sceKernelDcacheWritebackInvalidateAll();
                devctl_write_sectors(fs_start, vbr, 1);
                devctl_write_sectors(fs_start + 6, vbr, 1);
                printf("  FAT32: TotalSectors=%u updated.\n", new_fs_sectors);
            }
        }
    }

    // clear es mbr entry
    memset(&entries[es_slot], 0, sizeof(MBRPartEntry));
    sceKernelDcacheWritebackInvalidateAll();
    devctl_write_sectors(0, sector_buf, 1);
    printf("  MBR updated.\n");

    // rescan
    devctl_rescan();

    pspDebugScreenSetTextColor(0xFF00FF00);
    printf("\nExtremeSpeed partition deleted. Reclaimed ");
    print_size(reclaimed);
    printf(".\n");
    printf("\nPress X to return.\n");
    wait_cross();
}

static void list_isos(void)
{
    pspDebugScreenClear();
    pspDebugScreenSetTextColor(0xFF00FFFF);
    printf("=== ExtremeSpeed ISOs ===\n\n");
    pspDebugScreenSetTextColor(0xFFFFFFFF);

    ExtremeSpeedInfo info;
    if (devctl_get_info(&info) < 0 || !info.initialized) {
        pspDebugScreenSetTextColor(0xFF0000FF);
        printf("ExtremeSpeed partition not detected.\n");
        printf("\nPress X to return.\n");
        wait_cross();
        return;
    }

    if (info.iso_count == 0) {
        printf("No ISOs on ExtremeSpeed partition.\n");
        printf("\nPress X to return.\n");
        wait_cross();
        return;
    }

    unsigned int table_sectors = (info.iso_count * ES_ENTRY_SIZE + 511) / 512;
    unsigned char *table_buf = malloc(table_sectors * 512);
    if (!table_buf) {
        printf("Out of memory.\n");
        printf("\nPress X to return.\n");
        wait_cross();
        return;
    }

    unsigned int s;
    for (s = 0; s < table_sectors; s++) {
        int r = devctl_read_sectors(info.partition_start_sector + 1 + s,
                                     table_buf + s * 512, 1);
        if (r < 0) {
            printf("Error reading table sector %u.\n", s);
            free(table_buf);
            printf("\nPress X to return.\n");
            wait_cross();
            return;
        }
    }
    pool_refresh(info.partition_start_sector);

    printf("  # | Game ID         | Size     | Filename\n");
    printf("  --+-----------------+----------+---------\n");

    unsigned int idx;
    int shown = 0;
    for (idx = 0; idx < info.iso_count; idx++) {
        unsigned int off = idx * ES_ENTRY_SIZE;
        const ExtremeSpeedEntry *ent = (const ExtremeSpeedEntry *)(table_buf + off);
        if (!(ent->flags & ES_FLAG_ACTIVE)) continue;

        unsigned int mb = ent->size_lo / (1024 * 1024);
        printf(" %2d | %-15s | %4u MB  | %s\n",
               shown + 1, ent->game_id, mb, pool_get(ent));
        shown++;

        if (shown % 20 == 0) {
            printf("\n-- Press X for more, O to return --\n");
            unsigned int btn = wait_any_button();
            if (btn & PSP_CTRL_CIRCLE) break;
            wait_release(btn);
        }
    }

    free(table_buf);
    printf("\n%d ISOs total.\n", shown);
    printf("\nPress X to return.\n");
    wait_cross();
}

#define XFER_BUF_SIZE (128 * 1024) // 128 kb transfer buffer (enough?)

// scanner for ms0:/ISO/ that walks subfolders and records relative paths like "Genre/game.iso"
// let's try to mirror gclite's category layout so thatisos in subfolders can also be pushed into the es partition
static void scan_iso_dir_recursive(const char *base_path, const char *rel_prefix,
                                   char iso_names[][256], unsigned int *iso_sizes,
                                   int *iso_count, int max_count)
{
    if (*iso_count >= max_count) return;
    SceUID dfd = sceIoDopen(base_path);
    if (dfd < 0) return;

    SceIoDirent dirent;
    memset(&dirent, 0, sizeof(dirent));
    while (*iso_count < max_count && sceIoDread(dfd, &dirent) > 0) {
        if (dirent.d_name[0] == '.') continue;  // skip . and ..

        // skip es virtual entries - they have magic date 1992-03-27
        if (dirent.d_stat.sce_st_ctime.year == 1992 &&
            dirent.d_stat.sce_st_ctime.month == 3 &&
            dirent.d_stat.sce_st_ctime.day == 27)
            continue;

        if (FIO_S_ISDIR(dirent.d_stat.st_mode)) {
            // recurse into subdirectory
            char sub_path[512];
            char sub_rel[256];
            snprintf(sub_path, sizeof(sub_path), "%s/%s", base_path, dirent.d_name);
            if (rel_prefix[0])
                snprintf(sub_rel, sizeof(sub_rel), "%s/%s", rel_prefix, dirent.d_name);
            else
                snprintf(sub_rel, sizeof(sub_rel), "%s", dirent.d_name);
            scan_iso_dir_recursive(sub_path, sub_rel, iso_names, iso_sizes, iso_count, max_count);
            memset(&dirent, 0, sizeof(dirent));
            continue;
        }

        int len = strlen(dirent.d_name);
        if (len < 4) { memset(&dirent, 0, sizeof(dirent)); continue; }
        const char *ext = dirent.d_name + len - 4;
        if (strcasecmp(ext, ".iso") != 0 && strcasecmp(ext, ".cso") != 0) {
            memset(&dirent, 0, sizeof(dirent));
            continue;
        }

        if (rel_prefix[0])
            snprintf(iso_names[*iso_count], 256, "%s/%s", rel_prefix, dirent.d_name);
        else
            snprintf(iso_names[*iso_count], 256, "%s", dirent.d_name);
        iso_sizes[*iso_count] = (unsigned int)dirent.d_stat.st_size;
        (*iso_count)++;
        memset(&dirent, 0, sizeof(dirent));
    }
    sceIoDclose(dfd);
}

// walk a path like "ms0:/ISO/GEnre/game.iso and mkdir each intermediate directory
// errors from existing dirs are ignored and skips the "ms0:/" prefix.
static void ensure_parent_dirs(const char *path)
{
    char tmp[300];
    int len = strlen(path);
    if (len >= (int)sizeof(tmp)) return;

    int i;
    for (i = 0; i < len; i++) {
        tmp[i] = path[i];
        if (path[i] == '/' && i > 5) {
            tmp[i] = '\0';
            sceIoMkdir(tmp, 0777);  // ignore errors (EEXIST is fine...right sony?)
            tmp[i] = '/';
        }
    }
}

static void transfer_to_es(void)
{
    pspDebugScreenClear();
    pspDebugScreenSetTextColor(0xFF00FFFF);
    printf("=== Transfer ISO -> ExtremeSpeed ===\n\n");
    pspDebugScreenSetTextColor(0xFFFFFFFF);

    ExtremeSpeedInfo info;
    if (devctl_get_info(&info) < 0 || !info.initialized) {
        pspDebugScreenSetTextColor(0xFF0000FF);
        printf("ExtremeSpeed partition not detected.\n");
        printf("\nPress X to return.\n");
        wait_cross();
        return;
    }

    printf("Scanning ms0:/ISO/ ...\n\n");

    char iso_names[64][256];
    unsigned int iso_sizes[64];
    int iso_count = 0;

    // recursive scan so isos inside subfolders (gclite category layout) are found
    scan_iso_dir_recursive("ms0:/ISO", "", iso_names, iso_sizes, &iso_count, 64);

    if (iso_count == 0) {
        printf("No ISOs found in ms0:/ISO/\n");
        printf("\nPress X to return.\n");
        wait_cross();
        return;
    }

    int sel = 0;
    while (!exit_requested) {
        pspDebugScreenSetXY(0, 4);
        int i;
        int start = sel > 15 ? sel - 15 : 0;
        for (i = start; i < iso_count && i < start + 20; i++) {
            if (i == sel)
                pspDebugScreenSetTextColor(0xFF00FF00);
            else
                pspDebugScreenSetTextColor(0xFFFFFFFF);
            printf("  %s %-40.40s %4u MB\n",
                   (i == sel) ? ">" : " ", iso_names[i], iso_sizes[i] / (1024 * 1024));
        }
        pspDebugScreenSetTextColor(0xFFFFFFFF);
        printf("\n  UP/DOWN select, X confirm, O cancel\n");

        SceCtrlData pad;
        sceCtrlReadBufferPositive(&pad, 1);
        if (pad.Buttons & PSP_CTRL_UP) { if (sel > 0) sel--; wait_release(PSP_CTRL_UP); }
        if (pad.Buttons & PSP_CTRL_DOWN) { if (sel < iso_count - 1) sel++; wait_release(PSP_CTRL_DOWN); }
        if (pad.Buttons & PSP_CTRL_CIRCLE) return;
        if (pad.Buttons & PSP_CTRL_CROSS) { wait_release(PSP_CTRL_CROSS); break; }
        sceDisplayWaitVblankStart();
    }

    pspDebugScreenClear();
    printf("Transferring: %s\n", iso_names[sel]);
    unsigned int file_size = iso_sizes[sel];
    printf("Size: %u MB\n\n", file_size / (1024 * 1024));

    if (devctl_get_info(&info) < 0 || !info.initialized) {
        printf("ES partition lost.\n"); wait_cross(); return;
    }
    unsigned int free_sectors = info.total_sectors - info.free_sector;
    unsigned int needed_sectors = (file_size + 511) / 512;
    if (needed_sectors > free_sectors) {
        pspDebugScreenSetTextColor(0xFF0000FF);
        printf("Not enough space on ExtremeSpeed partition!\n");
        printf("  Need: "); print_size(needed_sectors);
        printf("  Free: "); print_size(free_sectors); printf("\n");
        printf("\nPress X to return.\n");
        wait_cross();
        return;
    }

    char path[300];
    snprintf(path, sizeof(path), "ms0:/ISO/%.255s", iso_names[sel]);
    SceUID src = sceIoOpen(path, PSP_O_RDONLY, 0);
    if (src < 0) {
        pspDebugScreenSetTextColor(0xFF0000FF);
        printf("Cannot open: %s (0x%08X)\n", path, (unsigned int)src);
        printf("\nPress X to return.\n");
        wait_cross();
        return;
    }

    // extract game id from iso's param.sfo before transfer
    char game_id[16];
    iso_extract_game_id(src, game_id, sizeof(game_id));
    sceIoLseek(src, 0, PSP_SEEK_SET); // reset the position for transfer (order is ccritical!)
    if (game_id[0]) printf("Game ID: %s\n", game_id);

    void *buf = malloc(XFER_BUF_SIZE);
    if (!buf) {
        sceIoClose(src);
        printf("Out of memory.\n");
        printf("\nPress X to return.\n");
        wait_cross();
        return;
    }

    unsigned int dest_sector = info.partition_start_sector + info.free_sector;
    unsigned int offset = 0;
    int error = 0;

    scePowerLock(0); // prevent sleep during transfer (should probably test if this actually works lol)

    while (offset < file_size && !exit_requested) {
        unsigned int chunk = XFER_BUF_SIZE;
        if (chunk > file_size - offset) chunk = file_size - offset;

        int rd = sceIoRead(src, buf, chunk);
        if (rd <= 0) { error = 1; break; }

        // pad last chunk to sector boundary
        unsigned int write_bytes = ((unsigned int)rd + 511) & ~511u;
        if (write_bytes > (unsigned int)rd)
            memset((char *)buf + rd, 0, write_bytes - rd);

        unsigned int write_sectors = write_bytes / 512;
        // write in batches of up to 128 sectors (64kb) for devctl
        unsigned int ws = 0;
        while (ws < write_sectors) {
            unsigned int batch = write_sectors - ws;
            if (batch > 128) batch = 128;
            int wr = devctl_write_sectors(dest_sector + ws,
                                           (char *)buf + ws * 512, batch);
            if (wr < 0) { error = 2; break; }
            ws += batch;
        }
        if (error) break;

        offset += (unsigned int)rd;
        dest_sector += write_sectors;

        // progress bar - update every 100 chunks (~12.8mb) to avoid display overhead
        { static int prog_counter = 0;
          if (++prog_counter >= 100 || offset >= file_size) {
              prog_counter = 0;
              unsigned int pct = (unsigned int)((unsigned long long)offset * 100 / file_size);
              pspDebugScreenSetXY(0, 4);
              printf("  Progress: [");
              int bi;
              for (bi = 0; bi < 40; bi++)
                  printf(bi < (int)(pct * 40 / 100) ? "#" : "-");
              printf("] %u%%\n", pct);
              printf("  %u / %u MB\n", offset / (1024 * 1024), file_size / (1024 * 1024));
          }
        }
    }

    scePowerUnlock(0);
    sceIoClose(src);
    free(buf);

    if (error) {
        pspDebugScreenSetTextColor(0xFF0000FF);
        printf("\nTransfer failed! (error=%d)\n", error);
        printf("\nPress X to return.\n");
        wait_cross();
        return;
    }

    printf("\nUpdating ExtremeSpeed table...\n");

    unsigned char sb_buf[512] __attribute__((aligned(64)));
    int r = devctl_read_sectors(info.partition_start_sector, sb_buf, 1);
    if (r < 0) {
        printf("Error reading superblock.\n");
        printf("\nPress X to return.\n");
        wait_cross();
        return;
    }

    ExtremeSpeedSuperblock *sb = (ExtremeSpeedSuperblock *)sb_buf;
    unsigned int new_idx = sb->iso_count;
    if (new_idx >= ES_MAX_ISOS) {
        printf("ISO table full (%d max).\n", ES_MAX_ISOS);
        printf("\nPress X to return.\n");
        wait_cross();
        return;
    }

    // we gonna build full filename, append to name pool, then writethe entry
    char fullname[ES_NAME_MAX_LEN];
    int fullname_len = snprintf(fullname, sizeof(fullname), "ISO/%s", iso_names[sel]);
    if (fullname_len <= 0 || fullname_len >= ES_NAME_MAX_LEN) {
        printf("Filename too long (%d chars max).\n", ES_NAME_MAX_LEN - 1);
        wait_cross(); return;
    }

    unsigned int pool_used = sb->pool_used_bytes;
    unsigned int name_off = pool_append_name(info.partition_start_sector,
                                              pool_used, fullname, fullname_len);
    if (name_off == 0xFFFFFFFFu) {
        printf("Name pool full or write failed!\n");
        wait_cross(); return;
    }

    // build the entry
    ExtremeSpeedEntry entry;
    memset(&entry, 0, sizeof(entry));
    memcpy(entry.game_id, game_id, ES_GAME_ID_SIZE);
    entry.start_sector = info.free_sector;
    entry.size_sectors = needed_sectors;
    entry.size_lo = file_size;
    entry.flags = ES_FLAG_ACTIVE;
    entry.name_off = name_off;
    entry.name_len = (uint16_t)fullname_len;

    unsigned int ent_byte_off = new_idx * ES_ENTRY_SIZE;
    unsigned int ent_sec = 1 + (ent_byte_off / 512);
    unsigned int ent_sec_off = ent_byte_off % 512;
    unsigned char ent_buf[512] __attribute__((aligned(64)));
    if (devctl_read_sectors(info.partition_start_sector + ent_sec, ent_buf, 1) < 0) {
        printf("CRITICAL: Entry sector read failed!\n"); wait_cross(); return;
    }
    memcpy(ent_buf + ent_sec_off, &entry, ES_ENTRY_SIZE);
    if (devctl_write_sectors_checked(info.partition_start_sector + ent_sec,
                                      ent_buf, 1) < 0) {
        printf("CRITICAL: Entry write failed!\n"); wait_cross(); return;
    }

    // write the superblock last (just in case power gets cut or somethin' midwrite)
    sb->iso_count = new_idx + 1;
    sb->pool_used_bytes = pool_used + fullname_len + 1;  // +1 for NUL
    if (write_superblock_with_backup(info.partition_start_sector, sb_buf) < 0) {
        printf("CRITICAL: Superblock write failed!\n"); wait_cross(); return;
    }

    // rescan so driver picks up the new entry
    devctl_rescan();

    pspDebugScreenSetTextColor(0xFF00FF00);
    printf("\nTransfer complete! %s is now on ExtremeSpeed.\n", iso_names[sel]);
    pspDebugScreenSetTextColor(0xFFFFFFFF);

    // offer to delete original iso to free filesystem space
    printf("\nDelete original from ms0:/ISO/? (X = Yes, O = No)\n");
    while (!exit_requested) {
        SceCtrlData pad;
        sceCtrlReadBufferPositive(&pad, 1);
        if (pad.Buttons & PSP_CTRL_CIRCLE) break;
        if (pad.Buttons & PSP_CTRL_CROSS) {
            wait_release(PSP_CTRL_CROSS);
            SceUID tf = sceIoOpen(path, PSP_O_WRONLY | PSP_O_TRUNC, 0777);
            if (tf >= 0) {
                sceIoClose(tf);
                int dr = sceIoRemove(path);
                if (dr >= 0) {
                    pspDebugScreenSetTextColor(0xFF00FF00);
                    printf("Deleted %s\n", iso_names[sel]);
                } else {
                    pspDebugScreenSetTextColor(0xFF00FFFF);
                    printf("Truncated to 0 bytes (dir entry cleanup failed)\n");
                }
            } else {
                pspDebugScreenSetTextColor(0xFF0000FF);
                printf("Delete failed: 0x%08X\n", (unsigned int)tf);
            }
            pspDebugScreenSetTextColor(0xFFFFFFFF);
            break;
        }
        sceDisplayWaitVblankStart();
    }

    printf("\nPress X to return.\n");
    wait_cross();
}

static void transfer_from_es(void)
{
    pspDebugScreenClear();
    pspDebugScreenSetTextColor(0xFF00FFFF);
    printf("=== Transfer ISO -> Memory Stick ===\n\n");
    pspDebugScreenSetTextColor(0xFFFFFFFF);

    ExtremeSpeedInfo es_info;
    if (devctl_get_info(&es_info) < 0 || !es_info.initialized || es_info.iso_count == 0) {
        printf("No ExtremeSpeed ISOs found.\n");
        printf("\nPress X to return.\n");
        wait_cross();
        return;
    }

    unsigned int table_sectors = (es_info.iso_count * ES_ENTRY_SIZE + 511) / 512;
    unsigned char *table_buf = malloc(table_sectors * 512);
    if (!table_buf) { printf("Out of memory.\n"); wait_cross(); return; }

    unsigned int s;
    for (s = 0; s < table_sectors; s++) {
        devctl_read_sectors(es_info.partition_start_sector + 1 + s,
                             table_buf + s * 512, 1);
    }
    pool_refresh(es_info.partition_start_sector);

    int active_indices[ES_MAX_ISOS];
    int active_count = 0;
    unsigned int idx;
    for (idx = 0; idx < es_info.iso_count && active_count < ES_MAX_ISOS; idx++) {
        const ExtremeSpeedEntry *ent = (const ExtremeSpeedEntry *)(table_buf + idx * ES_ENTRY_SIZE);
        if (ent->flags & ES_FLAG_ACTIVE)
            active_indices[active_count++] = (int)idx;
    }

    if (active_count == 0) {
        printf("No active ISOs.\n");
        free(table_buf);
        printf("\nPress X to return.\n");
        wait_cross();
        return;
    }

    int sel = 0;
    while (!exit_requested) {
        pspDebugScreenSetXY(0, 3);
        int i;
        for (i = 0; i < active_count && i < 20; i++) {
            const ExtremeSpeedEntry *ent = (const ExtremeSpeedEntry *)(table_buf + active_indices[i] * ES_ENTRY_SIZE);
            if (i == sel) pspDebugScreenSetTextColor(0xFF00FF00);
            else pspDebugScreenSetTextColor(0xFFFFFFFF);
            printf("  %s %-40.40s %4lu MB\n",
                   (i == sel) ? ">" : " ", pool_get(ent), ent->size_lo / (1024*1024));
        }
        pspDebugScreenSetTextColor(0xFFFFFFFF);
        printf("\n  UP/DOWN select, X confirm, O cancel\n");

        SceCtrlData pad;
        sceCtrlReadBufferPositive(&pad, 1);
        if (pad.Buttons & PSP_CTRL_UP) { if (sel > 0) sel--; wait_release(PSP_CTRL_UP); }
        if (pad.Buttons & PSP_CTRL_DOWN) { if (sel < active_count - 1) sel++; wait_release(PSP_CTRL_DOWN); }
        if (pad.Buttons & PSP_CTRL_CIRCLE) { free(table_buf); return; }
        if (pad.Buttons & PSP_CTRL_CROSS) { wait_release(PSP_CTRL_CROSS); break; }
        sceDisplayWaitVblankStart();
    }

    const ExtremeSpeedEntry *ent = (const ExtremeSpeedEntry *)(table_buf + active_indices[sel] * ES_ENTRY_SIZE);

    pspDebugScreenClear();
    printf("Transferring back: %s\n", pool_get(ent));
    printf("Size: %lu MB\n\n", ent->size_lo / (1024*1024));

    char dest[300];
    snprintf(dest, sizeof(dest), "ms0:/%s", pool_get(ent));
    ensure_parent_dirs(dest);

    SceUID dst = sceIoOpen(dest, PSP_O_WRONLY | PSP_O_CREAT | PSP_O_TRUNC, 0777);
    if (dst < 0) {
        pspDebugScreenSetTextColor(0xFF0000FF);
        printf("Cannot create: %s (0x%08X)\n", dest, dst);
        free(table_buf);
        printf("\nPress X to return.\n");
        wait_cross();
        return;
    }

    void *buf = malloc(XFER_BUF_SIZE);
    if (!buf) { sceIoClose(dst); free(table_buf); printf("Out of memory.\n"); wait_cross(); return; }

    unsigned int src_sector = es_info.partition_start_sector + ent->start_sector;
    unsigned int remaining = ent->size_lo;
    unsigned int offset = 0;
    int error = 0;

    scePowerLock(0);

    while (remaining > 0 && !exit_requested) {
        unsigned int chunk = XFER_BUF_SIZE;
        if (chunk > remaining) chunk = remaining;
        unsigned int read_sectors = (chunk + 511) / 512;

        unsigned int rs = 0;
        while (rs < read_sectors) {
            unsigned int batch = read_sectors - rs;
            if (batch > 128) batch = 128;
            int rr = devctl_read_sectors(src_sector + rs, (char *)buf + rs * 512, batch);
            if (rr < 0) { error = 1; break; }
            rs += batch;
        }
        if (error) break;
        src_sector += read_sectors;

        int wr = sceIoWrite(dst, buf, chunk);
        if (wr < (int)chunk) { error = 2; break; }

        remaining -= chunk;
        offset += chunk;

        { static int prog_counter2 = 0;
          if (++prog_counter2 >= 100 || remaining == 0) {
              prog_counter2 = 0;
              unsigned int pct = (unsigned int)((unsigned long long)offset * 100 / ent->size_lo);
              pspDebugScreenSetXY(0, 3);
              printf("  Progress: [");
              int bi;
              for (bi = 0; bi < 40; bi++) printf(bi < (int)(pct * 40 / 100) ? "#" : "-");
              printf("] %u%%\n", pct);
              printf("  %u / %lu MB\n", offset / (1024*1024), ent->size_lo / (1024*1024));
          }
        }
    }

    scePowerUnlock(0);
    sceIoClose(dst);
    free(buf);

    free(table_buf);

    if (error) {
        pspDebugScreenSetTextColor(0xFF0000FF);
        printf("\nTransfer failed! (error=%d)\n", error);
        pspDebugScreenSetTextColor(0xFFFFFFFF);
        printf("\nPress X to return.\n");
        wait_cross();
        return;
    }

    pspDebugScreenSetTextColor(0xFF00FF00);
    printf("\nTransfer complete!\n");
    pspDebugScreenSetTextColor(0xFFFFFFFF);
    printf("\nPress X to return.\n");
    wait_cross();
}


static void delete_iso(void)
{
    pspDebugScreenClear();
    pspDebugScreenSetTextColor(0xFF00FFFF);
    printf("=== Delete ISO from ExtremeSpeed ===\n\n");
    pspDebugScreenSetTextColor(0xFFFFFFFF);

    ExtremeSpeedInfo es_info;
    if (devctl_get_info(&es_info) < 0 || !es_info.initialized || es_info.iso_count == 0) {
        printf("No ExtremeSpeed ISOs found.\n");
        printf("\nPress X to return.\n");
        wait_cross();
        return;
    }

    unsigned int table_sectors = (es_info.iso_count * ES_ENTRY_SIZE + 511) / 512;
    unsigned char *table_buf = malloc(table_sectors * 512);
    if (!table_buf) { printf("Out of memory.\n"); wait_cross(); return; }

    unsigned int s;
    for (s = 0; s < table_sectors; s++) {
        devctl_read_sectors(es_info.partition_start_sector + 1 + s,
                             table_buf + s * 512, 1);
    }
    pool_refresh(es_info.partition_start_sector);

    int active_indices[ES_MAX_ISOS];
    int active_count = 0;
    unsigned int idx;
    for (idx = 0; idx < es_info.iso_count && active_count < ES_MAX_ISOS; idx++) {
        const ExtremeSpeedEntry *ent = (const ExtremeSpeedEntry *)(table_buf + idx * ES_ENTRY_SIZE);
        if (ent->flags & ES_FLAG_ACTIVE)
            active_indices[active_count++] = (int)idx;
    }

    if (active_count == 0) {
        printf("No active ISOs.\n");
        free(table_buf);
        printf("\nPress X to return.\n");
        wait_cross();
        return;
    }

    int sel = 0;
    while (!exit_requested) {
        pspDebugScreenSetXY(0, 3);
        int i;
        for (i = 0; i < active_count && i < 20; i++) {
            const ExtremeSpeedEntry *ent = (const ExtremeSpeedEntry *)(table_buf + active_indices[i] * ES_ENTRY_SIZE);
            if (i == sel) pspDebugScreenSetTextColor(0xFF0000FF);
            else pspDebugScreenSetTextColor(0xFFFFFFFF);
            printf("  %s %-40.40s %4lu MB\n",
                   (i == sel) ? ">" : " ", pool_get(ent), ent->size_lo / (1024*1024));
        }
        pspDebugScreenSetTextColor(0xFFFFFFFF);
        printf("\n  UP/DOWN select, X delete, O cancel\n");

        SceCtrlData pad;
        sceCtrlReadBufferPositive(&pad, 1);
        if (pad.Buttons & PSP_CTRL_UP) { if (sel > 0) sel--; wait_release(PSP_CTRL_UP); }
        if (pad.Buttons & PSP_CTRL_DOWN) { if (sel < active_count - 1) sel++; wait_release(PSP_CTRL_DOWN); }
        if (pad.Buttons & PSP_CTRL_CIRCLE) { free(table_buf); return; }
        if (pad.Buttons & PSP_CTRL_CROSS) { wait_release(PSP_CTRL_CROSS); break; }
        sceDisplayWaitVblankStart();
    }

    ExtremeSpeedEntry *ent = (ExtremeSpeedEntry *)(table_buf + active_indices[sel] * ES_ENTRY_SIZE);

    pspDebugScreenClear();
    printf("Delete: %s\n\n", pool_get(ent));
    pspDebugScreenSetTextColor(0xFF0000FF);
    printf("Press START to confirm, O to cancel.\n");
    pspDebugScreenSetTextColor(0xFFFFFFFF);

    while (!exit_requested) {
        SceCtrlData pad;
        sceCtrlReadBufferPositive(&pad, 1);
        if (pad.Buttons & PSP_CTRL_CIRCLE) { free(table_buf); return; }
        if (pad.Buttons & PSP_CTRL_START) { wait_release(PSP_CTRL_START); break; }
        sceDisplayWaitVblankStart();
    }

    ent->flags &= ~ES_FLAG_ACTIVE;

    // !NOTE: Don't... not... do this. (depending on if on fat or exfat I can't get a quick delete to be consistent for some reason)
    unsigned int entry_sec = 1 + (unsigned int)active_indices[sel];
    devctl_write_sectors_checked(es_info.partition_start_sector + entry_sec, ent, 1);

    devctl_rescan();
    free(table_buf);

    pspDebugScreenSetTextColor(0xFF00FF00);
    printf("\nISO deleted (marked inactive).\n");
    pspDebugScreenSetTextColor(0xFFFFFFFF);
    printf("Note: disk space is not reclaimed until partition is rebuilt.\n");
    printf("\nPress X to return.\n");
    wait_cross();
}

//just do what we do for single but lotss
static void transfer_all_to_es(void)
{
    pspDebugScreenClear();
    pspDebugScreenSetTextColor(0xFF00FFFF);
    printf("=== Transfer All ISOs -> ExtremeSpeed ===\n\n");
    pspDebugScreenSetTextColor(0xFFFFFFFF);

    ExtremeSpeedInfo info;
    if (devctl_get_info(&info) < 0 || !info.initialized) {
        pspDebugScreenSetTextColor(0xFF0000FF);
        printf("ExtremeSpeed partition not detected.\n");
        printf("\nPress X to return.\n");
        wait_cross();
        return;
    }

    char iso_names[64][256];
    unsigned int iso_sizes[64];
    int iso_count = 0;

    scan_iso_dir_recursive("ms0:/ISO", "", iso_names, iso_sizes, &iso_count, 64);

    if (iso_count == 0) {
        printf("No ISOs found in ms0:/ISO/\n");
        printf("\nPress X to return.\n");
        wait_cross();
        return;
    }

    printf("Found %d ISOs. Transfer all to ExtremeSpeed?\n", iso_count);
    printf("Press START to begin, O to cancel.\n\n");

    while (!exit_requested) {
        SceCtrlData pad;
        sceCtrlReadBufferPositive(&pad, 1);
        if (pad.Buttons & PSP_CTRL_CIRCLE) return;
        if (pad.Buttons & PSP_CTRL_START) { wait_release(PSP_CTRL_START); break; }
        sceDisplayWaitVblankStart();
    }

    void *buf = malloc(XFER_BUF_SIZE);
    if (!buf) { printf("Out of memory.\n"); wait_cross(); return; }

    scePowerLock(0);

    int transferred = 0, failed = 0;
    int fi;
    for (fi = 0; fi < iso_count && !exit_requested; fi++) {

        unsigned int file_size = iso_sizes[fi];
        unsigned int needed_sectors = (file_size + 511) / 512;
        unsigned int free_sectors = info.total_sectors - info.free_sector;
        if (needed_sectors > free_sectors) {
            pspDebugScreenSetTextColor(0xFF0000FF);
            printf("  [%d/%d] %s — no space, skipped\n", fi+1, iso_count, iso_names[fi]);
            pspDebugScreenSetTextColor(0xFFFFFFFF);
            failed++;
            continue;
        }

        printf("  [%d/%d] %s (%u MB)...", fi+1, iso_count, iso_names[fi], file_size/(1024*1024));

        char path[300];
        snprintf(path, sizeof(path), "ms0:/ISO/%.255s", iso_names[fi]);
        SceUID src = sceIoOpen(path, PSP_O_RDONLY, 0);
        if (src < 0) { printf(" open fail\n"); failed++; continue; }

        char game_id_bulk[16];
        iso_extract_game_id(src, game_id_bulk, sizeof(game_id_bulk));
        sceIoLseek(src, 0, PSP_SEEK_SET);

        unsigned int dest_sector = info.partition_start_sector + info.free_sector;
        unsigned int offset = 0;
        int error = 0;

        while (offset < file_size && !exit_requested) {
            unsigned int chunk = XFER_BUF_SIZE;
            if (chunk > file_size - offset) chunk = file_size - offset;
            int rd = sceIoRead(src, buf, chunk);
            if (rd <= 0) { error = 1; break; }
            unsigned int write_bytes = ((unsigned int)rd + 511) & ~511u;
            if (write_bytes > (unsigned int)rd)
                memset((char *)buf + rd, 0, write_bytes - rd);
            unsigned int write_sectors = write_bytes / 512;
            unsigned int ws = 0;
            while (ws < write_sectors) {
                unsigned int batch = write_sectors - ws;
                if (batch > 128) batch = 128;
                int wr = devctl_write_sectors(dest_sector + ws, (char *)buf + ws * 512, batch);
                if (wr < 0) { error = 2; break; }
                ws += batch;
            }
            if (error) break;
            offset += (unsigned int)rd;
            dest_sector += write_sectors;
        }
        sceIoClose(src);

        if (error) { printf(" FAILED\n"); failed++; continue; }

        unsigned char sb_buf[512] __attribute__((aligned(64)));
        devctl_read_sectors(info.partition_start_sector, sb_buf, 1);
        ExtremeSpeedSuperblock *sb = (ExtremeSpeedSuperblock *)sb_buf;
        unsigned int new_idx = sb->iso_count;
        if (new_idx >= ES_MAX_ISOS) { printf(" table full\n"); failed++; continue; }

        char fullname[ES_NAME_MAX_LEN];
        int fullname_len = snprintf(fullname, sizeof(fullname), "ISO/%s", iso_names[fi]);
        if (fullname_len <= 0 || fullname_len >= ES_NAME_MAX_LEN) {
            printf(" name too long\n"); failed++; continue;
        }
        unsigned int pool_used = sb->pool_used_bytes;
        unsigned int name_off = pool_append_name(info.partition_start_sector,
                                                  pool_used, fullname, fullname_len);
        if (name_off == 0xFFFFFFFFu) {
            printf(" name pool full\n"); failed++; continue;
        }

        ExtremeSpeedEntry entry;
        memset(&entry, 0, sizeof(entry));
        memcpy(entry.game_id, game_id_bulk, ES_GAME_ID_SIZE);
        entry.start_sector = info.free_sector;
        entry.size_sectors = needed_sectors;
        entry.size_lo = file_size;
        entry.flags = ES_FLAG_ACTIVE;
        entry.name_off = name_off;
        entry.name_len = (uint16_t)fullname_len;

        // 8 entries per 64-byte slot, 8 fit per sector → never crosses (we could allow more but who's really gonna put more than 632 ISOs on their stick?)
        unsigned int ent_byte_off = new_idx * ES_ENTRY_SIZE;
        unsigned int ent_sec = 1 + (ent_byte_off / 512);
        unsigned int ent_sec_off = ent_byte_off % 512;
        unsigned char ent_buf[512] __attribute__((aligned(64)));
        devctl_read_sectors(info.partition_start_sector + ent_sec, ent_buf, 1);
        memcpy(ent_buf + ent_sec_off, &entry, ES_ENTRY_SIZE);
        devctl_write_sectors(info.partition_start_sector + ent_sec, ent_buf, 1);

        sb->iso_count = new_idx + 1;
        sb->pool_used_bytes = pool_used + fullname_len + 1;  // +1 for NUL
        write_superblock_with_backup(info.partition_start_sector, sb_buf);
        info.free_sector += needed_sectors;

        sceIoRemove(path);

        pspDebugScreenSetTextColor(0xFF00FF00);
        printf(" OK\n");
        pspDebugScreenSetTextColor(0xFFFFFFFF);
        transferred++;
    }

    scePowerUnlock(0);
    free(buf);
    devctl_rescan();

    printf("\n");
    pspDebugScreenSetTextColor(0xFF00FF00);
    printf("Done! %d transferred", transferred);
    if (failed > 0) {
        pspDebugScreenSetTextColor(0xFF0000FF);
        printf(", %d failed", failed);
    }
    printf("\n");
    pspDebugScreenSetTextColor(0xFFFFFFFF);
    printf("\nPress X to return.\n");
    wait_cross();
}

static void transfer_all_from_es(void)
{
    pspDebugScreenClear();
    pspDebugScreenSetTextColor(0xFF00FFFF);
    printf("=== Transfer All ISOs -> Memory Stick ===\n\n");
    pspDebugScreenSetTextColor(0xFFFFFFFF);

    ExtremeSpeedInfo es_info;
    if (devctl_get_info(&es_info) < 0 || !es_info.initialized || es_info.iso_count == 0) {
        printf("No ExtremeSpeed ISOs found.\n");
        printf("\nPress X to return.\n");
        wait_cross();
        return;
    }

    unsigned int table_sectors = (es_info.iso_count * ES_ENTRY_SIZE + 511) / 512;
    unsigned char *table_buf = malloc(table_sectors * 512);
    if (!table_buf) { printf("Out of memory.\n"); wait_cross(); return; }

    unsigned int s;
    for (s = 0; s < table_sectors; s++)
        devctl_read_sectors(es_info.partition_start_sector + 1 + s, table_buf + s * 512, 1);
    pool_refresh(es_info.partition_start_sector);

    int active_count = 0;
    unsigned int idx;
    for (idx = 0; idx < es_info.iso_count; idx++) {
        const ExtremeSpeedEntry *ent = (const ExtremeSpeedEntry *)(table_buf + idx * ES_ENTRY_SIZE);
        if (ent->flags & ES_FLAG_ACTIVE) active_count++;
    }

    if (active_count == 0) {
        printf("No active ISOs.\n");
        free(table_buf);
        printf("\nPress X to return.\n");
        wait_cross();
        return;
    }

    printf("Found %d ISOs on ExtremeSpeed. Transfer all back?\n", active_count);
    printf("Press START to begin, O to cancel.\n\n");

    while (!exit_requested) {
        SceCtrlData pad;
        sceCtrlReadBufferPositive(&pad, 1);
        if (pad.Buttons & PSP_CTRL_CIRCLE) { free(table_buf); return; }
        if (pad.Buttons & PSP_CTRL_START) { wait_release(PSP_CTRL_START); break; }
        sceDisplayWaitVblankStart();
    }

    void *buf = malloc(XFER_BUF_SIZE);
    if (!buf) { free(table_buf); printf("Out of memory.\n"); wait_cross(); return; }

    scePowerLock(0);

    int transferred = 0, failed = 0;
    int fi_num = 0;
    for (idx = 0; idx < es_info.iso_count && !exit_requested; idx++) {
        ExtremeSpeedEntry *ent = (ExtremeSpeedEntry *)(table_buf + idx * ES_ENTRY_SIZE);
        if (!(ent->flags & ES_FLAG_ACTIVE)) continue;
        fi_num++;

        printf("  [%d/%d] %s (%lu MB)...", fi_num, active_count,
               pool_get(ent), ent->size_lo / (1024*1024));

        char dest[300];
        snprintf(dest, sizeof(dest), "ms0:/%s", pool_get(ent));

        SceUID dst = sceIoOpen(dest, PSP_O_WRONLY | PSP_O_CREAT | PSP_O_TRUNC, 0777);
        if (dst < 0) { printf(" create fail\n"); failed++; continue; }

        unsigned int src_sector = es_info.partition_start_sector + ent->start_sector;
        unsigned int remaining = ent->size_lo;
        int error = 0;

        while (remaining > 0 && !exit_requested) {
            unsigned int chunk = XFER_BUF_SIZE;
            if (chunk > remaining) chunk = remaining;
            unsigned int read_sectors = (chunk + 511) / 512;
            unsigned int rs = 0;
            while (rs < read_sectors) {
                unsigned int batch = read_sectors - rs;
                if (batch > 128) batch = 128;
                int rr = devctl_read_sectors(src_sector + rs, (char *)buf + rs * 512, batch);
                if (rr < 0) { error = 1; break; }
                rs += batch;
            }
            if (error) break;
            src_sector += read_sectors;
            int wr = sceIoWrite(dst, buf, chunk);
            if (wr < (int)chunk) { error = 2; break; }
            remaining -= chunk;
        }
        sceIoClose(dst);

        if (error) { printf(" FAILED\n"); failed++; continue; }

        // mark es entry inactive
        ent->flags &= ~ES_FLAG_ACTIVE;
        unsigned int ent_byte_off = idx * ES_ENTRY_SIZE;
        unsigned int ent_sec = 1 + ent_byte_off / 512;
        devctl_write_sectors(es_info.partition_start_sector + ent_sec,
                              table_buf + (ent_sec - 1) * 512, 1);
        if ((ent_byte_off % 512) + ES_ENTRY_SIZE > 512)
            devctl_write_sectors(es_info.partition_start_sector + ent_sec + 1,
                                  table_buf + ent_sec * 512, 1);

        pspDebugScreenSetTextColor(0xFF00FF00);
        printf(" OK\n");
        pspDebugScreenSetTextColor(0xFFFFFFFF);
        transferred++;
    }

    scePowerUnlock(0);
    free(buf);
    free(table_buf);
    devctl_rescan();

    printf("\n");
    pspDebugScreenSetTextColor(0xFF00FF00);
    printf("Done! %d transferred", transferred);
    if (failed > 0) {
        pspDebugScreenSetTextColor(0xFF0000FF);
        printf(", %d failed", failed);
    }
    printf("\n");
    pspDebugScreenSetTextColor(0xFFFFFFFF);
    printf("\nPress X to return.\n");
    wait_cross();
}

// lets do a reclaim function
// ...though it's more like a defragmentation crawler...
// not intending ES ISOs to be moved around that much (inb4 the flood of gh issues)
static void compact_es(void)
{
    pspDebugScreenClear();
    pspDebugScreenSetTextColor(0xFF00FFFF);
    printf("=== Reclaim ES Space ===\n\n");
    pspDebugScreenSetTextColor(0xFFFFFFFF);

    ExtremeSpeedInfo es_info;
    if (devctl_get_info(&es_info) < 0 || !es_info.initialized) {
        pspDebugScreenSetTextColor(0xFF0000FF);
        printf("ExtremeSpeed partition not detected.\n");
        printf("\nPress X to return.\n");
        wait_cross();
        return;
    }
    if (es_info.iso_count == 0) {
        printf("No ISOs on ExtremeSpeed partition.\n");
        printf("\nPress X to return.\n");
        wait_cross();
        return;
    }

    unsigned int table_sectors = (es_info.iso_count * ES_ENTRY_SIZE + 511) / 512;
    unsigned char *table_buf = malloc(table_sectors * 512);
    if (!table_buf) { printf("Out of memory.\n"); wait_cross(); return; }
    unsigned int s;
    for (s = 0; s < table_sectors; s++)
        devctl_read_sectors(es_info.partition_start_sector + 1 + s, table_buf + s * 512, 1);
    pool_refresh(es_info.partition_start_sector);

    int active[ES_MAX_ISOS];
    int active_count = 0;
    int idx;
    for (idx = 0; idx < (int)es_info.iso_count; idx++) {
        ExtremeSpeedEntry *ent = (ExtremeSpeedEntry *)(table_buf + idx * ES_ENTRY_SIZE);
        if (ent->flags & ES_FLAG_ACTIVE)
            active[active_count++] = idx;
    }
    int ai, aj;
    for (ai = 1; ai < active_count; ai++) {
        int key = active[ai];
        ExtremeSpeedEntry *ka = (ExtremeSpeedEntry *)(table_buf + key * ES_ENTRY_SIZE);
        aj = ai - 1;
        while (aj >= 0) {
            ExtremeSpeedEntry *ja = (ExtremeSpeedEntry *)(table_buf + active[aj] * ES_ENTRY_SIZE);
            if (ja->start_sector <= ka->start_sector) break;
            active[aj + 1] = active[aj];
            aj--;
        }
        active[aj + 1] = key;
    }

    // !NOTE: remove this later! if we need to (logging)
    printf("iso_count=%lu, active=%d, data_start=%lu, free_sector=%lu\n",
           es_info.iso_count, active_count, es_info.data_start_sector, es_info.free_sector);
    for (ai = 0; ai < active_count; ai++) {
        ExtremeSpeedEntry *ent = (ExtremeSpeedEntry *)(table_buf + active[ai] * ES_ENTRY_SIZE);
        printf("  [%d] idx=%d start=%lu size=%lu %s\n",
               ai, active[ai], ent->start_sector, ent->size_sectors, pool_get(ent));
    }

    // check if compaction is needed
    unsigned int next_free = es_info.data_start_sector;
    int needs_compact = 0;
    for (ai = 0; ai < active_count; ai++) {
        ExtremeSpeedEntry *ent = (ExtremeSpeedEntry *)(table_buf + active[ai] * ES_ENTRY_SIZE);
        if (ent->start_sector != next_free) { needs_compact = 1; break; }
        next_free += ent->size_sectors;
    }
    if (!needs_compact) {
        pspDebugScreenSetTextColor(0xFF00FF00);
        printf("\nPartition is already compact. No gaps found.\n");
        free(table_buf);
        printf("\nPress X to return.\n");
        wait_cross();
        return;
    }

    // show what will happen
    unsigned int total_used = 0;
    for (ai = 0; ai < active_count; ai++) {
        ExtremeSpeedEntry *ent = (ExtremeSpeedEntry *)(table_buf + active[ai] * ES_ENTRY_SIZE);
        total_used += ent->size_sectors;
    }
    unsigned int gap_sectors = (es_info.free_sector - es_info.data_start_sector) - total_used;
    printf("%d active ISOs, ", active_count);
    print_size(gap_sectors);
    printf(" of gaps to reclaim.\n\n");

    pspDebugScreenSetTextColor(0xFF00FFFF);
    printf("Press START to compact, or O to cancel.\n");
    pspDebugScreenSetTextColor(0xFFFFFFFF);

    while (!exit_requested) {
        SceCtrlData pad;
        sceCtrlReadBufferPositive(&pad, 1);
        if (pad.Buttons & PSP_CTRL_CIRCLE) { free(table_buf); return; }
        if (pad.Buttons & PSP_CTRL_START) { wait_release(PSP_CTRL_START); break; }
        sceDisplayWaitVblankStart();
    }

    // allocate transfer buffer
    void *buf = malloc(XFER_BUF_SIZE);
    if (!buf) { printf("Out of memory.\n"); free(table_buf); wait_cross(); return; }

    pspDebugScreenClear();
    printf("Compacting ExtremeSpeed partition...\n\n");

    next_free = es_info.data_start_sector;
    for (ai = 0; ai < active_count; ai++) {
        ExtremeSpeedEntry *ent = (ExtremeSpeedEntry *)(table_buf + active[ai] * ES_ENTRY_SIZE);
        unsigned int src = ent->start_sector;
        unsigned int dst = next_free;
        unsigned int sectors_left = ent->size_sectors;

        printf("  [%d/%d] %s", ai + 1, active_count, pool_get(ent));
        if (src == dst) {
            printf(" — already in place\n");
            next_free += sectors_left;
            continue;
        }
        printf("\n");

        unsigned int moved = 0;
        while (sectors_left > 0) {
            unsigned int batch = sectors_left;
            if (batch > 256) batch = 256;

            unsigned int rs = 0;
            while (rs < batch) {
                unsigned int rb = batch - rs;
                if (rb > 128) rb = 128;
                devctl_read_sectors(es_info.partition_start_sector + src + moved + rs,
                                    (char *)buf + rs * 512, rb);
                rs += rb;
            }
            // write to desty
            unsigned int ws = 0;
            while (ws < batch) {
                unsigned int wb = batch - ws;
                if (wb > 128) wb = 128;
                devctl_write_sectors(es_info.partition_start_sector + dst + moved + ws,
                                     (char *)buf + ws * 512, wb);
                ws += wb;
            }

            moved += batch;
            sectors_left -= batch;

            unsigned int pct = (unsigned int)((unsigned long long)moved * 100 / ent->size_sectors);
            pspDebugScreenSetXY(4, (int)(7 + ai));
            printf("%u%%  ", pct);
        }
        pspDebugScreenSetXY(4, (int)(7 + ai));
        printf("done\n");

        // hmm do the write now just in case power failure
        ent->start_sector = dst;
        unsigned int entry_byte_off = (unsigned int)active[ai] * ES_ENTRY_SIZE;
        unsigned int entry_sec_start = entry_byte_off / 512;
        unsigned int entry_sec_end = (entry_byte_off + ES_ENTRY_SIZE - 1) / 512;
        unsigned int es;
        for (es = entry_sec_start; es <= entry_sec_end; es++)
            devctl_write_sectors(es_info.partition_start_sector + 1 + es,
                                 table_buf + es * 512, 1);

        next_free += ent->size_sectors;
    }

    unsigned char sb_buf[512] __attribute__((aligned(64)));
    devctl_read_sectors(es_info.partition_start_sector, sb_buf, 1);
    ExtremeSpeedSuperblock *sb = (ExtremeSpeedSuperblock *)sb_buf;
    sb->iso_count = active_count;
    write_superblock_with_backup(es_info.partition_start_sector, sb_buf);

    { int needs_repack = 0; for (ai = 0; ai < active_count; ai++) { if (active[ai] != ai) { needs_repack = 1; break; } } if (needs_repack) {
        unsigned int new_table_size = (active_count * ES_ENTRY_SIZE + 511) / 512 * 512;
        unsigned char *new_table = malloc(new_table_size);
        if (new_table) {
            memset(new_table, 0, new_table_size);
            for (ai = 0; ai < active_count; ai++)
                memcpy(new_table + ai * ES_ENTRY_SIZE,
                       table_buf + active[ai] * ES_ENTRY_SIZE, ES_ENTRY_SIZE);
            unsigned int new_secs = (active_count * ES_ENTRY_SIZE + 511) / 512;
            for (s = 0; s < new_secs; s++)
                devctl_write_sectors(es_info.partition_start_sector + 1 + s,
                                     new_table + s * 512, 1);
            free(new_table);
        }
    }}

    free(buf);
    free(table_buf);

    // rescan so driver picks up changes
    devctl_rescan();

    pspDebugScreenSetTextColor(0xFF00FF00);
    printf("\nCompaction complete! Reclaimed ");
    print_size(gap_sectors);
    printf(" of space.\n");
    printf("\nPress X to return.\n");
    pspDebugScreenSetTextColor(0xFFFFFFFF);
    wait_cross();
}

static char eboot_path[512];

// format stuffs

static void __attribute__((unused)) create_sony_dirs(void)
{
    // standard psp directory structure created by xmb after format
    sceIoMkdir("ms0:/PSP", 0777);
    sceIoMkdir("ms0:/PSP/GAME", 0777);
    sceIoMkdir("ms0:/PSP/SAVEDATA", 0777);
    sceIoMkdir("ms0:/PSP/COMMON", 0777);
    sceIoMkdir("ms0:/PSP/SYSTEM", 0777);
    sceIoMkdir("ms0:/ISO", 0777);
    sceIoMkdir("ms0:/ISO/VIDEO", 0777);
    sceIoMkdir("ms0:/SEPLUGINS", 0777);
    sceIoMkdir("ms0:/MUSIC", 0777);
    sceIoMkdir("ms0:/VIDEO", 0777);
    sceIoMkdir("ms0:/PICTURE", 0777);
}

static void format_memstick(void)
{
    pspDebugScreenClear();
    pspDebugScreenSetTextColor(0xFF00FFFF);
    printf("=== Format Memory Stick ===\n\n");
    pspDebugScreenSetTextColor(0xFFFFFFFF);

    const char *fmt_options[] = {
        "FAT32  (32KB clusters, max 4GB files)",
        "exFAT  (128KB clusters, no size limit)"
    };
    int fmt_sel = 0;
    int fmt_exfat = -1;

    while (!exit_requested) {
        pspDebugScreenSetXY(0, 4);
        printf("Select filesystem:\n\n");
        int fi;
        for (fi = 0; fi < 2; fi++) {
            if (fi == fmt_sel)
                pspDebugScreenSetTextColor(0xFF00FF00);
            else
                pspDebugScreenSetTextColor(0xFFFFFFFF);
            printf("  %s %s\n", (fi == fmt_sel) ? ">" : " ", fmt_options[fi]);
        }
        pspDebugScreenSetTextColor(0xFF808080);
        printf("\n  UP/DOWN navigate, X select, O cancel\n");
        pspDebugScreenSetTextColor(0xFFFFFFFF);

        sceDisplayWaitVblankStart();
        SceCtrlData pad;
        sceCtrlReadBufferPositive(&pad, 1);
        if (pad.Buttons & PSP_CTRL_UP) { if (fmt_sel > 0) fmt_sel--; wait_release(PSP_CTRL_UP); }
        if (pad.Buttons & PSP_CTRL_DOWN) { if (fmt_sel < 1) fmt_sel++; wait_release(PSP_CTRL_DOWN); }
        if (pad.Buttons & PSP_CTRL_CIRCLE) return;
        if (pad.Buttons & PSP_CTRL_CROSS) { fmt_exfat = fmt_sel; wait_release(PSP_CTRL_CROSS); break; }
    }
    if (fmt_exfat < 0) return;

    // confirmation
    pspDebugScreenClear();
    pspDebugScreenSetTextColor(0xFF0000FF);
    printf("!!! WARNING !!!\n\n");
    pspDebugScreenSetTextColor(0xFFFFFFFF);
    printf("This will FORMAT the memory stick as %s.\n\n",
           fmt_exfat ? "exFAT (128KB clusters)" : "FAT32 (32KB clusters)");
    printf("ALL DATA ON THE MEMORY STICK WILL BE ERASED!\n");
    printf("This includes games, saves, ISOs, and the ES partition.\n\n");
    pspDebugScreenSetTextColor(0xFF00FFFF);
    printf("Press START to confirm, or O to cancel.\n");
    pspDebugScreenSetTextColor(0xFFFFFFFF);

    while (!exit_requested) {
        SceCtrlData pad;
        sceCtrlReadBufferPositive(&pad, 1);
        if (pad.Buttons & PSP_CTRL_CIRCLE) return;
        if (pad.Buttons & PSP_CTRL_START) { wait_release(PSP_CTRL_START); break; }
        sceDisplayWaitVblankStart();
    }

    pspDebugScreenClear();
    printf("Formatting as %s...\n", fmt_exfat ? "exFAT" : "FAT32");

    // back up our own eboot.pbp into ram before format erases it
    void *eboot_buf = NULL;
    int eboot_size = 0;
    if (eboot_path[0]) {
        SceUID ef = sceIoOpen(eboot_path, PSP_O_RDONLY, 0);
        if (ef >= 0) {
            eboot_size = (int)sceIoLseek(ef, 0, PSP_SEEK_END);
            sceIoLseek(ef, 0, PSP_SEEK_SET);
            if (eboot_size > 0 && eboot_size < 4 * 1024 * 1024) {
                eboot_buf = malloc(eboot_size);
                if (eboot_buf) {
                    int rd = sceIoRead(ef, eboot_buf, eboot_size);
                    if (rd != eboot_size) { free(eboot_buf); eboot_buf = NULL; }
                }
            }
            sceIoClose(ef);
        }
    }
    if (eboot_buf)
        printf("Backed up EBOOT.PBP (%d bytes)\n", eboot_size);

    // save es partition mbr entry before format (format rewrites entire mbr)
    unsigned char es_mbr_entry[16];
    int had_es = 0;
    int es_mbr_slot = -1;
    {
        int r = devctl_read_sectors(0, sector_buf, 1);
        if (r >= 0 && sector_buf[510] == 0x55 && sector_buf[511] == 0xAA) {
            int pi;
            for (pi = 0; pi < 4; pi++) {
                int off = 0x1BE + pi * 16;
                if (sector_buf[off + 4] == ES_PARTITION_TYPE) {
                    memcpy(es_mbr_entry, sector_buf + off, 16);
                    es_mbr_slot = pi;
                    had_es = 1;
                    printf("ES partition preserved (slot %d)\n", pi);
                    break;
                }
            }
        }
    }

    // pass format type to driver: 0=fat32, 1=exfat (should we support fat16??)
    int ret = sceIoDevctl("fatms0:", ES_DEVCTL_FORMAT, &fmt_exfat, 4, NULL, 0);
    if (ret < 0) {
        pspDebugScreenSetTextColor(0xFF0000FF);
        printf("\nFormat FAILED (0x%08X)\n", ret);
        if (eboot_buf) free(eboot_buf);
        printf("\nPress X to return.\n");
        wait_cross();
        return;
    }

    // wait for ms0: to become available after format reinit. the driver fires remove+insert events and resets fs_initialized=0.
    // we need to wait for the media thread to reassign ms0: and for deferred to run on the next io call
    printf("Waiting for remount...\n");
    {
        int attempts, mounted = 0;
        for (attempts = 0; attempts < 60; attempts++) {
            sceKernelDelayThread(500000); // 500ms
            SceUID td = sceIoDopen("ms0:/");
            if (td >= 0) {
                sceIoDclose(td);
                mounted = 1;
                break;
            }
        }
        if (!mounted)
            printf("Warning: ms0: not ready after 30s\n");
    }

    // restore es partition entry in mbr if it existed before format. we saved the full original mbr before format. the format wrote a new mbr with just the fs partition. we patch the saved mbr to update the fs partition size (format may have changed it) while keeping the es entry intact, then write it back. we use the saved mbr (pre-format) as the base because es devctl reads are unreliable during the post-format reinit race.
    if (had_es && es_mbr_slot >= 0) {
        printf("Restoring ES partition...\n");
        // invalidate es's cached blk_fd, it gets stale after format reinit, wait for driver to stabilize, then sync'er up bud
        sceKernelDelayThread(2000000); // 2 seconds for driver reinit (lets lower this if possible)
        devctl_sync_fd();

        // rremake the mbr
        unsigned int es_lba = es_mbr_entry[8] | (es_mbr_entry[9]<<8) |
                               (es_mbr_entry[10]<<16) | (es_mbr_entry[11]<<24);

        memset(sector_buf, 0, 512);
        // es partition in its original slot FIRST (so fs doesn't overwrite it if slot 0)
        memcpy(sector_buf + 0x1BE + es_mbr_slot * 16, es_mbr_entry, 16);
        // fs partition in a slot that doesn't conflict with es
        int fs_slot = (es_mbr_slot == 0) ? 1 : 0;
        unsigned int fs_start = 2048; // standard alignment, format always uses this
        unsigned int fs_sectors = es_lba - fs_start;
        sector_buf[0x1BE + fs_slot * 16] = 0x80;
        sector_buf[0x1BE + fs_slot * 16 + 1] = 0xFE; sector_buf[0x1BE + fs_slot * 16 + 2] = 0xFF; sector_buf[0x1BE + fs_slot * 16 + 3] = 0xFF;
        sector_buf[0x1BE + fs_slot * 16 + 4] = fmt_exfat ? 0x07 : 0x0C;
        sector_buf[0x1BE + fs_slot * 16 + 5] = 0xFE; sector_buf[0x1BE + fs_slot * 16 + 6] = 0xFF; sector_buf[0x1BE + fs_slot * 16 + 7] = 0xFF;
        memcpy(sector_buf + 0x1BE + fs_slot * 16 + 8, &fs_start, 4);
        memcpy(sector_buf + 0x1BE + fs_slot * 16 + 12, &fs_sectors, 4);
        sector_buf[510] = 0x55; sector_buf[511] = 0xAA;

        // wait for driver to stabilize again , then write mbr
        sceKernelDelayThread(2000000);
        sceKernelDcacheWritebackInvalidateAll();
        int wr = devctl_write_sectors(0, sector_buf, 1);
        if (wr >= 0) {
            printf("  MBR restored with ES partition\n");
        } else {
            pspDebugScreenSetTextColor(0xFF0000FF);
            printf("  MBR write FAILED: %d\n", wr);
            pspDebugScreenSetTextColor(0xFFFFFFFF);
        }

        // rescan so driver picks up es partition
        devctl_rescan();
        sceKernelDelayThread(500000);

        ExtremeSpeedInfo verify_info;
        if (devctl_get_info(&verify_info) >= 0 && verify_info.initialized) {
            printf("  ES partition verified: %lu ISOs\n", verify_info.iso_count);
        } else {
            pspDebugScreenSetTextColor(0xFF0000FF);
            printf("  WARNING: ES not detected after restore\n");
            pspDebugScreenSetTextColor(0xFFFFFFFF);
        }
    }

    // create standard psp directories
    printf("Creating directories...\n");
    int mr;
    mr = sceIoMkdir("ms0:/PSP", 0777);
    printf("  PSP: %s\n", mr >= 0 ? "OK" : "FAIL");
    mr = sceIoMkdir("ms0:/PSP/GAME", 0777);
    printf("  PSP/GAME: %s\n", mr >= 0 ? "OK" : "FAIL");
    sceIoMkdir("ms0:/PSP/SAVEDATA", 0777);
    sceIoMkdir("ms0:/PSP/COMMON", 0777);
    sceIoMkdir("ms0:/PSP/SYSTEM", 0777);
    sceIoMkdir("ms0:/ISO", 0777);
    sceIoMkdir("ms0:/ISO/VIDEO", 0777);
    sceIoMkdir("ms0:/SEPLUGINS", 0777);
    sceIoMkdir("ms0:/MUSIC", 0777);
    sceIoMkdir("ms0:/VIDEO", 0777);
    sceIoMkdir("ms0:/PICTURE", 0777);

    // restore our eboot.pbp so the app survives format
    if (eboot_buf) {
        // derive directory from eboot_path by finding last '/'
        char eboot_dir[512];
        {
            int len = 0, last_slash = -1;
            while (eboot_path[len]) { if (eboot_path[len] == '/') last_slash = len; len++; }
            if (last_slash > 0) { memcpy(eboot_dir, eboot_path, last_slash); eboot_dir[last_slash] = '\0'; }
            else eboot_dir[0] = '\0';
        }
        if (eboot_dir[0]) sceIoMkdir(eboot_dir, 0777);
        SceUID wf = sceIoOpen(eboot_path,
                              PSP_O_WRONLY | PSP_O_CREAT | PSP_O_TRUNC, 0777);
        if (wf >= 0) {
            sceIoWrite(wf, eboot_buf, eboot_size);
            sceIoClose(wf);
            printf("Restored EBOOT.PBP\n");
        }
        free(eboot_buf);
    }

    devctl_rescan();

    pspDebugScreenSetTextColor(0xFF00FF00);
    printf("\nFormat complete! Memory stick formatted as %s.\n",
           fmt_exfat ? "exFAT" : "FAT32");
    printf("Standard PSP directories created.\n");
    printf("\nPress X to return.\n");
    wait_cross();
}

// flash0 install/uninstall helpers

// copy file from src to dst, returns bytes written or -1 on error
static int copy_file(const char *src_path, const char *dst_path)
{
    static unsigned char copy_buf[16 * 1024];
    SceUID fdr = sceIoOpen(src_path, PSP_O_RDONLY, 0);
    SceUID fdw = sceIoOpen(dst_path,
                           PSP_O_WRONLY | PSP_O_CREAT | PSP_O_TRUNC, 0777);
    if (fdr < 0 || fdw < 0) {
        if (fdr >= 0) sceIoClose(fdr);
        if (fdw >= 0) sceIoClose(fdw);
        return -1;
    }
    int total = 0;
    while (1) {
        int rd = sceIoRead(fdr, copy_buf, sizeof(copy_buf));
        if (rd <= 0) break;
        sceIoWrite(fdw, copy_buf, rd);
        total += rd;
    }
    sceIoClose(fdr);
    sceIoClose(fdw);
    return total;
}

// get eboot directory prefix length (we should use the existing function but lets keep it specific for now)
static int eboot_dir_len(void)
{
    int len = 0, last_slash = -1;
    while (eboot_path[len]) { if (eboot_path[len] == '/') last_slash = len; len++; }
    return (last_slash >= 0) ? last_slash + 1 : -1;
}

// use a hybrid vsh/kernel flash0 writer (same style of seen used in other flash0 installers)

typedef struct {
    char src[128];
    char dst[128];
} kflash_args_t;

static int kflash_install(const char *src, const char *dst)
{
    int dlen = eboot_dir_len();
    if (dlen < 0) return -1;

    char prx_path[512];
    memcpy(prx_path, eboot_path, dlen);
    memcpy(prx_path + dlen, "kernel_flash.prx", 17);

    kflash_args_t args;
    memset(&args, 0, sizeof(args));
    strncpy(args.src, src, sizeof(args.src) - 1);
    strncpy(args.dst, dst, sizeof(args.dst) - 1);

    SceUID mod = sceKernelLoadModule(prx_path, 0, NULL);
    if (mod < 0) return mod;

    int status = 0;
    sceKernelStartModule(mod, sizeof(args), &args, &status, NULL);

    // stop + unload explicitly either way.
    sceKernelStopModule(mod, 0, NULL, NULL, NULL);
    sceKernelUnloadModule(mod);

    return status;
}

static void install_flash0(void)
{
    pspDebugScreenClear();
    pspDebugScreenSetTextColor(0xFF00FFFF);
    printf("\n  Install DratiniFS to flash0\n\n");

    int dlen = eboot_dir_len();
    if (dlen < 0) {
        pspDebugScreenSetTextColor(0xFF0000FF);
        printf("Error: cannot determine EBOOT directory.\n");
        printf("\nPress X to return.\n");
        wait_cross();
        return;
    }

    char prx_path[512], backup_path[512];
    memcpy(prx_path, eboot_path, dlen);
    memcpy(prx_path + dlen, "dratinifs.prx", 14);
    memcpy(backup_path, eboot_path, dlen);
    memcpy(backup_path + dlen, "backup.prx", 11);

    // verify source exists and get size for display
    SceUID src = sceIoOpen(prx_path, PSP_O_RDONLY, 0);
    if (src < 0) {
        pspDebugScreenSetTextColor(0xFF0000FF);
        printf("Error: cannot open %s\n", prx_path);
        printf("Make sure dratinifs.prx is next to EBOOT.PBP.\n");
        printf("\nPress X to return.\n");
        wait_cross();
        return;
    }
    SceOff file_size = sceIoLseek(src, 0, PSP_SEEK_END);
    sceIoClose(src);

    pspDebugScreenSetTextColor(0xFFFFFFFF);
    printf("Source:  %s\n", prx_path);
    printf("Size:    %d bytes\n", (int)file_size);
    printf("Dest:    flash0:/kd/fatms.prx\n\n");

    pspDebugScreenSetTextColor(0xFF00FFFF);
    printf("This will OVERWRITE flash0:/kd/fatms.prx!\n");
    printf("The original will be backed up to backup.prx.\n\n");
    pspDebugScreenSetTextColor(0xFFFFFFFF);
    printf("Press X to install, O to cancel.\n");

    // wait for confirm or cancel
    for (;;) {
        SceCtrlData pad;
        sceCtrlReadBufferPositive(&pad, 1);
        if (pad.Buttons & PSP_CTRL_CIRCLE) {
            wait_release(PSP_CTRL_CIRCLE);
            return;
        }
        if (pad.Buttons & PSP_CTRL_CROSS) {
            wait_release(PSP_CTRL_CROSS);
            break;
        }
        sceDisplayWaitVblankStart();
    }

    // backup current fatms.prx before overwriting
    printf("\nBacking up flash0:/kd/fatms.prx...\n");
    int backup_bytes = copy_file("flash0:/kd/fatms.prx", backup_path);
    if (backup_bytes <= 0) {
        pspDebugScreenSetTextColor(0xFF0000FF);
        printf("Error: could not backup flash0:/kd/fatms.prx.\n");
        printf("\nPress X to return.\n");
        wait_cross();
        return;
    }
    printf("Backup saved (%d bytes).\n", backup_bytes);

    printf("Invoking kernel writer for flash0...\n");

    int rc = kflash_install(prx_path, "flash0:/kd/fatms.prx");

    if (rc == 0) {
        flash_state = 2;
        backup_found = 1;
        pspDebugScreenSetTextColor(0xFF00FF00);
        printf("\nDratiniFS installed to flash0:/kd/fatms.prx (%d bytes).\n",
               (int)file_size);
        printf("\nPress X to reboot.\n");
        wait_cross();
        scePowerRequestColdReset(0);
    } else {
        pspDebugScreenSetTextColor(0xFF0000FF);
        printf("Error: flash0 write failed: 0x%08X\n", rc);
        printf("If the error starts with 0x8002, ensure kernel_flash.prx\n");
        printf("is sitting next to EBOOT.PBP. Otherwise flash0 may be\n");
        printf("corrupted and should be restored from backup.\n");
        printf("\nPress X to return.\n");
        wait_cross();
    }
}

static void uninstall_flash0(void)
{
    pspDebugScreenClear();
    pspDebugScreenSetTextColor(0xFF00FFFF);
    printf("\n  Uninstall DratiniFS from flash0\n\n");

    int dlen = eboot_dir_len();
    if (dlen < 0) {
        pspDebugScreenSetTextColor(0xFF0000FF);
        printf("Error: cannot determine EBOOT directory.\n");
        printf("\nPress X to return.\n");
        wait_cross();
        return;
    }

    char backup_path[512];
    memcpy(backup_path, eboot_path, dlen);
    memcpy(backup_path + dlen, "backup.prx", 11);

    // verify backup exists and get size
    SceUID bk = sceIoOpen(backup_path, PSP_O_RDONLY, 0);
    if (bk < 0) {
        pspDebugScreenSetTextColor(0xFF0000FF);
        printf("Error: backup.prx not found.\n");
        printf("\nPress X to return.\n");
        wait_cross();
        return;
    }
    SceOff file_size = sceIoLseek(bk, 0, PSP_SEEK_END);
    sceIoClose(bk);

    pspDebugScreenSetTextColor(0xFFFFFFFF);
    printf("Source:  %s\n", backup_path);
    printf("Size:    %d bytes\n", (int)file_size);
    printf("Dest:    flash0:/kd/fatms.prx\n\n");

    pspDebugScreenSetTextColor(0xFF00FFFF);
    printf("This will restore the original fatms.prx.\n\n");
    pspDebugScreenSetTextColor(0xFFFFFFFF);
    printf("Press X to uninstall, O to cancel.\n");

    for (;;) {
        SceCtrlData pad;
        sceCtrlReadBufferPositive(&pad, 1);
        if (pad.Buttons & PSP_CTRL_CIRCLE) {
            wait_release(PSP_CTRL_CIRCLE);
            return;
        }
        if (pad.Buttons & PSP_CTRL_CROSS) {
            wait_release(PSP_CTRL_CROSS);
            break;
        }
        sceDisplayWaitVblankStart();
    }

    printf("\nInvoking kernel writer for flash0...\n");

    int rc = kflash_install(backup_path, "flash0:/kd/fatms.prx");

    if (rc == 0) {
        flash_state = 0;
        pspDebugScreenSetTextColor(0xFF00FF00);
        printf("\nOriginal fatms.prx restored (%d bytes).\n", (int)file_size);
        printf("\nPress X to reboot.\n");
        wait_cross();
        scePowerRequestColdReset(0);
    } else {
        pspDebugScreenSetTextColor(0xFF0000FF);
        printf("Error: flash0 write failed: 0x%08X\n", rc);
        printf("If the error starts with 0x8002, ensure kernel_flash.prx\n");
        printf("is sitting next to EBOOT.PBP.\n");
        printf("\nPress X to return.\n");
        wait_cross();
    }
}

// main menu scaffoldski

int main(int argc, char *argv[])
{
    pspDebugScreenInit();
    sceCtrlSetSamplingCycle(0);
    sceCtrlSetSamplingMode(PSP_CTRL_MODE_ANALOG);
    setup_callbacks();

    // detect our own eboot path from argv[0] which should look something likee "ms0:/psp/game/dratinifsextremespeed/eboot.pbp"
    // just so we can restore it after format
    eboot_path[0] = '\0';
    if (argc > 0 && argv[0]) {
        int len = 0;
        while (argv[0][len] && len < 510) len++;
        memcpy(eboot_path, argv[0], len);
        eboot_path[len] = '\0';
    }

    int sel = 0;

    // check if dratinifs.prx and backup.prx exist beside our eboot
    prx_found = 0; backup_found = 0;
    {
        int dlen = eboot_dir_len();
        if (dlen >= 0) {
            char path[512];
            SceUID fd;
            memcpy(path, eboot_path, dlen);
            memcpy(path + dlen, "dratinifs.prx", 14);
            fd = sceIoOpen(path, PSP_O_RDONLY, 0);
            if (fd >= 0) { prx_found = 1; sceIoClose(fd); }
            memcpy(path + dlen, "backup.prx", 11);
            fd = sceIoOpen(path, PSP_O_RDONLY, 0);
            if (fd >= 0) { backup_found = 1; sceIoClose(fd); }
        }
    }

    // query running driver version via devctl
    // if it responds, DratiniFS is installed; compare against our compiled version
    // 0 = install needed (no dratinifs), 1 = update needed (older version), 2 = up to date
    flash_state = 0;
    unsigned int running_version = 0xFFFFFFFF;
    if (sceIoDevctl("fatms0:", ES_DEVCTL_GET_VERSION, NULL, 0,
                     &running_version, sizeof(running_version)) >= 0
        && running_version != 0xFFFFFFFF) {
        flash_state = (running_version < DRATINIFS_VERSION) ? 1 : 2;
    }

    // build menu dynamically
    #define MAX_MENU 13
    const char *menu_items[MAX_MENU];
    int num_items = 0;
    int idx_install = -1, idx_uninstall = -1, idx_partinfo = -1, idx_partition = -1;
    int idx_list = -1, idx_to_es = -1, idx_from_es = -1;
    int idx_all_to = -1, idx_all_from = -1, idx_delete = -1;
    int idx_compact = -1, idx_format = -1, idx_exit = -1;

    // cache es state - only refresh after returning from submenus
    int es_detected = 0;
    { ExtremeSpeedInfo _ei;
      es_detected = (devctl_get_info(&_ei) >= 0 && _ei.initialized);
    }

    #define REBUILD_MENU() do { \
        num_items = 0; \
        idx_install = idx_uninstall = idx_partinfo = idx_partition = -1; \
        idx_list = idx_to_es = idx_from_es = -1; \
        idx_all_to = idx_all_from = idx_delete = -1; \
        idx_compact = idx_format = idx_exit = -1; \
        if (prx_found && flash_state == 0) { idx_install = num_items; menu_items[num_items++] = "Install DratiniFS to flash0"; } \
        else if (prx_found && flash_state == 1) { idx_install = num_items; menu_items[num_items++] = "Update DratiniFS in flash0"; } \
        else if (flash_state == 2 && backup_found) { idx_uninstall = num_items; menu_items[num_items++] = "Uninstall DratiniFS"; } \
        idx_partinfo = num_items; menu_items[num_items++] = "Show Partition Info"; \
        idx_partition = num_items; menu_items[num_items++] = es_detected ? "Delete ExtremeSpeed Partition" : "Create ExtremeSpeed Partition"; \
        idx_list = num_items; menu_items[num_items++] = "List ExtremeSpeed ISOs"; \
        idx_to_es = num_items; menu_items[num_items++] = "Transfer ISO -> ExtremeSpeed"; \
        idx_from_es = num_items; menu_items[num_items++] = "Transfer ES ISO -> Memory Stick"; \
        idx_all_to = num_items; menu_items[num_items++] = "Transfer ALL ISOs -> ExtremeSpeed"; \
        idx_all_from = num_items; menu_items[num_items++] = "Transfer ALL ES ISOs -> Memory Stick"; \
        idx_delete = num_items; menu_items[num_items++] = "Delete ISO from ExtremeSpeed"; \
        idx_compact = num_items; menu_items[num_items++] = "Reclaim ES Space"; \
        idx_format = num_items; menu_items[num_items++] = "Format Memory Stick"; \
        idx_exit = num_items; menu_items[num_items++] = "Exit"; \
    } while (0)

    REBUILD_MENU();

    pspDebugScreenClear();

    while (!exit_requested) {
        // redraw in-place so we don't flicker
        pspDebugScreenSetXY(0, 0);
        pspDebugScreenSetTextColor(0xFF00FFFF);
        printf("\n  ExtremeSpeed Manager\n");
        pspDebugScreenSetTextColor(0xFF808080);
        printf("  Zero-overhead ISO partition for DratiniFS\n\n");
        pspDebugScreenSetTextColor(0xFFFFFFFF);

        int i;
        for (i = 0; i < num_items; i++) {
            if (i == sel)
                pspDebugScreenSetTextColor(0xFF00FF00);
            else
                pspDebugScreenSetTextColor(0xFFFFFFFF);
            printf("  %s %d. %s\n", (i == sel) ? ">" : " ", i + 1, menu_items[i]);
        }

        pspDebugScreenSetTextColor(0xFF808080);
        printf("\n  UP/DOWN navigate, X select\n");
        pspDebugScreenSetTextColor(0xFFFFFFFF);

        sceDisplayWaitVblankStart();

        SceCtrlData pad;
        sceCtrlReadBufferPositive(&pad, 1);
        if (pad.Buttons & PSP_CTRL_UP) { if (sel > 0) sel--; wait_release(PSP_CTRL_UP); }
        if (pad.Buttons & PSP_CTRL_DOWN) { if (sel < num_items - 1) sel++; wait_release(PSP_CTRL_DOWN); }
        if (pad.Buttons & PSP_CTRL_CROSS) {
            wait_release(PSP_CTRL_CROSS);
            if (sel == idx_install)        install_flash0();
            else if (sel == idx_uninstall) uninstall_flash0();
            else if (sel == idx_partinfo) show_partition_info();
            else if (sel == idx_partition) {
                ExtremeSpeedInfo _ei;
                if (devctl_get_info(&_ei) >= 0 && _ei.initialized)
                    delete_es_partition();
                else
                    create_partition();
            }
            else if (sel == idx_list)     list_isos();
            else if (sel == idx_to_es)    transfer_to_es();
            else if (sel == idx_from_es)  transfer_from_es();
            else if (sel == idx_all_to)   transfer_all_to_es();
            else if (sel == idx_all_from) transfer_all_from_es();
            else if (sel == idx_delete)   delete_iso();
            else if (sel == idx_compact)  compact_es();
            else if (sel == idx_format)   format_memstick();
            else if (sel == idx_exit)     { sceKernelExitGame(); return 0; }

            pspDebugScreenClear();
            // refresh state after submenu
            { ExtremeSpeedInfo _ei;
              es_detected = (devctl_get_info(&_ei) >= 0 && _ei.initialized);
            }
            // refresh flash state (version may have changed after install/uninstall)
            flash_state = 0;
            running_version = 0xFFFFFFFF;
            if (sceIoDevctl("fatms0:", ES_DEVCTL_GET_VERSION, NULL, 0,
                             &running_version, sizeof(running_version)) >= 0
                && running_version != 0xFFFFFFFF) {
                flash_state = (running_version < DRATINIFS_VERSION) ? 1 : 2;
            }
            // refresh backup.prx check
            {
                int dlen = eboot_dir_len();
                backup_found = 0;
                if (dlen >= 0) {
                    char path[512];
                    memcpy(path, eboot_path, dlen);
                    memcpy(path + dlen, "backup.prx", 11);
                    SceUID fd = sceIoOpen(path, PSP_O_RDONLY, 0);
                    if (fd >= 0) { backup_found = 1; sceIoClose(fd); }
                }
            }
            REBUILD_MENU();
            if (sel >= num_items) sel = num_items - 1;
        }
    }

    sceKernelExitGame();
    return 0;
}
