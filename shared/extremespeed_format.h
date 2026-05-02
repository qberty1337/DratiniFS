// extremespeed ISO partition format
// share this between dratini and ES homebrew (PSP) and the PC Rust app.
//
// new layout allows max 632 ISOs:
//   sector 0: [ExtremeSpeedSuperblock] - 512 bytes
//   sectors 1..79: [ExtremeSpeedEntry] x 8 per sector (632 entries total)
//   sectors 80..199: [Name Pool] - NUL-terminated names packed sequentially
//   sector 200: [Superblock backup] - CRC32 protected
//   sectors 201..255:[Reserved] - zero-filled, journal area
//   sector 256+: [ISO data] - packed sequentially
//
// ISOs are laid out contiguously with no gaps
// reading byte offset X from ISO N is pure arithmetic: abs_sector = es_partition_start + entry[N].start_sector + (X / 512)
// new entry table indexing (packed): sector_for_entry(i) = es_partition_start + 1 + (i / 8) and byte_off_in_sector(i) = (i % 8) * 64
// new name pool addressing (maybe make this more efficient in next vers?): abs_byte_off = es_partition_start * 512 + ES_NAME_POOL_START_SECTOR * 512 + entry.name_off

#ifndef EXTREMESPEED_FORMAT_H
#define EXTREMESPEED_FORMAT_H

#include <stdint.h>

// constants

// let's define the block magic that is unique to ES partitions
#define ES_MAGIC 0x44505345  // 'E','S','P','D'
#define ES_VERSION 3
#define ES_SECTOR_SIZE 512
#define ES_PARTITION_TYPE 0xDA

// entry table
#define ES_ENTRIES_PER_SECTOR 8
#define ES_TABLE_FIRST_SECTOR 1
#define ES_TABLE_SECTORS 79
#define ES_MAX_ISOS (ES_TABLE_SECTORS * ES_ENTRIES_PER_SECTOR) // 632 Max ISOs

// name pool
#define ES_NAME_POOL_START_SECTOR 80
#define ES_NAME_POOL_SECTORS 120
#define ES_NAME_POOL_BYTES (ES_NAME_POOL_SECTORS * ES_SECTOR_SIZE)
#define ES_NAME_INVALID_OFF 0xFFFFFFFFu
#define ES_NAME_MAX_LEN 256

// backup superblock + reserved region (lets keep this in line for the future... just in case)
#define ES_BACKUP_SB_SECTOR 200
#define ES_RESERVED_FIRST_SECTOR 201
#define ES_RESERVED_LAST_SECTOR 255

// thne first sector of the ISO data region
#define ES_DATA_START_DEFAULT 256

// struct sizes
#define ES_SUPERBLOCK_SIZE 512
#define ES_ENTRY_SIZE 64

#define ES_GAME_ID_SIZE 16

// entry flags
#define ES_FLAG_ACTIVE 0x01  // ISO exists

// superblock flags
#define ES_FLAG_FROM_UNALLOCATED 0x01  // Partition created from unallocated space (don't expand FS on delete)

// dratinifs version - bump this when releasing new builds
#define DRATINIFS_VERSION_MAJOR 1
#define DRATINIFS_VERSION_MINOR 4
#define DRATINIFS_VERSION ((DRATINIFS_VERSION_MAJOR << 8) | DRATINIFS_VERSION_MINOR)

// devctl command codes (homebrew -> dratinifs driver)
#define ES_DEVCTL_READ_SECTOR 0x04100001
#define ES_DEVCTL_WRITE_SECTOR 0x04100002
#define ES_DEVCTL_GET_INFO 0x04100003
#define ES_DEVCTL_RESCAN 0x04100004
#define ES_DEVCTL_FORMAT 0x04100005  // 0 = FAT32, 1 = exFAT (allow fat16??)
#define ES_DEVCTL_SYNC_FD 0x04100006
#define ES_DEVCTL_GET_VERSION 0x04100007

// superblock struct
typedef struct {
    uint32_t magic;
    uint32_t version;
    uint32_t iso_count; // number of active ISOs in the table
    uint32_t data_start_sector; // sector offset to first ISO data (256 in v3)
    uint32_t total_sectors; // total partition size in sectors (from MBR)
    uint32_t flags; // 0 = from shrink, 1 = from unallocated
    uint32_t checksum; // crc32 of superblock + table, 0 if unchecked
    uint32_t pool_used_bytes; // v3: next-free byte offset in name pool
    uint32_t reserved[120]; // zero-padded to 512 bytes
} __attribute__((packed)) ExtremeSpeedSuperblock;

// ISO table struct
typedef struct {
    char     game_id[ES_GAME_ID_SIZE]; // PARAM.SFO DISC_ID, NUL-padded
    uint32_t start_sector; // partition-relative LBA of ISO data
    uint32_t size_sectors; // ceil(file_size / 512)
    uint32_t size_lo; // file size in bytes (PSP ISO < 2GB)
    uint32_t flags; // ES_FLAG_ACTIVE etc.
    uint32_t name_off; // byte offset into name pool
    uint16_t name_len; // strlen of the name (no NUL)
    uint16_t reserved_short; // pad/future
    uint32_t reserved[6]; // future fields, zero on write
} __attribute__((packed)) ExtremeSpeedEntry;

// devctl argument for sector read/write commands
typedef struct {
    uint32_t sector; // absolute LBA on the physical device
    uint32_t count; // number of sectors to read/write
    void   *buffer; //std buff
} __attribute__((packed)) ExtremeSpeedDevCtlArg;

// devctl response for ES_DEVCTL_GET_INFO
typedef struct {
    uint32_t initialized; // should be1 if ES partition was detected
    uint32_t partition_start_sector; // absolute LBA of ES partition
    uint32_t total_sectors;
    uint32_t iso_count; // active ISOs (can be deactivated)
    uint32_t data_start_sector; // (relative to partition)
    uint32_t free_sector;
} __attribute__((packed)) ExtremeSpeedInfo;

// asserts
#if defined(__GNUC__) || defined(__clang__)
_Static_assert(sizeof(ExtremeSpeedSuperblock) == ES_SUPERBLOCK_SIZE, "ExtremeSpeedSuperblock size mismatch");
_Static_assert(sizeof(ExtremeSpeedEntry) == ES_ENTRY_SIZE, "ExtremeSpeedEntry size mismatch");
_Static_assert((ES_ENTRIES_PER_SECTOR * ES_ENTRY_SIZE) == ES_SECTOR_SIZE, "Entry packing mismatch");
_Static_assert(ES_DATA_START_DEFAULT > ES_BACKUP_SB_SECTOR, "Data region overlaps reserved area");
#endif

#endif
