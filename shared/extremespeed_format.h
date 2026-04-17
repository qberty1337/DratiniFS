// extremespeed ISO partition format
//share this between dratini and ES homebrew
//
// layout is like:
// sector 0: [ExtremeSpeedSuperblock] - 512 bytes
// sectors 1..79: [ExtremeSpeedEntry] × iso_count - 1 entry per sector
// sector 80: [Superblock backup] - CRC32 protected
// sectors 81..127: [Reserved] - zero-filled, typical journal stuffs
// sector 128+: [ISO data] - packed sequentially
//
// ISOs are laid out contiguously with no gaps
// reading byte offset X from ISO N is pure arithmetic: abs_sector = es_partition_start + entry[N].start_sector + (X / 512)

#ifndef EXTREMESPEED_FORMAT_H
#define EXTREMESPEED_FORMAT_H

#include <stdint.h>

// constants

// let's define the block magic that is unique to ES partitions
#define ES_MAGIC 0x44505345  // 'E','S','P','D'
#define ES_VERSION 2
#define ES_SECTOR_SIZE 512
#define ES_MAX_ISOS 79
#define ES_PARTITION_TYPE 0xDA
// First sector of ISO data region (64KB from partition start).
// example something like 79 entries × 512 bytes = 79 sectors (1-79). Sector 80 = superblock backup.
// sectors 81-127 reserved. Data starts at sector 128.
#define ES_DATA_START_DEFAULT 128

// struct sizes
#define ES_SUPERBLOCK_SIZE 512
#define ES_ENTRY_SIZE 512

// field sizes
#define ES_FILENAME_SIZE 256
#define ES_GAME_ID_SIZE 16

// entry flags
#define ES_FLAG_ACTIVE 0x01  // ISO exists

// superblock flags
#define ES_FLAG_FROM_UNALLOCATED 0x01  // Partition created from unallocated space (don't expand FS on delete)

// dratinifs version - bump this when releasing new builds
// packed as (major << 8 | minor) for now... i guess i could use major version only integers but this package is quite standard
#define DRATINIFS_VERSION_MAJOR 1
#define DRATINIFS_VERSION_MINOR 3
#define DRATINIFS_VERSION ((DRATINIFS_VERSION_MAJOR << 8) | DRATINIFS_VERSION_MINOR)

// devctl command codes (homebrew -> dratinifs driver)
#define ES_DEVCTL_READ_SECTOR 0x04100001  // read N sectors at absolute LBA
#define ES_DEVCTL_WRITE_SECTOR 0x04100002  // write N sectors at absolute LBA
#define ES_DEVCTL_GET_INFO 0x04100003  // return ES partition info
#define ES_DEVCTL_RESCAN 0x04100004  // re-read MBR + ES superblock
#define ES_DEVCTL_FORMAT 0x04100005  // 0 = FAT32, 1 = exFAT (allow fat16??)
#define ES_DEVCTL_SYNC_FD 0x04100006  // invalidate cached blk_fd in es
#define ES_DEVCTL_GET_VERSION 0x04100007  // return DratiniFS version

// superblock struct
typedef struct {
    uint32_t magic;
    uint32_t version;
    uint32_t iso_count; // number of active ISOs in thetable
    uint32_t data_start_sector; // sector offset to first ISO data (default 128)
    uint32_t total_sectors; // total partition size in sectors (from MBR)
    uint32_t flags; // 0 = created by shrink, 1=created from unallocated
    uint32_t checksum; // crc32 of superblock + table, 0 iss unchecked
    uint32_t reserved[121]; // zero-padded to 512 bytes
} __attribute__((packed)) ExtremeSpeedSuperblock;

// ISO table struct
typedef struct {
    char filename[ES_FILENAME_SIZE]; // relative path ("ISO/game.iso"), padded it nulls
    char game_id[ES_GAME_ID_SIZE];  // PARAM.SFO DISC_ID ("ULUS10041"), also padding it with nulls
    uint32_t start_sector; // absolute sector offset from partition start to ISO data
    uint32_t size_sectors; // number of sectors - ceil(file_size / 512)
    uint32_t size_lo; // file size in bytes, low 32 bits
    uint32_t size_hi; // file size in bytes, high 32 bits (0 for PSP ISOs)
    uint32_t flags;
    uint32_t reserved[55]; // zero-padded to 512 bytes
} __attribute__((packed)) ExtremeSpeedEntry;

// devctl argument for sector read/write commands
typedef struct {
    uint32_t sector; // absolute LBA on the physical device
    uint32_t count; // number of sectors to read/write
    void *buffer; //std buff
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
#endif

#endif