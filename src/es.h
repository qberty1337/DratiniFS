// extremespeed raw iso partition interface 
// this abstraction layer fixes exfat cold boot from my testing thus far between FAT32 boots vs deferred exFAT mounts (probably should do this a better way eventually)

#ifndef ES_H
#define ES_H

#include <pspiofilemgr.h>
#include "extremespeed_format.h"

// share some globals between our main and es code

int es_get_blk_fd(void);
unsigned long long es_get_blk_fd_pos(void);
void es_set_blk_fd_pos(unsigned long long pos);

// copy these k_sceio functions from iofilemgr_kernel for now
extern long long k_sceIoLseek64k(int fd, int pad, unsigned int offset_lo, unsigned int offset_hi, int whence);
extern int k_sceIoRead(int fd, void *data, unsigned int size);
extern int k_sceIoWrite(int fd, const void *data, unsigned int size);

extern int es_initialized;
extern int es_iso_count;
extern ExtremeSpeedEntry es_table[];
extern char es_dir_paths[][256];

// establish functions for main's use of es
int es_try_open(PspIoDrvFileArg *arg, char *file, void *open_files_ptr, int max_files);

int es_io_read(void *file_ptr, char *data, int len);

int es_try_getstat(const char *file, SceIoStat *stat);

// ioioctl returns 0 for unknown ioctls on es files (should we try and reverse all the ioctls? Not all of them are obvious...)
int es_try_ioctl(int no_fat_chain);

int es_try_remove(const char *file);

// iodread emit overlay entries after real dir ends. returns 1 if entry, 0 if done. *iter tracks position across calls
// ...let's see if we can improve efficiency down the pipeline evbentually
int es_dread_overlay(int fd, SceIoDirent *dir, int *iter);

// check if any active es entry has a filename that starts with "dir/"
int es_check_dir_prefix(const char *dir);

unsigned int es_mbr_find(const unsigned char *mbr_buf, unsigned int *out_num_sectors);

// read es superblock + table from partition (make sure we remember to structure the superblock)
int es_init_partition(unsigned int partition_start);

int es_devctl_read_sector(void *indata, int inlen);
int es_devctl_write_sector(void *indata, int inlen);
int es_devctl_get_info(void *outdata, int outlen);
int es_devctl_rescan(void);
void es_sync_blk_fd(void);

void es_save_dirname(int fd, const char *dirname);

#endif
