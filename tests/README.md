# DratiniFS tests

Two suites that run the **same scenarios** (`common/scenarios.c`): one on the PC against the driver sources in
`src/` (unmodified), and one on a real PSP through the real driver stack. Expected results are Sony 6.61
fatms's own behaviour, so a new DratiniFS feature must leave both suites green.

```
common/   fstest.h + scenarios.c   the shared suite (25 groups: read/write/seek/append/truncate/dirs,
                                   listings, SJIS+UTF-8 names, rename/move, chstat, fragmentation,
                                   300-file dirs, savedata, async, threads, devctls, 40 MB big files)
unit/     test_fatcache.c, test_path.c   small unit tests (FAT window cache, path parsing)
host/     the driver (`src/`) compiled for the PC: PSP SDK shim, kernel stand-ins (hostkern.c: mutexes,
          event flags, threads, codepage driver, msstor RAM disk), iofilemgr emulation (io_host.c),
          host_main.c (golden / full / fill modes), golden/<fat>.txt
psp/      the EBOOT (main.c + io_psp.c + sdkver.S)
out/      scratch output of the host run (images, logs) - safe to delete
```

## Host tests (run after every driver change)

```
sh tests/run_tests.sh            # Git Bash; needs Docker
sh tests/run_tests.sh --regolden # rewrite host/golden/*.txt (only for an intended change)
```

Builds the driver 32-bit (Sony struct layouts need ILP32) in a privileged `alpine:3.20 --platform linux/386`
container (privileged for fuse-exfat on a loop device). The card is a whole-disk image served as msstor0: and
msstor0p1: the way Sony's msstor does: MBR entry 0 = partition 1, and a capacity limit, held in a fake msstor
module mapped at kernel addresses, so the large-card patch has something real to patch. For FAT12, FAT16 and
FAT32 superfloppy images made by `mkfs.fat`:

- **golden**: deterministic run of the suite. The SHA-256 of the resulting image and the card I/O counts
  (`IO rd_calls rd_sectors wr_calls wr_sectors`) must match `host/golden/<fat>.txt`, which locks the on-disk
  layout and the amount of I/O. More I/O than before = a slowdown, even with every check passing.
- **full**: the whole suite in both name encodings, plus kernel hooks (module_reboot_phase, sysevents),
  the codepage tables (the Wipeout Pulse crash), remount, write protect, user-mode pointer checks, and
  interop: files seeded by `mtools` are read by the driver and files written by the driver are read back
  by `mtools`. `fsck.fat` must report the image clean.
- **fill**: full root directory (FAT12/16) and full card (short write, then 0; free space restored).

Then the DratiniFS features:

- **fat32mbr**: the FAT32 run on an MBR-partitioned card. Its golden must equal the superfloppy run plus exactly
  one sector read (the MBR, at insert), which proves FAT behaviour is untouched.
- **bigcard** patch / guard / nolower: the adapter reports half the partition. The driver must raise msstor's size
  and keep it across msstor card setups and re-inserts, and the card must fill to the end. It must leave msstor
  alone when the code isn't Sony 6.61's, and never lower a real Memory Stick's size.
- **exfat**: golden, full (both name encodings, files seeded by fuse-exfat incl. contiguous NoFatChain files and
  Japanese names, exFAT-only checks: growing contiguous files and directories, freeing contiguous files) and fill.
  `fsck.exfat` must find every image clean and fuse-exfat must read back what the driver wrote. (Planting a wrong
  entry checksum or name hash fails both the golden and fsck.)
- **es on fat32 / exfat**: an ES v3 partition after the FS partition, past the adapter's reported size: ES files
  (any case, unaligned reads, past-EOF reads), ES-first reads, writes only to the FS, the listing overlay, ES-only
  directories, remove (FS copy first, then the ES entry, persisted), the 0x041000xx devctls (k1-checked buffers).
- **short FAT**: a FAT32 whose data area has more clusters than its FAT holds (the old ES PC tool's format). The
  driver must cap the cluster count at the FAT's size; before that, XMB's free-space scan hung at boot.

- **PSP Go** (`fstest_host_go`, the sources built with `-DPSPGO` as fatmsef.prx): internal storage is two eflash image
  files beside the Memory Stick.
  - `go`: the scenario suite on `ef0:` and `eh0:`, plus `ms0:` with ES next to them. It checks that volumes stay
    separate (ES only on ms0:), the internal-storage devctls, USB mode's release/reclaim exports, sysevents
    0x400/0x100002 and the reboot phase.
  - `goemu`: a game run from internal storage. `ms0:` must be `ef0:` (same fd, buffers and lock, files visible from
    both), and the Memory Stick is never touched. `goemu-nomsemu` covers the case without msemu's devices.
  - `goformat` / `goformat16`: Sony's internal-storage format byte for byte, on 8 GB and 16 GB flash, and from an
    IdStorage layout (valid and corrupt digest).
  - `fsck.fat` must pass, apart from Sony's blank label and, on `eh0:`, FAT16 bytes on a FAT12-sized volume.

`fstest_host walk <card image> -` mounts any card image and lists, stats and reads everything on it (a real card
dumped with `research/cardscan/dump_card.ps1`, or a replayed format).

Must end with `ALL HOST TESTS PASSED`. If a golden changes, either the change was meant to alter
layout/I/O (then `--regolden` and say why in the commit) or it's a regression.
`FST_FSCK_EACH=1 sh tests/run_tests.sh` runs fsck after every group to find which one breaks the image.

## PSP EBOOT

```
sh tests/psp/build.sh            # -> tests/psp/EBOOT.PBP
```

Copy `EBOOT.PBP` to `ms0:/PSP/GAME/FATMSTEST/` and run it. X starts the test and O exits. Pass 1 runs every group with
SDK 6.60 (UTF-8 names, like retail games) and pass 2 runs the name groups with SDK 3.70 (Shift-JIS names).
It needs about 50 MB free: it writes ~45 MB under `ms0:/FATMSTEST` and removes it at the end (the `big`
group SKIPs if there isn't room). The log is `ms0:/FATMSTEST.LOG`, appended after each group so a crash
still leaves a trail.

- Run it on **stock Sony fatms first**. It must say `ALL CHECKS PASSED` there too, which confirms the
  expectations are Sony's. Then run it on the baseline/DratiniFS build.
- Run it twice, rebooting in between: `ms0:/FATMSPERSIST` holds files written by one run and verified
  by the next (cold read from the card). That folder is kept on purpose.
- Under DratiniFS it starts with **card checks** (skipped on Sony's / the baseline fatms): the partition
  table and FS type, the last sector of every partition read and written back unchanged (past the adapter's
  reported size this needs the large-card patch), and up to four ES ISOs compared with their raw sectors
  (start, unaligned end, past-EOF, the 1992-03-27 date). All of it is read-only apart from that same-bytes
  write-back. The scenario suite then runs on whatever the card is, FAT or exFAT.

## Sony behaviour the suite encodes (don't "fix" these in the driver)

- devctl `0x0241D814` mode 1 ("write-through") loses directory updates: Sony's dirent write-back in that
  mode only writes slots that are already dirty and never marks one dirty. The suite only toggles the mode.
- `sceIoRename`: iofilemgr strips `ms0:` from the new name too (6.61 asm text+0x190c, a `beql` delay slot
  that uofw's C leaves out). A path in another directory stays in the source directory, and a case-only
  rename = EEXIST.
- iofilemgr, not fatms, answers some user-mode calls, and `host/io_host.c` copies it: at most 10 open fds
  per device (fatms `dev_type` 0xa0010) then 0x80020320; devctl cmd bit 15 (e.g. 0x241D814) = 0x800200D1;
  lseek whence > 2 = 0x80020324. The driver's own EMFILE and the 0x241D814 switch only show up for
  kernel callers, so the host tests them with `host_kcall`.
- Short-name aliases follow Sony's rules (`lower.txt` -> `LOWER~1.TXT`, `.hidden` -> `HIDDEN~1`).
- Name encoding comes from the caller's compiled SDK version: < 3.80 Shift-JIS, >= 3.80 UTF-8.

## Adding a scenario

Add a group to `common/scenarios.c`, using only the `fs_*` calls from `fstest.h`. It then runs on both the
host and the PSP. Anything that only the host can arrange (write protect, remount, I/O counting)
goes in `host/host_main.c`. Changing the deterministic suite changes the goldens, so regenerate them on the
known-good driver *before* making the driver change.
