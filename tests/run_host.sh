#!/bin/sh
# Host tests for DratiniFS (the Sony 6.61 fatms baseline + features). Runs inside a 32-bit Linux with gcc, dosfstools and mtools
# (tests/run_tests.sh starts one in docker). For each of FAT12 / FAT16 / FAT32:
#   unit    test_fatcache + test_path
#   golden  deterministic suite on a fresh image; image SHA-256 + I/O counts must match
#           host/golden/<fs>.txt (on-disk layout and amount of card I/O are locked)
#   full    image seeded by mtools -> suite (threads, async, both SDK name modes) + host-only
#           checks -> fsck.fat on the full tree and after cleanup -> mtools reads back survivors
#   fill    root directory full + card full -> fsck.fat
# then the DratiniFS features: fat32mbr (golden = superfloppy + 1 MBR read), bigcard (the large-card patch),
# exfat (golden/full/fill, fsck.exfat + fuse-exfat as oracles), es on fat32 / exfat (ExtremeSpeed).
# Usage: sh run_host.sh [--regolden]
set -u
cd "$(dirname "$0")"
B=../src
O=out
REGOLD=0
[ "${1:-}" = "--regolden" ] && REGOLD=1
FAILS=0
fail() { echo "FAIL: $*"; FAILS=$((FAILS + 1)); }
export MTOOLS_SKIP_CHECK=1

rm -rf "$O" && mkdir -p "$O"
CFLAGS="-std=gnu11 -O1 -g -Wall -Wextra -Wno-unused-parameter -fno-strict-aliasing -DFATMS_HOST -Ihost/shim -I$B -Icommon"
SRC="$B/main.c $B/fatfs_core.c $B/fatfs_path.c $B/fatfs_dirent.c $B/fatfs_ops.c $B/fatfs_read.c $B/fatfs_write.c $B/fatfs_fatcache.c $B/fatfs_devctl.c $B/msstor_cap.c $B/exfat.c $B/es.c $B/format.c"
echo "== build"
gcc $CFLAGS -o "$O/fstest_host" $SRC host/hostkern.c host/io_host.c host/host_main.c common/scenarios.c -lpthread || exit 1
gcc -O2 -Wall -DFATCACHE_HOST -I$B -o "$O/test_fatcache" unit/test_fatcache.c $B/fatfs_fatcache.c || exit 1
gcc -O2 -Wall -DFATCACHE_HOST -I$B -o "$O/test_path" unit/test_path.c $B/fatfs_path.c || exit 1
gcc -O2 -Wall -o "$O/mkimg" host/mkimg.c || exit 1

echo "== unit"
"$O/test_fatcache" | tail -1 || fail "test_fatcache"
"$O/test_path" | tail -1 || fail "test_path"

# fsck.fat -n must report nothing, except two things that are Sony behaviour, not damage:
#  - the FAT32 FSInfo free count is never updated (Sony's fatms doesn't touch FSInfo)
#  - Shift-JIS short names: dosfstools isn't codepage-aware (0x5C is a valid SJIS trail byte)
# and, with a second argument "sonylabel", a third: the PSP Go's internal-storage format (fatmsef
# 0xde00) leaves the boot sector's volume label blank (11 spaces), which dosfstools calls invalid.
# "sonyeh" (eh0:'s format, fatmsef 0xdc44/0xe104) adds a fourth: Sony writes FAT16's first FAT bytes
# (F8 FF FF 7F) on a volume whose cluster count makes it FAT12, so cluster 2 reads as used (0x07F).
fsck_ok() {
    fsck.fat -n "$1" > "$O/fsck.txt" 2>&1; rc=$?
    bad=0; path=""; skip=0
    while IFS= read -r l; do
        case "$l" in
            "fsck.fat "*|Checking*|"Leaving filesystem unchanged."|"$1: "*|"") continue ;;
            *"Free cluster summary"*) skip=1; continue ;;
            *"Label '' stored in boot sector is not valid."*) [ -n "${2:-}" ] && { skip=1; continue; } ;;
            "Reclaimed 1 unused cluster (32768 bytes).") [ "${2:-}" = sonyeh ] && continue ;;
            *"Auto-removing label from boot sector."*) [ $skip = 1 ] && continue ;;
            *"Bad short file name ("*)
                if printf '%s' "$l" | LC_ALL=C grep -q '[^ -~]'; then skip=1; path=""; continue; fi ;;
            *"Auto-correcting."*|*"Auto-renaming it."*|*"Renamed to FSCK"*) [ $skip = 1 ] && continue ;;
            /*) path="$l"; continue ;;
        esac
        [ -n "$path" ] && echo "   $path"
        path=""; skip=0; bad=1
        echo "   $l"
    done < "$O/fsck.txt"
    if [ $bad = 1 ] || [ $rc -gt 1 ]; then fail "fsck.fat $1"; fi
}

seed() {    # seed $img $cs: files written by mtools, read back by the driver (fragmented, LFN, empty)
    d="$O/seed_$1"; mkdir -p "$d"; img="$O/$1.full.img"; cs=$2
    head -c $((5 * cs + 100)) /dev/urandom > "$d/a.bin"
    head -c $((5 * cs + 7)) /dev/urandom > "$d/b.bin"
    head -c $((12 * cs + 300)) /dev/urandom > "$d/c.bin"
    head -c 1234 /dev/urandom > "$d/lfn.bin"
    head -c 777 /dev/urandom > "$d/mixed.bin"
    : > "$d/empty.bin"
    mmd -i "$img" ::/SEED "::/SEED/Long Seed Dir" &&
    mcopy -i "$img" "$d/a.bin" ::/SEED/A.BIN && mcopy -i "$img" "$d/b.bin" ::/SEED/B.BIN &&
    mdel -i "$img" ::/SEED/A.BIN && mcopy -i "$img" "$d/c.bin" ::/SEED/C.BIN &&
    mcopy -i "$img" "$d/lfn.bin" "::/SEED/Long Seed Dir/a seeded long file name.txt" &&
    mcopy -i "$img" "$d/mixed.bin" "::/SEED/MiXeD.TxT" &&
    mcopy -i "$img" "$d/empty.bin" ::/SEED/EMPTY.TXT || { fail "mtools seeding $1"; return; }
    printf 'ms0:/SEED/B.BIN\t%s\nms0:/SEED/C.BIN\t%s\nms0:/SEED/Long Seed Dir/a seeded long file name.txt\t%s\nms0:/SEED/MiXeD.TxT\t%s\nms0:/SEED/EMPTY.TXT\t%s\n' \
        "$d/b.bin" "$d/c.bin" "$d/lfn.bin" "$d/mixed.bin" "$d/empty.bin" > "$d/manifest.txt"
}

for spec in fat12:12:4:2 fat16:16:32:4 fat32:32:48:1; do
    IFS=: read name fat mb spc <<EOF
$spec
EOF
    cs=$((spc * 512))
    echo "== $name ($mb MB, $cs-byte clusters)"
    dd if=/dev/zero of="$O/$name.img" bs=1M count="$mb" 2>/dev/null
    mkfs.fat --invariant -i 12345678 -F "$fat" -s "$spc" "$O/$name.img" >/dev/null || { fail "mkfs $name"; continue; }

    echo "-- golden"
    cp "$O/$name.img" "$O/$name.golden.img"
    if "$O/fstest_host" golden "$O/$name.golden.img" "$O/$name.golden.out.img" > "$O/$name.golden.log" 2>&1; then
        sum=$(sha256sum "$O/$name.golden.out.img" | cut -d' ' -f1)
        io=$(grep '^IO ' "$O/$name.golden.log" | cut -d' ' -f2-)
        g="host/golden/$name.txt"
        if [ $REGOLD = 1 ] || [ ! -f "$g" ]; then
            mkdir -p host/golden && printf 'sha256 %s\nio %s\n' "$sum" "$io" > "$g"; echo "   golden written: $g"
        else
            gsum=$(sed -n 's/^sha256 //p' "$g"); gio=$(sed -n 's/^io //p' "$g")
            [ "$sum" = "$gsum" ] || fail "$name: on-disk result differs from the golden image (layout changed; --regolden if intended)"
            if [ "$io" != "$gio" ]; then
                echo "   card I/O (read calls, sectors, write calls, sectors): now $io, golden $gio"
                fail "$name: amount of card I/O changed (--regolden if intended)"
            fi
        fi
        fsck_ok "$O/$name.golden.out.img"
        grep -E '^(RESULT|IO)' "$O/$name.golden.log" | sed 's/^/   /'
    else
        grep -E 'FAIL|RESULT' "$O/$name.golden.log"; fail "$name golden run"
    fi

    echo "-- full"
    cp "$O/$name.img" "$O/$name.full.img"
    seed "$name" "$cs"
    if "$O/fstest_host" full "$O/$name.full.img" "$O/$name" "$O/seed_$name/manifest.txt" > "$O/$name.full.log" 2>&1; then
        grep -E '^RESULT|SKIP' "$O/$name.full.log" | sed 's/^/   /'
    else
        grep -E 'FAIL|RESULT|unexpected|alias|long name' "$O/$name.full.log"; fail "$name full run"
    fi
    [ -f "$O/$name.keep.img" ] && fsck_ok "$O/$name.keep.img"
    if [ -f "$O/$name.final.img" ]; then
        fsck_ok "$O/$name.final.img"
        n=0
        while IFS="$(printf '\t')" read -r src want; do
            mcopy -n -i "$O/$name.final.img" "$src" "$O/got.bin" 2>/dev/null && cmp -s "$O/got.bin" "$want" ||
                fail "$name: mtools reads '$src' differently from what the driver wrote"
            n=$((n + 1))
        done < "$O/$name.survive.txt"
        echo "   mtools read back $n driver-written files"
    fi

    echo "-- fill"
    cp "$O/$name.img" "$O/$name.fill.img"
    if "$O/fstest_host" fill "$O/$name.fill.img" "$O/$name.fill.out.img" > "$O/$name.fill.log" 2>&1; then
        grep '^RESULT' "$O/$name.fill.log" | sed 's/^/   /'
        fsck_ok "$O/$name.fill.out.img"
    else
        grep -E 'FAIL|RESULT' "$O/$name.fill.log"; fail "$name fill run"
    fi
done

# ---- MBR-partitioned cards (msstor0p1: = MBR entry 0) ----
# mbr <disk> <part image> <lba> <type> [<sectors after the partition>]
mbr() {
    psec=$(( $(stat -c%s "$2") / 512 ))
    "$O/mkimg" disk "$1" $(( $3 + psec + ${5:-0} )) && "$O/mkimg" part "$1" 0 "$4" "$3" "$psec" "$2"
}
echo "== fat32mbr (48 MB FAT32 partition at LBA 2048)"
dd if=/dev/zero of="$O/fat32p.img" bs=1M count=48 2>/dev/null
mkfs.fat --invariant -i 12345678 -F 32 -s 1 -h 2048 "$O/fat32p.img" >/dev/null || fail "mkfs fat32p"
mbr "$O/fat32mbr.img" "$O/fat32p.img" 2048 0x0c || fail "mbr fat32mbr"
echo "-- golden"
cp "$O/fat32mbr.img" "$O/fat32mbr.golden.img"           # the harness works on the image in place
if "$O/fstest_host" golden "$O/fat32mbr.golden.img" "$O/fat32mbr.golden.out.img" > "$O/fat32mbr.golden.log" 2>&1; then
    sum=$(sha256sum "$O/fat32mbr.golden.out.img" | cut -d' ' -f1)
    io=$(grep '^IO ' "$O/fat32mbr.golden.log" | cut -d' ' -f2-)
    g="host/golden/fat32mbr.txt"
    if [ $REGOLD = 1 ] || [ ! -f "$g" ]; then printf 'sha256 %s\nio %s\n' "$sum" "$io" > "$g"; echo "   golden written: $g"
    else
        [ "$sum" = "$(sed -n 's/^sha256 //p' "$g")" ] || fail "fat32mbr: on-disk result differs from the golden image"
        [ "$io" = "$(sed -n 's/^io //p' "$g")" ] || fail "fat32mbr: amount of card I/O changed (now $io)"
    fi
    # same FAT32 run as the superfloppy one, plus the one MBR sector read at insert
    set -- $(sed -n 's/^io //p' host/golden/fat32.txt)
    [ "$io" = "$(($1 + 1)) $(($2 + 1)) $3 $4" ] || fail "fat32mbr: I/O is not the superfloppy run + the MBR read ($io)"
    "$O/mkimg" get "$O/fat32mbr.golden.out.img" 2048 98304 "$O/fat32mbr.part.img" && fsck_ok "$O/fat32mbr.part.img"
    grep -E '^(RESULT|IO)' "$O/fat32mbr.golden.log" | sed 's/^/   /'
else
    grep -E 'FAIL|RESULT' "$O/fat32mbr.golden.log"; fail "fat32mbr golden run"
fi
# the adapter reports half the partition: the driver must raise msstor's size (and must not touch
# msstor when its code isn't Sony's, or lower a real Memory Stick's size)
half=$((2048 + 98304 / 2))
for v in patch:$half guard:$half nolower:$((2048 + 98304 + 5000)); do
    var=${v%%:*}; attr=${v#*:}
    echo "-- bigcard $var"
    cp "$O/fat32mbr.img" "$O/big_$var.img"
    if "$O/fstest_host" bigcard "$O/big_$var.img" "$O/big_$var.out.img" "$attr" "$var" > "$O/big_$var.log" 2>&1; then
        grep '^RESULT' "$O/big_$var.log" | sed 's/^/   /'
        "$O/mkimg" get "$O/big_$var.out.img" 2048 98304 "$O/big_$var.part.img" && fsck_ok "$O/big_$var.part.img"
    else
        grep -E 'FAIL|RESULT' "$O/big_$var.log"; fail "bigcard $var"
    fi
done

# ---- exFAT (DratiniFS): 64 MB partition at LBA 2048, 4 KB clusters ----
# oracles: fsck.exfat (exfatprogs) and fuse-exfat, an independent driver that seeds the card
# (it writes contiguous NoFatChain files) and reads back what the driver wrote
XP=$((64 * 2048))
xfsck() {   # xfsck <disk image>: fsck.exfat on its partition
    "$O/mkimg" get "$1" 2048 $XP "$O/xpart.img" || { fail "extract $1"; return; }
    fsck.exfat -n "$O/xpart.img" > "$O/fsck.txt" 2>&1 || { sed 's/^/   /' "$O/fsck.txt"; fail "fsck.exfat $1"; }
}
xmount() {  # xmount <partition image>: fuse-exfat at $O/mnt
    XL=$(losetup -f) && losetup "$XL" "$1" && mkdir -p "$O/mnt" && mount.exfat-fuse "$XL" "$O/mnt" >/dev/null 2>&1
}
xumount() { umount "$O/mnt"; losetup -d "$XL"; }
xseed() {   # files written by fuse-exfat, read back by the driver (contiguous, fragmented, Japanese, empty)
    d="$O/seed_exfat"; mkdir -p "$d"; cs=4096
    head -c $((5 * cs + 100)) /dev/urandom > "$d/a.bin"; head -c $((5 * cs + 7)) /dev/urandom > "$d/b.bin"
    head -c $((12 * cs + 300)) /dev/urandom > "$d/c.bin"; head -c 1234 /dev/urandom > "$d/lfn.bin"
    head -c 777 /dev/urandom > "$d/mixed.bin"; : > "$d/empty.bin"; head -c 3333 /dev/urandom > "$d/jp.bin"
    m="$O/mnt"
    xmount "$1" || { fail "fuse mount (seeding)"; return; }
    mkdir -p "$m/SEED/Long Seed Dir" "$m/SEED/日本語" &&
    cp "$d/a.bin" "$m/SEED/A.BIN" && cp "$d/b.bin" "$m/SEED/B.BIN" && rm "$m/SEED/A.BIN" &&
    cp "$d/c.bin" "$m/SEED/C.BIN" && cp "$d/lfn.bin" "$m/SEED/Long Seed Dir/a seeded long file name.txt" &&
    cp "$d/mixed.bin" "$m/SEED/MiXeD.TxT" && cp "$d/empty.bin" "$m/SEED/EMPTY.TXT" &&
    cp "$d/jp.bin" "$m/SEED/日本語/ファイル.bin" || fail "fuse seeding"
    xumount
    printf 'ms0:/SEED/B.BIN\t%s\nms0:/SEED/C.BIN\t%s\nms0:/SEED/Long Seed Dir/a seeded long file name.txt\t%s\nms0:/SEED/MiXeD.TxT\t%s\nms0:/SEED/EMPTY.TXT\t%s\nms0:/SEED/日本語/ファイル.bin\t%s\n' \
        "$d/b.bin" "$d/c.bin" "$d/lfn.bin" "$d/mixed.bin" "$d/empty.bin" "$d/jp.bin" > "$d/manifest.txt"
}
echo "== exfat (64 MB exFAT partition at LBA 2048, 4 KB clusters)"
dd if=/dev/zero of="$O/exfatp.img" bs=1M count=64 2>/dev/null
mkfs.exfat -q -c 4096 "$O/exfatp.img" >/dev/null && "$O/mkimg" exfatfix "$O/exfatp.img" || fail "mkfs.exfat"
mbr "$O/exfat.img" "$O/exfatp.img" 2048 0x07 || fail "mbr exfat"
echo "-- golden"
cp "$O/exfat.img" "$O/exfat.golden.img"
if "$O/fstest_host" golden "$O/exfat.golden.img" "$O/exfat.golden.out.img" > "$O/exfat.golden.log" 2>&1; then
    sum=$(sha256sum "$O/exfat.golden.out.img" | cut -d' ' -f1)
    io=$(grep '^IO ' "$O/exfat.golden.log" | cut -d' ' -f2-)
    g="host/golden/exfat.txt"
    if [ $REGOLD = 1 ] || [ ! -f "$g" ]; then printf 'sha256 %s\nio %s\n' "$sum" "$io" > "$g"; echo "   golden written: $g"
    else
        [ "$sum" = "$(sed -n 's/^sha256 //p' "$g")" ] || fail "exfat: on-disk result differs from the golden image"
        [ "$io" = "$(sed -n 's/^io //p' "$g")" ] || fail "exfat: amount of card I/O changed (now $io)"
    fi
    xfsck "$O/exfat.golden.out.img"
    grep -E '^(RESULT|IO)' "$O/exfat.golden.log" | sed 's/^/   /'
else
    grep -E 'FAIL|RESULT' "$O/exfat.golden.log"; fail "exfat golden run"
fi
echo "-- full"
cp "$O/exfatp.img" "$O/exfatp.seed.img"
xseed "$O/exfatp.seed.img"
mbr "$O/exfat.full.img" "$O/exfatp.seed.img" 2048 0x07
if "$O/fstest_host" full "$O/exfat.full.img" "$O/exfat" "$O/seed_exfat/manifest.txt" > "$O/exfat.full.log" 2>&1; then
    grep -E '^RESULT|SKIP' "$O/exfat.full.log" | sed 's/^/   /'
else
    grep -E 'FAIL|RESULT|unexpected|alias|long name|mismatch' "$O/exfat.full.log"; fail "exfat full run"
fi
[ -f "$O/exfat.keep.img" ] && xfsck "$O/exfat.keep.img"
if [ -f "$O/exfat.final.img" ]; then
    xfsck "$O/exfat.final.img"
    "$O/mkimg" get "$O/exfat.final.img" 2048 $XP "$O/xpart.img"
    n=0
    if xmount "$O/xpart.img"; then
        while IFS="$(printf '\t')" read -r src want; do
            cmp -s "$O/mnt/${src#::/}" "$want" || fail "exfat: fuse-exfat reads '$src' differently from what the driver wrote"
            n=$((n + 1))
        done < "$O/exfat.survive.txt"
        xumount
    else fail "fuse mount (read back)"; fi
    echo "   fuse-exfat read back $n driver-written files"
fi
echo "-- fill"
cp "$O/exfat.img" "$O/exfat.fill.img"
if "$O/fstest_host" fill "$O/exfat.fill.img" "$O/exfat.fill.out.img" > "$O/exfat.fill.log" 2>&1; then
    grep '^RESULT' "$O/exfat.fill.log" | sed 's/^/   /'
    xfsck "$O/exfat.fill.out.img"
else
    grep -E 'FAIL|RESULT' "$O/exfat.fill.log"; fail "exfat fill run"
fi

# the same suite on exFAT with 128 KB clusters, as on real cards (Windows' default for 64 GB, DratiniFS's
# format): the big file is then shorter than 1024 clusters (256 MB sparse volume, in /tmp)
echo "== exfat128 (256 MB exFAT partition at LBA 2048, 128 KB clusters)"
X128=/tmp/exfat128p.img; XP=$((256 * 2048))
rm -f $X128 && truncate -s 256M $X128 && mkfs.exfat -q -c 131072 $X128 >/dev/null || fail "mkfs.exfat 128K"
mbr /tmp/exfat128.img $X128 2048 0x07 && rm -f $X128
if "$O/fstest_host" full /tmp/exfat128.img "$O/exfat128" > "$O/exfat128.full.log" 2>&1; then
    grep -E '^RESULT|SKIP' "$O/exfat128.full.log" | sed 's/^/   /'
else
    grep -E 'FAIL|RESULT|SKIP' "$O/exfat128.full.log"; fail "exfat128 full run"
fi
xfsck /tmp/exfat128.img
rm -f /tmp/exfat128.img "$O/exfat128.keep.img" "$O/exfat128.final.img"
XP=$((64 * 2048))

# ---- ExtremeSpeed (DratiniFS): an ES partition after a FAT32 / exFAT partition ----
# the adapter reports only the FS partition (attr = its end), so ES reads also need the large-card patch
d="$O/seed_es"; mkdir -p "$d"
head -c 1048699 /dev/urandom > "$d/game1.bin"; head -c 5000 /dev/urandom > "$d/dup_es.bin"
head -c 6000 /dev/urandom > "$d/dup_fs.bin"; head -c 300000 /dev/urandom > "$d/game2.bin"
head -c 777 /dev/urandom > "$d/game3.bin"; head -c 2048 /dev/urandom > "$d/root.bin"
head -c 4321 /dev/urandom > "$d/real.bin"; head -c 1000 /dev/urandom > "$d/gone.bin"
for fs in fat32 exfat; do
    echo "== es on $fs"
    if [ $fs = fat32 ]; then
        cp "$O/fat32p.img" "$O/esp.img"
        mmd -i "$O/esp.img" ::/ISO && mcopy -i "$O/esp.img" "$d/real.bin" ::/ISO/real.iso &&
            mcopy -i "$O/esp.img" "$d/dup_fs.bin" ::/ISO/dup.iso || fail "es: seeding $fs"
        type=0x0c
    else
        cp "$O/exfatp.img" "$O/esp.img"
        if xmount "$O/esp.img"; then
            mkdir -p "$O/mnt/ISO" && cp "$d/real.bin" "$O/mnt/ISO/real.iso" && cp "$d/dup_fs.bin" "$O/mnt/ISO/dup.iso" || fail "es: seeding $fs"
            xumount
        else fail "fuse mount (es)"; fi
        type=0x07
    fi
    psec=$(( $(stat -c%s "$O/esp.img") / 512 )); eslba=$((2048 + psec))
    mbr "$O/es_$fs.img" "$O/esp.img" 2048 $type 16384 &&
    "$O/mkimg" es "$O/es_$fs.img" 1 $eslba 16384 ISO/game1.iso="$d/game1.bin" ISO/dup.iso="$d/dup_es.bin" \
        ISO/Genre/game2.iso="$d/game2.bin" ISO/Genre/Sub/game3.iso="$d/game3.bin" ROOTFILE.ISO="$d/root.bin" \
        -ISO/gone.iso="$d/gone.bin" || fail "es: building the card"
    cp "$O/es_$fs.img" "$O/es_$fs.fresh.img"                # the run changes the card in place
    if "$O/fstest_host" es "$O/es_$fs.img" "$O/es_$fs.out.img" "$d" $eslba > "$O/es_$fs.log" 2>&1; then
        grep '^RESULT' "$O/es_$fs.log" | sed 's/^/   /'
    else
        grep -E 'FAIL|RESULT' "$O/es_$fs.log"; fail "es on $fs"
    fi
    if [ $fs = fat32 ]; then "$O/mkimg" get "$O/es_$fs.out.img" 2048 $psec "$O/es.part.img" && fsck_ok "$O/es.part.img"
    else xfsck "$O/es_$fs.out.img"; fi
done
# the same ES card at the far end of a sparse ~4.3 GB disk: ES sectors sit just below and past the
# 4 GB byte offset, so 64-bit offsets are exercised (a seek result cast to int fails there)
echo "== es past 4 GB"
B4=/tmp/es_4g.img; eslba=$((8388608 - 300))
rm -f $B4
mbr $B4 "$O/fat32p.img" 2048 0x0c $((eslba - 2048 - 98304 + 16384)) &&
"$O/mkimg" es $B4 1 $eslba 16384 ISO/game1.iso="$d/game1.bin" ISO/dup.iso="$d/dup_es.bin"     ISO/Genre/game2.iso="$d/game2.bin" ISO/Genre/Sub/game3.iso="$d/game3.bin" ROOTFILE.ISO="$d/root.bin"     -ISO/gone.iso="$d/gone.bin" || fail "es: building the 4 GB card"
cp "$O/fat32p.img" "$O/esp.img" && mmd -i "$O/esp.img" ::/ISO && mcopy -i "$O/esp.img" "$d/real.bin" ::/ISO/real.iso &&
    mcopy -i "$O/esp.img" "$d/dup_fs.bin" ::/ISO/dup.iso && "$O/mkimg" part $B4 0 0x0c 2048 98304 "$O/esp.img" || fail "es: 4 GB card FS"
if "$O/fstest_host" es $B4 - "$d" $((2048 + 98304)) > "$O/es_4g.log" 2>&1; then        # "-": no copy, the card is used in place
    grep '^RESULT' "$O/es_4g.log" | sed 's/^/   /'
else
    grep -E 'FAIL|RESULT' "$O/es_4g.log"; fail "es past 4 GB"
fi
"$O/mkimg" get $B4 2048 98304 "$O/es.part.img" && fsck_ok "$O/es.part.img"
rm -f $B4

# ---- formatting (DratiniFS): the ES app's (exFAT, FAT32) and XMB's (keeps the type) ----
# checks per step: fsck of the new filesystem, fuse-exfat / mtools can read it, partition 0 = the new
# FS at LBA 2048 up to ES, and the ES partition (MBR entry + every sector of it) unchanged
le() { printf '%08x' "$1" | sed 's/\(..\)\(..\)\(..\)\(..\)/\4\3\2\1/'; }   # 32-bit little endian, hex
fmt_check() {   # fmt_check <image> <step> <lba> <sectors> <es lba (0: superfloppy)>
    img=$1; i=$2; lba=$3; len=$4; eslba=$5
    want=07; [ $i -ge 2 ] && want=0c
    if [ "$eslba" != 0 ]; then
        got=$(od -A n -t x1 -j 446 -N 16 "$img" | tr -d ' \n')
        [ "$got" = "80feffff${want}feffff$(le $lba)$(le $len)" ] || fail "format step $i: partition 0 entry is $got"
        [ "$(od -A n -t x1 -j 462 -N 48 "$img")" = "$(od -A n -t x1 -j 462 -N 48 "$O/es_fat32.fresh.img")" ] || fail "format step $i: other MBR entries changed"
        "$O/mkimg" get "$img" $eslba 16384 "$O/es_area.img"
        [ "$(sha256sum < "$O/es_area.img")" = "$ES_SUM" ] || fail "format step $i: the ES partition changed"
    fi
    "$O/mkimg" get "$img" $lba $len "$O/fmtp.img"
    if [ $want = 07 ]; then
        fsck.exfat -n "$O/fmtp.img" > "$O/fsck.txt" 2>&1 || { sed 's/^/   /' "$O/fsck.txt"; fail "format step $i: fsck.exfat"; }
        if xmount "$O/fmtp.img"; then [ -f "$O/mnt/FMT.BIN" ] || fail "format step $i: fuse-exfat can't see FMT.BIN"; xumount
        else fail "format step $i: fuse-exfat can't mount it"; fi
    else
        fsck_ok "$O/fmtp.img"
        mdir -i "$O/fmtp.img" ::/FMT.BIN >/dev/null 2>&1 || fail "format step $i: mtools can't see FMT.BIN"
    fi
}
echo "== format (FAT32 + ES card)"
cp "$O/es_fat32.fresh.img" "$O/fmt_es.img" 2>/dev/null || fail "format: no ES card"
psec=98304; eslba=$((2048 + psec))
"$O/mkimg" get "$O/fmt_es.img" $eslba 16384 "$O/es_area.img"; ES_SUM=$(sha256sum < "$O/es_area.img")
if "$O/fstest_host" format "$O/fmt_es.img" "$O/fmt_es" "$d" $eslba > "$O/fmt_es.log" 2>&1; then
    grep '^RESULT' "$O/fmt_es.log" | sed 's/^/   /'
else
    grep -E 'FAIL|RESULT' "$O/fmt_es.log"; fail "format on the ES card"
fi
for i in 0 1 2 3; do [ -f "$O/fmt_es.$i.img" ] && fmt_check "$O/fmt_es.$i.img" $i 2048 $psec $eslba; done
echo "== format (superfloppy card)"
cp "$O/fat32.img" "$O/fmt_sf.img"
if "$O/fstest_host" format "$O/fmt_sf.img" "$O/fmt_sf" - > "$O/fmt_sf.log" 2>&1; then
    grep '^RESULT' "$O/fmt_sf.log" | sed 's/^/   /'
else
    grep -E 'FAIL|RESULT' "$O/fmt_sf.log"; fail "format on a superfloppy card"
fi
for i in 0 1 2 3; do [ -f "$O/fmt_sf.$i.img" ] && fmt_check "$O/fmt_sf.$i.img" $i 0 98304 0; done

# the old ES PC tool's FAT32 format: the boot sector's data area has more clusters than its FAT holds
# (fsck.fat: "only space for N FAT entries"). XMB's free-space scan used to loop at the FAT's end.
echo "== short FAT (old PC tool FAT32 format)"
cp "$O/fat32p.img" "$O/shortfat.img" && mmd -i "$O/shortfat.img" ::/PSP && mcopy -i "$O/shortfat.img" "$O/mkimg" ::/PSP/F.BIN &&
    printf '\000\100\002\000' | dd of="$O/shortfat.img" bs=1 seek=32 conv=notrunc 2>/dev/null &&   # 147456 sectors
    truncate -s $((147456 * 512)) "$O/shortfat.img" || fail "short FAT: building the card"
if timeout 120 "$O/fstest_host" walk "$O/shortfat.img" - > "$O/shortfat.log" 2>&1 &&
   grep -q "devinfo: $(( $(od -An -tu4 -j36 -N4 "$O/shortfat.img") * 128 - 2 )) clusters" "$O/shortfat.log"; then
    grep '^RESULT' "$O/shortfat.log" | sed 's/^/   /'
else
    grep -E 'FAIL|RESULT|devinfo' "$O/shortfat.log"; fail "short FAT (hang or wrong cluster count)"
fi

# ---- exFAT files past 4 GB: a 16 GB sparse volume (32 KB clusters) whose big contiguous files are laid out by
# exfat_bigfile.py (no gigabytes written). The driver reads and writes across 4 GB, renames, moves, appends
# (chaining the file), fills a gap, removes and truncates. fsck.exfat must pass before and after, and
# fuse-exfat must read back the size and the bytes the driver wrote.
echo "== exfat big files (past 4 GB)"
BIGI=/tmp/exbig.img
rm -f $BIGI; truncate -s 16G $BIGI
mkfs.exfat -q -c 32768 $BIGI >/dev/null &&
    python3 host/exfat_bigfile.py make $BIGI BIG.BIN 5368721465 BIG3.BIN 4294968073 BIG4.BIN 4294967301 > "$O/exbig.make.log" ||
    fail "exbig: building the volume"
fsck.exfat -n $BIGI > "$O/fsck.txt" 2>&1 || { sed 's/^/   /' "$O/fsck.txt"; fail "exbig: the script's volume isn't clean"; }
if "$O/fstest_host" exbig $BIGI - > "$O/exbig.log" 2>&1; then
    grep '^RESULT' "$O/exbig.log" | sed 's/^/   /'
else
    grep -E 'FAIL|RESULT' "$O/exbig.log"; fail "exfat big files"
fi
fsck.exfat -n $BIGI > "$O/fsck.txt" 2>&1 || { sed 's/^/   /' "$O/fsck.txt"; fail "fsck.exfat after the big-file run"; }
if xmount $BIGI; then
    python3 host/exfat_bigfile.py check "$O/mnt/VID/Big Video.mp4" 5368871475 \
        4294967288 43524f53532d3447422d4f4b21212121 5368721457 3130004001000001 4295000067 0380000001000001 \
        0 0000000000000001 5368721465 030a11181f262d343b424950575e656c 5368821449 f3fa01080f161d242b323940474e555c \
        5368822465 00000000000000000000000000000000 5368871465 454e442d4d41524b4552 | sed 's/^/   fuse-exfat: /' ||
        fail "fuse-exfat reads the big file differently"
    [ -e "$O/mnt/BIG3.BIN" ] && fail "BIG3.BIN still there"
    xumount
else fail "fuse mount (exbig)"; fi
rm -f $BIGI

# ---- PSP Go (fatmsef.prx): the same sources with PSPGO, serving ms0: + ef0: + eh0: ----
# ef0:/eh0: are eflash image files (host_ef_load). The Memory Stick is the ES card from above.
echo "== build (PSP Go)"
gcc $CFLAGS -DPSPGO -o "$O/fstest_host_go" $SRC $B/fatfs_ef.c host/hostkern.c host/io_host.c host/host_main.c common/scenarios.c -lpthread || exit 1
gorun() {    # gorun <name> <mode> <ms image> <ef image> <eh image>
    if "$O/fstest_host_go" "$2" "$3" "$O/$1" "$4" "$5" > "$O/$1.log" 2>&1; then
        grep '^RESULT' "$O/$1.log" | sed 's/^/   /'
    else
        grep -E 'FAIL|RESULT' "$O/$1.log"; fail "$1"
    fi
}
newfat() {   # newfat <image> <MB> <fat bits>
    dd if=/dev/zero of="$1" bs=1M count=$2 2>/dev/null && mkfs.fat --invariant -i 12345678 -F $3 "$1" >/dev/null || fail "mkfs $1"
}
echo "== go: ms0: (ES card) + ef0: (FAT32) + eh0: (FAT16)"
cp "$O/es_fat32.fresh.img" "$O/go_ms.img"
newfat "$O/go_ef.img" 48 32; newfat "$O/go_eh.img" 16 16
gorun go go "$O/go_ms.img" "$O/go_ef.img" "$O/go_eh.img"
fsck_ok "$O/go_ef.img"; fsck_ok "$O/go_eh.img"
"$O/mkimg" get "$O/go_ms.img" 2048 98304 "$O/go_ms.part.img" && fsck_ok "$O/go_ms.part.img"
echo "== go: emulated ms0: (a game run from internal storage)"
newfat "$O/go_ef.img" 48 32; newfat "$O/go_eh.img" 16 16
gorun goemu goemu "$O/go_ms.img" "$O/go_ef.img" "$O/go_eh.img"
fsck_ok "$O/go_ef.img"
gorun goemu-nomsemu goemu-nomsemu "$O/go_ms.img" "$O/go_ef.img" "$O/go_eh.img"
echo "== go: internal storage format (Sony's layouts: 8 GB flash, 16 GB flash)"
EF=/tmp/go_ef8.img; EH=/tmp/go_eh.img
rm -f $EF $EH; truncate -s $((0xe5ddc0 * 512)) $EF; truncate -s $((0x1dfc0 * 512)) $EH
gorun goformat goformat "$O/go_ms.img" $EF $EH
fsck_ok $EF sonylabel
fsck_ok $EH sonyeh
EF=/tmp/go_ef16.img
rm -f $EF; truncate -s $((0x1d3dd80 * 512)) $EF
gorun goformat16 goformat16 "$O/go_ms.img" $EF $EH
fsck_ok $EF sonylabel
rm -f /tmp/go_ef8.img /tmp/go_ef16.img /tmp/go_eh.img

echo
if [ $FAILS = 0 ]; then echo "ALL HOST TESTS PASSED"; else echo "$FAILS HOST TEST STEP(S) FAILED (logs in tests/out)"; fi
[ $FAILS = 0 ]
