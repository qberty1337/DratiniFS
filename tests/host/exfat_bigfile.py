#!/usr/bin/env python3
"""exfat_bigfile.py - files past 4 GB on a fresh exFAT image, without writing gigabytes.

  exfat_bigfile.py make <image> NAME SIZE [NAME SIZE ...]
      Lays each file out contiguously (NoFatChain) after the volume's last used cluster: bitmap bits,
      an entry set in the root (checksum, name hash), ValidDataLength = DataLength = SIZE. The data stays
      sparse zeros except 8-byte markers at 0, 4 GB - 8, 4 GB, 4 GB + cluster + 3 and SIZE - 8: the value
      (n << 56) | offset, n being the file's position on the command line (1, 2, ...), little-endian.
      Names are upper-case ASCII (the upcase table is then the identity).
  exfat_bigfile.py check <file> SIZE OFFSET HEXBYTES [OFFSET HEXBYTES ...]
      Through any exFAT implementation (fuse-exfat here): the file's size, and bytes at offsets.
"""
import os, struct, sys

G4 = 1 << 32


def le(b, o, n):
    return int.from_bytes(b[o:o + n], 'little')


def marker(n, off):
    return struct.pack('<Q', (n << 56) | off)


def make(img, files):
    f = open(img, 'r+b')
    boot = f.read(512)
    assert boot[3:11] == b'EXFAT   ', 'not exFAT'
    bps, spc = 1 << boot[108], 1 << boot[109]
    fat_off, heap, count, root = le(boot, 80, 4), le(boot, 88, 4), le(boot, 92, 4), le(boot, 96, 4)
    cb = bps * spc

    def cl_off(cl):
        return (heap + (cl - 2) * spc) * bps

    def fat(cl):
        f.seek(fat_off * bps + cl * 4)
        return le(f.read(4), 0, 4)

    # the root directory's clusters
    rcl, root_cls = root, []
    while 2 <= rcl < count + 2 and len(root_cls) < 64:
        root_cls.append(rcl)
        nx = fat(rcl)
        rcl = nx if nx not in (0, 0xffffffff) else 0
    rootdir = bytearray()
    for cl in root_cls:
        f.seek(cl_off(cl))
        rootdir += f.read(cb)
    bm_cl = bm_len = None
    end = None
    for i in range(0, len(rootdir), 32):
        t = rootdir[i]
        if t == 0x81 and bm_cl is None:
            bm_cl, bm_len = le(rootdir, i + 20, 4), le(rootdir, i + 24, 8)
        if t == 0 and end is None:
            end = i
    assert bm_cl and end is not None
    f.seek(cl_off(bm_cl))                                      # mkfs.exfat lays the bitmap out in one run
    bm = bytearray(f.read(bm_len))
    used = [i for i in range(count) if bm[i >> 3] >> (i & 7) & 1]
    nxt = (max(used) + 1 if used else 0) + 2                  # first free cluster after the used ones
    pos = end
    for k, (name, size) in enumerate(files, 1):
        n = (size + cb - 1) // cb
        assert nxt - 2 + n <= count, 'image too small'
        for c in range(nxt, nxt + n):
            bm[(c - 2) >> 3] |= 1 << ((c - 2) & 7)
        u = name.encode('utf-16-le')
        assert name == name.upper() and len(name) <= 15
        h = 0
        for b in u:                                             # NameHash over the up-cased name
            h = (((h & 1) << 15) | (h >> 1)) + b & 0xffff
        s = bytearray(96)
        s[0], s[1] = 0x85, 2
        struct.pack_into('<H', s, 4, 0x20)
        for o in (8, 12, 16):
            struct.pack_into('<I', s, o, (46 << 25) | (10 << 21) | (10 << 16))   # 2026-10-10 00:00
        s[32], s[33], s[35] = 0xc0, 0x03, len(name)            # AllocationPossible | NoFatChain
        struct.pack_into('<H', s, 36, h)                       # NameHash
        struct.pack_into('<Q', s, 40, size)                    # ValidDataLength
        struct.pack_into('<I', s, 52, nxt)                     # FirstCluster
        struct.pack_into('<Q', s, 56, size)                    # DataLength
        s[64] = 0xc1
        s[66:66 + len(u)] = u
        cs = 0
        for i, b in enumerate(s):                              # SetChecksum (bytes 2-3 excluded)
            if i not in (2, 3):
                cs = (((cs & 1) << 15) | (cs >> 1)) + b & 0xffff
        struct.pack_into('<H', s, 2, cs)
        assert pos + 96 + 32 <= len(rootdir), 'root directory full'
        rootdir[pos:pos + 96] = s
        pos += 96
        for off in (0, G4 - 8, G4, G4 + cb + 3, size - 8):
            if 0 <= off <= size - 8:
                f.seek(cl_off(nxt) + off)
                f.write(marker(k, off))
        print(f'{name}: {size} bytes, clusters {nxt}..{nxt + n - 1}')
        nxt += n
    f.seek(cl_off(bm_cl))
    f.write(bm)
    for i, cl in enumerate(root_cls):
        f.seek(cl_off(cl))
        f.write(rootdir[i * cb:(i + 1) * cb])
    f.close()


def check(path, size, pairs):
    st = os.stat(path)
    if st.st_size != size:
        sys.exit(f'{path}: size {st.st_size}, want {size}')
    with open(path, 'rb') as f:
        for off, want in pairs:
            f.seek(off)
            got = f.read(len(want))
            if got != want:
                sys.exit(f'{path} @ {off}: {got.hex()}, want {want.hex()}')
    print(f'{path}: {size} bytes, {len(pairs)} spots match')


if __name__ == '__main__':
    a = sys.argv
    if len(a) >= 5 and a[1] == 'make' and len(a) % 2 == 1:
        make(a[2], [(a[i], int(a[i + 1], 0)) for i in range(3, len(a), 2)])
    elif len(a) >= 4 and a[1] == 'check' and len(a) % 2 == 0:
        check(a[2], int(a[3], 0), [(int(a[i], 0), bytes.fromhex(a[i + 1])) for i in range(4, len(a), 2)])
    else:
        sys.exit(__doc__)
