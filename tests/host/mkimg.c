/* mkimg.c - build whole-disk test images (host tool, see tests/run_host.sh)
 *   mkimg disk <out> <sectors>                                  zero-filled disk, empty MBR (55 AA)
 *   mkimg part <disk> <slot> <type> <lba> <sectors> [<image>]   MBR entry (slot 0 gets status 0x80, as
 *                                                               msstor requires) + copy the image there
 *   mkimg get  <disk> <lba> <sectors> <out>                     copy a partition out (for fsck / fuse)
 *   mkimg exfatfix <exfat image>                                volume serial 0x12345678 + boot checksums
 *   mkimg es <disk> <slot> <lba> <sectors> name=file...         an ES v3 partition (type 0xDA); "-name=file"
 *                                                               makes an inactive entry */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <unistd.h>

static FILE *open_rw(const char *p) { FILE *f = fopen(p, "r+b"); if (!f) { perror(p); exit(1); } return f; }
static void put32(uint8_t *p, uint32_t v) { p[0] = (uint8_t)v; p[1] = (uint8_t)(v >> 8); p[2] = (uint8_t)(v >> 16); p[3] = (uint8_t)(v >> 24); }
static void copy(FILE *dst, long long doff, FILE *src, long long soff, long long n)
{
    static uint8_t buf[1 << 16];
    fseeko(dst, doff, SEEK_SET); fseeko(src, soff, SEEK_SET);
    while (n > 0) {
        size_t k = n > (long long)sizeof buf ? sizeof buf : (size_t)n;
        if (fread(buf, 1, k, src) != k || fwrite(buf, 1, k, dst) != k) { fprintf(stderr, "copy failed\n"); exit(1); }
        n -= (long long)k;
    }
}

int main(int argc, char **argv)
{
    if (argc >= 4 && !strcmp(argv[1], "disk")) {
        FILE *f = fopen(argv[2], "wb");
        uint8_t mbr[512] = { 0 };
        mbr[510] = 0x55; mbr[511] = 0xaa;
        fwrite(mbr, 1, 512, f);
        if (ftruncate(fileno(f), (off_t)strtoull(argv[3], 0, 0) * 512)) { perror("truncate"); return 1; }
        return fclose(f) ? 1 : 0;
    }
    if (argc >= 7 && !strcmp(argv[1], "part")) {
        int slot = atoi(argv[3]);
        uint32_t type = (uint32_t)strtoul(argv[4], 0, 0), lba = (uint32_t)strtoul(argv[5], 0, 0), n = (uint32_t)strtoul(argv[6], 0, 0);
        FILE *f = open_rw(argv[2]);
        uint8_t mbr[512];
        if (fread(mbr, 1, 512, f) != 512) return 1;
        uint8_t *e = mbr + 0x1be + slot * 16;
        memset(e, 0, 16);
        e[0] = slot == 0 ? 0x80 : 0x00;
        e[1] = 0xfe; e[2] = 0xff; e[3] = 0xff; e[5] = 0xfe; e[6] = 0xff; e[7] = 0xff;   /* CHS: use LBA */
        e[4] = (uint8_t)type; put32(e + 8, lba); put32(e + 12, n);
        fseek(f, 0, SEEK_SET); fwrite(mbr, 1, 512, f);
        if (argc >= 8) { FILE *s = fopen(argv[7], "rb"); if (!s) { perror(argv[7]); return 1; } copy(f, (long long)lba * 512, s, 0, (long long)n * 512); fclose(s); }
        return fclose(f) ? 1 : 0;
    }
    if (argc >= 3 && !strcmp(argv[1], "exfatfix")) {      /* fixed volume serial -> same image every run */
        FILE *f = open_rw(argv[2]);
        static uint8_t b[24][512];
        if (fread(b, 512, 24, f) != 24) return 1;
        for (int r = 0; r < 2; r++) {                       /* main and backup boot region */
            uint8_t (*s)[512] = b + r * 12;
            put32(s[0] + 100, 0x12345678);
            uint32_t sum = 0;
            for (int k = 0; k < 11; k++)
                for (int i = 0; i < 512; i++)
                    if (k || (i != 106 && i != 107 && i != 112)) sum = ((sum & 1) ? 0x80000000u : 0) + (sum >> 1) + s[k][i];
            for (int i = 0; i < 128; i++) put32(s[11] + i * 4, sum);
        }
        fseek(f, 0, SEEK_SET); fwrite(b, 512, 24, f);
        return fclose(f) ? 1 : 0;
    }
    if (argc >= 6 && !strcmp(argv[1], "es")) {            /* an ExtremeSpeed v3 partition, as the ES app writes */
        int slot = atoi(argv[3]);
        uint32_t lba = (uint32_t)strtoul(argv[4], 0, 0), n = (uint32_t)strtoul(argv[5], 0, 0), count = 0, pool = 0, next = 256;
        static uint8_t sb[512], tab[79 * 512], names[120 * 512];
        FILE *f = open_rw(argv[2]);
        for (int i = 6; i < argc; i++) {                   /* name=file; a leading '-' = inactive entry */
            char *eq = strchr(argv[i], '='), *name = argv[i] + (argv[i][0] == '-');
            if (!eq) return 2;
            *eq = 0;
            FILE *s = fopen(eq + 1, "rb");
            if (!s) { perror(eq + 1); return 1; }
            fseek(s, 0, SEEK_END);
            uint32_t size = (uint32_t)ftell(s), secs = (size + 511) / 512;
            uint8_t *e = tab + count * 64;
            snprintf((char *)e, 16, "TEST%05u", count);
            put32(e + 16, next); put32(e + 20, secs); put32(e + 24, size); put32(e + 28, argv[i][0] == '-' ? 0 : 1);
            put32(e + 32, pool); e[36] = (uint8_t)strlen(name); e[37] = (uint8_t)(strlen(name) >> 8);
            memcpy(names + pool, name, strlen(name) + 1);
            pool += (uint32_t)strlen(name) + 1;
            copy(f, (long long)(lba + next) * 512, s, 0, size);
            fclose(s);
            next += secs; count++;
        }
        put32(sb, 0x44505345); put32(sb + 4, 3); put32(sb + 8, count); put32(sb + 12, 256);
        put32(sb + 16, n); put32(sb + 20, 1); put32(sb + 28, pool);
        fseeko(f, (long long)lba * 512, SEEK_SET); fwrite(sb, 1, 512, f); fwrite(tab, 1, sizeof tab, f); fwrite(names, 1, sizeof names, f);
        fseeko(f, (long long)(lba + 200) * 512, SEEK_SET); fwrite(sb, 1, 512, f);
        fclose(f);
        char a[16], b[16], c2[16];
        snprintf(a, 16, "%d", slot); snprintf(b, 16, "%u", lba); snprintf(c2, 16, "%u", n);
        char *pa[] = { argv[0], "part", argv[2], a, "0xda", b, c2, 0 };
        return main(7, pa);
    }
    if (argc >= 6 && !strcmp(argv[1], "get")) {
        FILE *f = fopen(argv[2], "rb"), *o = fopen(argv[5], "wb");
        if (!f || !o) return 1;
        copy(o, 0, f, (long long)strtoull(argv[3], 0, 0) * 512, (long long)strtoull(argv[4], 0, 0) * 512);
        return fclose(o) ? 1 : 0;
    }
    fprintf(stderr, "usage: mkimg disk|part|get ...\n");
    return 2;
}
