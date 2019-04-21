#include "cache.h"

#include "util.h"

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>

/**
 * \brief Data file block size
 * \details The data file block size is set to 128KiB, for convenience. This is
 * because the maximum requested block size by FUSE seems to be 128KiB under
 * Debian Stretch. Note that the minimum requested block size appears to be
 * 4KiB.
 *
 * More information regarding block size can be found at:
 * https://wiki.vuze.com/w/Torrent_Piece_Size
 *
 * Note that at the current configuration, a 16GiB file uses 128MiB of memory to
 * store the bitmap, but hey, I have 16GiB on my computer!
 */
#define DATA_BLK_SZ         131072

/**
 * \brief the maximum length of a path
 * \details This corresponds the maximum path length under Ext4. If you need
 * longer path, then fuck you.
 */
#define MAX_PATH_LEN        4096

/**
 * \brief The metadata directory
 */
char *META_DIR;

/**
 * \brief The data directory
 */
char *DATA_DIR;


void Cache_init(const char *dir)
{
    META_DIR = strndupcat(dir, "meta/", MAX_PATH_LEN);
    DATA_DIR = strndupcat(dir, "data/", MAX_PATH_LEN);
}

Cache *Cache_alloc()
{
    Cache *cf = calloc(1, sizeof(Cache));
    if (!cf) {
        fprintf(stderr, "Cache_new(): calloc failure!\n");
        exit(EXIT_FAILURE);
    }
    return cf;
}

void Cache_free(Cache *cf)
{
    if (cf->filename) {
        free(cf->filename);
    }
    if (cf->seg) {
        free(cf->seg);
    }
    free(cf);
}

int Cache_exist(const char *fn)
{
    int meta_exists = 1;
    int data_exists = 1;
    char *metafn = strndupcat(META_DIR, fn, MAX_PATH_LEN);
    char *datafn = strndupcat(DATA_DIR, fn, MAX_PATH_LEN);

    if (access(metafn, F_OK)) {
        fprintf(stderr, "Cache_exist(): access(): %s\n", strerror(errno));
        meta_exists = 0;
    }

    if (access(datafn, F_OK)) {
        fprintf(stderr, "Cache_exist(): access(): %s\n", strerror(errno));
        data_exists = 0;
    }

    if (meta_exists ^ data_exists) {
        if (meta_exists) {
            if(unlink(metafn)) {
                fprintf(stderr, "Cache_exist(): unlink(): %s\n",
                        strerror(errno));
            }
        }
        if (data_exists) {
            if(unlink(datafn)) {
                fprintf(stderr, "Cache_exist(): unlink(): %s\n",
                        strerror(errno));
            }
        }
    }

    free(metafn);
    free(datafn);

    return !(meta_exists & data_exists);
}

Cache *Cache_create(const char *fn, long len, long time)
{
    Cache *cf = Cache_alloc();

    cf->filename = strndup(fn, MAX_PATH_LEN);
    cf->time = time;
    cf->len = len;

    if (Data_create(cf)) {
        Cache_free(cf);
        fprintf(stderr, "Cache_create(): Data_create() failed!\n");
        return NULL;
    }

    if (Meta_create(cf)) {
        Cache_free(cf);
        fprintf(stderr, "Cache_create(): Meta_create() failed!\n");
        return NULL;
    }
    return cf;
}

void Cache_delete(const char *fn)
{
    char *metafn = strndupcat(META_DIR, fn, MAX_PATH_LEN);
    char *datafn = strndupcat(DATA_DIR, fn, MAX_PATH_LEN);
    if (!access(metafn, F_OK)) {
        if(unlink(metafn)) {
            fprintf(stderr, "Cache_delete(): unlink(): %s\n",
                    strerror(errno));
        }
    }

    if (!access(datafn, F_OK)) {
        if(unlink(datafn)) {
            fprintf(stderr, "Cache_delete(): unlink(): %s\n",
                    strerror(errno));
        }
    }
    free(metafn);
    free(datafn);
}

Cache *Cache_open(const char *fn)
{
    /* Check if both metadata and data file exist */
    if (Cache_validate(fn)) {
        return NULL;
    }

    /* Create the cache in-memory data structure */
    Cache *cf = Cache_alloc();
    cf->filename = strndup(fn, MAX_PATH_LEN);

    if (Meta_read(cf)) {
        Cache_free(cf);
        return NULL;
    }

    /* In consistent metadata / data file */
    if (cf->len != Data_size(fn)) {
        fprintf(stderr,
            "Cache_open(): metadata is inconsistent with the data file!\n");
        Cache_delete(fn);
        Cache_free(cf);
        return NULL;
    }

    return cf;
}

int Meta_create(const Cache *cf)
{
    cf->blksz = DATA_BLK_SZ;
    cf->nseg = cf->len / cf->blksz + 1;
    cf->seg = calloc(nseg, sizeof(Seg));
    if (!(cf->seg)) {
        fprintf(stderr, "Meta_create(): calloc failure!\n");
        exit(EXIT_FAILURE);
    }
    return Meta_write(cf);
}

int Meta_read(Cache *cf)
{
    FILE *fp;
    char *metafn = strndupcat(META_DIR, cf->filename, MAX_PATH_LEN);
    fp = fopen(metafn, "r");
    free(metafn);
    int res = 0;
    int nmemb = 0;

    if (!fp) {
        /* The metadata file does not exist */
        fprintf(stderr, "Meta_read(): fopen(): %s\n", strerror(errno));
        return -1;
    }

    fread(&(cf->time), sizeof(long), 1, fp);
    fread(&(cf->len), sizeof(long), 1, fp);
    fread(&(cf->len), sizeof(int), 1, fp);
    fread(&(cf->nseg), sizeof(int), 1, fp);

    /* Allocate some memory for the segment */
    cf->seg = malloc(cf->nseg * sizeof(Seg));
    if (!(cf->seg)) {
        fprintf(stderr, "Meta_read(): malloc failure!\n");
        exit(EXIT_FAILURE);
    }
    /* Read all the segment */
    nmemb = fread(cf->seg, sizeof(Seg), cf->nseg, fp);

    /* Error checking for fread */
    if (ferror(fp)) {
        fprintf(stderr,
                "Meta_read(): fread(): encountered error (from ferror)!\n");
        res = -1;
    }

    /* Check for inconsistent metadata file */
    if (nmemb != cf-> nseg) {
        fprintf(stderr,
                "Meta_read(): corrupted metadata!\n");
        res = -1;
    }

    if (fclose(fp)) {
        fprintf(stderr, "Meta_read(): fclose(): %s\n", strerror(errno));
    }

    return res;
}

int Meta_write(const Cache *cf)
{
    FILE *fp;
    char *metafn = strndupcat(META_DIR, cf->filename, MAX_PATH_LEN);
    fp = fopen(metafn, "w");
    free(metafn);
    int res = 0;

    if (!fp) {
        /* Cannot create the metadata file */
        fprintf(stderr, "Meta_write(): fopen(): %s\n", strerror(errno));
        return -1;
    }

    fwrite(&(cf->time), sizeof(long), 1, fp);
    fwrite(&(cf->len), sizeof(long), 1, fp);
    fwrite(&(cf->blksz), sizeof(int), 1, fp);
    fwrite(&(cf->nseg), sizeof(int), 1, fp);
    fwrite(cf->seg, sizeof(Seg), cf->nseg, fp);

    /* Error checking for fwrite */
    if (ferror(fp)) {
        fprintf(stderr,
                "Meta_write(): fwrite(): encountered error (from ferror)!\n");
        res = -1;
    }

    if (fclose(fp)) {
        fprintf(stderr, "Meta_write(): fclose(): %s\n", strerror(errno));
    }
    return res;
}

int Data_create(Cache *cf)
{
    int fd;
    int mode;

    mode = S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;
    char *datafn = strndupcat(DATA_DIR, cf->filename, MAX_PATH_LEN);
    fd = open(datafn, O_WRONLY | O_CREAT, mode);
    free(datafn);
    if (fd == -1) {
        fprintf(stderr, "Data_create(): open(): %s\n", strerror(errno));
        return -1;
    }
    if (ftruncate(fd, cf->len)) {
        fprintf(stderr, "Data_create(): ftruncate(): %s\n", strerror(errno));
    }
    if (close(fd)) {
        fprintf(stderr, "Data_create(): close:(): %s\n", strerror(errno));
    }
    return 0;
}

long Data_size(const char *fn)
{
    char *datafn = strndupcat(DATA_DIR, fn, MAX_PATH_LEN);
    struct stat st;
    int s = stat(datafn, &st);
    free(datafn);
    if (!s) {
        return st.st_blksize;
    }
    fprintf(stderr, "Data_size(): stat(): %s\n", strerror(errno));
    return -1;
}

long Data_read(const Cache *cf, long offset, long len,
               uint8_t *buf)
{
    if (len == 0) {
        fprintf(stderr, "Data_read(): requested to read 0 byte!\n");
        return -1;
    }

    FILE *fp;
    char *datafn = strndupcat(DATA_DIR, cf->filename, MAX_PATH_LEN);
    fp = fopen(datafn, "r");
    free(datafn);
    long byte_read = -1;

    if (!fp) {
        /* Failed to open the data file */
        fprintf(stderr, "Data_read(): fopen(): %s\n", strerror(errno));
        return -1;
    }

    if (fseek(fp, offset, SEEK_SET)) {
        /* fseek failed */
        fprintf(stderr, "Data_read(): fseek(): %s\n", strerror(errno));
        goto cleanup;
    }

    byte_read = fread(buf, sizeof(uint8_t), len, fp);
    if (byte_read != len) {
        fprintf(stderr,
                "Data_read(): fread(): requested %ld, returned %ld!\n",
                len, byte_read);
        if (feof(fp)) {
            /* reached EOF */
            fprintf(stderr,
                    "Data_read(): fread(): reached the end of the file!\n");
        }
        if (ferror(fp)) {
            /* filesystem error */
            fprintf(stderr,
                    "Data_read(): fread(): encountered error (from ferror)!\n");
        }
    }

    cleanup:
    if (fclose(fp)) {
        fprintf(stderr, "Data_read(): fclose(): %s\n", strerror(errno));
    }
    return byte_read;
}

long Data_write(const Cache *cf, long offset, long len,
                const uint8_t *buf)
{
    if (len == 0) {
        fprintf(stderr, "Data_write(): requested to write 0 byte!\n");
        return -1;
    }

    FILE *fp;
    char *datafn = strndupcat(DATA_DIR, cf->filename, MAX_PATH_LEN);
    fp = fopen(datafn, "r+");
    free(datafn);
    long byte_written = -1;

    if (!fp) {
        /* Failed to open the data file */
        fprintf(stderr, "Data_write(): fopen(): %s\n", strerror(errno));
        return -1;
    }

    if (fseek(fp, offset, SEEK_SET)) {
        /* fseek failed */
        fprintf(stderr, "Data_write(): fseek(): %s\n", strerror(errno));
        goto cleanup;
    }

    byte_written = fwrite(buf, sizeof(uint8_t), len, fp);
    if (byte_written != len) {
        fprintf(stderr,
                "Data_write(): fwrite(): requested %ld, returned %ld!\n",
                len, byte_written);
        if (feof(fp)) {
            /* reached EOF */
            fprintf(stderr,
                    "Data_write(): fwrite(): reached the end of the file!\n");
        }
        if (ferror(fp)) {
            /* filesystem error */
            fprintf(stderr,
                "Data_write(): fwrite(): encountered error (from ferror)!\n");
        }
    }

    cleanup:
    if (fclose(fp)) {
        fprintf(stderr, "Data_write(): fclose(): %s\n", strerror(errno));
    }
    return byte_written;
}
