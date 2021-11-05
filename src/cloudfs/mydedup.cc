//
// Created by Wilson_Xu on 2021/11/4.
//
// ref: https://github.com/libfuse/libfuse/blob/master/example/passthrough.c
// ref: https://github.com/libfuse/libfuse/blob/master/example/passthrough.c
// ref: https://github.com/libfuse/libfuse/blob/master/example/passthrough.c
#include <ctype.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <fuse.h>
#include <getopt.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/xattr.h>
#include <openssl/md5.h>
#include <time.h>
#include <unistd.h>
#include "cloudapi.h"
#include "dedup.h"
#include "cloudfs.h"
#include "mydedup.h"

#define BUF_SIZE (1024)


#define SHOWPF


#define UNUSED __attribute__((unused))

#ifdef SHOWPF
#define PF(...) fprintf(logfile,__VA_ARGS__)
#else
#define PF(...) sizeof(__VA_ARGS__)
#endif

#define NOI() fprintf(logfile,"INFO: NOT IMPLEMENTED [%s]\t Line:%d \n\n", __func__, __LINE__)

#ifdef SHOWINFO
#define INFO() fprintf(logfile,"INFO: SSD ONLY [%s]\t Line:%d\n", __func__, __LINE__)
#define INFOF() fprintf(logfile,"INFO: F [%s]\t Line:%d\n", __func__, __LINE__)
#else
#define INFO() sizeof(void)
#define INFOF() sizeof(void)
#endif

#define ON_SSD 0
#define ON_CLOUD 1
#define N_DIRTY 0
#define DIRTY 1

#define BUCKET ("test")
#define TEMPDIR (".tempfiles")

static rabinpoly_t *rp;
struct dedup_config de_cfg_s;
struct dedup_config *de_cfg;


void mydedup_init(int window_size, int avg_seg_size, int min_seg_size, int max_seg_size, int cache, FILE *logfile) {
    de_cfg = &de_cfg_s;
    de_cfg->window_size = window_size;
    de_cfg->avg_seg_size = avg_seg_size;
    de_cfg->min_seg_size = min_seg_size;
    de_cfg->max_seg_size = max_seg_size;
    de_cfg->cache = cache;
    PF("[%s]:\n", __func__);
}

int mydedup_segmentation(char *fpath, int *num_seg, FILE *logfile) {
    int ret = 0;
    int fd;
    fd = open(fpath, O_RDONLY);
    if (fd < 0) {
        ret = cloudfs_error("dedup_segmentation");
        return ret;
    }

    rp = rabin_init(de_cfg->window_size, de_cfg->avg_seg_size, de_cfg->min_seg_size, de_cfg->max_seg_size);

    if (!rp) {
        ret = cloudfs_error("dedup_segmentation");
        return ret;
    }

    MD5_CTX ctx;
    unsigned char md5[MD5_DIGEST_LENGTH];
    int new_segment = 0;
    int len, segment_len = 0, b;
    char buf[BUF_SIZE];
    int bytes;

    MD5_Init(&ctx);

    while ((bytes = read(fd, buf, sizeof buf)) > 0) {
        char *buftoread = (char *) &buf[0];
        while ((len = rabin_segment_next(rp, buftoread, bytes,
                                         &new_segment)) > 0) {
            MD5_Update(&ctx, buftoread, len);
            segment_len += len;

            if (new_segment) {
                MD5_Final(md5, &ctx);

                printf("%u ", segment_len);
                for (b = 0; b < MD5_DIGEST_LENGTH; b++) {
                    printf("%02x", md5[b]);
                }
                printf("\n");

                MD5_Init(&ctx);
                segment_len = 0;
            }

            buftoread += len;
            bytes -= len;

            if (!bytes) {
                break;
            }
        }
        if (len == -1) {
            fprintf(stderr, "Failed to process the segment\n");
            exit(2);
        }
    }
}