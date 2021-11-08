//
// Created by Wilson_Xu on 2021/11/4.
//

#ifndef SRC_MYDEDUP_H
#define SRC_MYDEDUP_H

#endif //SRC_MYDEDUP_H
struct dedup_config {

    int window_size;
    int avg_seg_size;
    int min_seg_size;
    int max_seg_size;
    int cache;
    struct cloudfs_state *fstate;
    FILE * logfile;
};


typedef struct seg_info {
    long seg_size;
    char md5[MD5_DIGEST_LENGTH * 2 + 1];
}seg_info_t, *seg_info_p;


void mydedup_init(int window_size, int avg_seg_size, int min_seg_size, int max_seg_size, FILE *logfile, struct cloudfs_state *fstate);
int dedup_segmentation(char *fpath, int *num_seg, FILE *logfile);