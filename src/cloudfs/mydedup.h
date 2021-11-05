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
};

void mydedup_init(unsigned int window_size, unsigned int avg_seg_size, unsigned int min_seg_size, unsigned int max_seg_size, int cache, FILE *logfile);
int dedup_segmentation(char *fpath, int *num_seg, FILE *logfile);