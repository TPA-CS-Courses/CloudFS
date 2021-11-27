//
// Created by Wilson_Xu on 2021/11/14.
//

#ifndef SRC_MYDEDUP_H
#define SRC_MYDEDUP_H

struct dedup_config {

    int window_size;
    int avg_seg_size;
    int min_seg_size;
    int max_seg_size;
    int cache;
    struct cloudfs_state *fstate;
    FILE *logfile;
};


typedef struct seg_info {
    long seg_size;
    char md5[MD5_DIGEST_LENGTH * 2 + 1];
} seg_info_t, *seg_info_p;

void debug_showfile(const char *file, const char *functionname, int line);

FILE *ffopen_(const char *func, const char *filename, const char *mode);

int ffclose_(const char *func, FILE *s);


void mydedup_init(int window_size, int avg_seg_size, int min_seg_size, int max_seg_size, FILE *logfile,
                  struct cloudfs_state *fstate);

void mydedup_destroy();

void get_tempfile_path_dedup(char *tempfile_path, char *path_s, int bufsize);


void get_fileproxy_path(char *fileproxy_path, char *path_s, int bufsize);

void get_seg_proxy_path(char *seg_proxy_path, const char *md5, int bufsize);

void get_tempseg_path(char *tempseg_path, const char *md5, int bufsize);

void debug_showsegs(std::vector <seg_info_p> segs);

bool file_exist(const char *path_s);

int mydedup_segmentation(char *fpath, std::vector <seg_info_p> &segs);


int mydedup_down_segs(char *path_s, std::vector <seg_info_p> &segs);


void mydedup_upload_segs(char *path_s, std::vector <seg_info_p> &segs);

void mydedup_upload_file(char *path_s);

int mydedup_getattr(const char *pathname, struct stat *statbuf);

void mydedup_get_seginfo(const char *path_s, std::vector <seg_info_p> &segs);


int mydedup_write(const char *pathname, const char *buf, size_t size, off_t offset,
                  struct fuse_file_info *fi);


int mydedup_read(const char *pathname, char *buf, size_t size, off_t offset, struct fuse_file_info *fi);

void mydedup_remove_segs(std::vector <seg_info_p> &segs);

void mydedup_remove_one_seg(char *md5);

int mydedup_release(const char *pathname, struct fuse_file_info *fi);

int set_ref(const char *pathname, int value);

int get_ref(const char *pathname, int *value_p);

void seg_upload(char *pathname, char *key, long size);

int mydedup_unlink(const char *pathname);

int mydedup_truncate(const char *pathname, off_t newsize);


#endif //SRC_MYDEDUP2_H
