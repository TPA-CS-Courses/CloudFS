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
    FILE *logfile;
};


typedef struct seg_info {
    long seg_size;
    char md5[MD5_DIGEST_LENGTH * 2 + 1];
    bool oncloud;
    char seg_ssd_path[MAX_PATH_LEN];
} seg_info_t, *seg_info_p;

typedef struct ret_range {
    int start;
    int end;
} ret_range_t, *ret_range_p;


void mydedup_init(int window_size, int avg_seg_size, int min_seg_size, int max_seg_size, FILE *logfile,
                  struct cloudfs_state *fstate);
long mydedup_determine_range(ret_range_p range, size_t size, off_t offset, seg_info_p segs[], int num_seg);

void get_fileproxy_path(char *fileproxy_path, const char *path_s, int bufsize);

void get_seg_proxy_path(char *seg_proxy_path, const char *md5, int bufsize);

void get_tempseg_path(char *tempseg_path, const char *md5, int bufsize);

int mydedup_segmentation(char *fpath, int &num_seg, std::vector <seg_info_p> & segs);

int ref_of_md5(char *md5);

int mydedup_readsegs_from_proxy(char *file_proxy, seg_info_p segs[]);

int mydedup_uploadfile_append(char *temp_file_path, char *file_proxy, seg_info_p orginal_segs[], int orginal_seg_count);

int mydedup_uploadfile(char *path_s);

int mydedup_extract_and_upload_seg(char *path_s, seg_info_p seg, long offset);

int seg_upload_one(char *pathname, long offset, char *key, long len);

long extract_seg(char *pathname, long offset, long len, char *target_file);

int set_ref(const char *pathname, int value);

int get_ref(const char *pathname, int *value);

void ref_plus_one(char *md5);

void seg_down(char *pathname, char *key);

void seg_upload(char *pathname, char *key, long size);

void debug_showfile(char *file, const char *functionname, int line);

void mydedup_remove_seg(char *md5);

bool file_exist(const char *path_s);

int mydedup_download_all(char *path_s, seg_info_p segs[], int end);

int mydedup_download_and_combine(char *path_s, seg_info_p segs[], int start, int end);

FILE * ffopen_(const char* func, const char * filename, const char * mode );

int ffclose_(const char* func, FILE * s);
