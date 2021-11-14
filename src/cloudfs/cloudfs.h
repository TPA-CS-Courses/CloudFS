#ifndef __CLOUDFS_H_
#define __CLOUDFS_H_

#define MAX_PATH_LEN 4096
#define MAX_HOSTNAME_LEN 1024

struct cloudfs_state {
    char ssd_path[MAX_PATH_LEN];
    char fuse_path[MAX_PATH_LEN];
    char hostname[MAX_HOSTNAME_LEN];
    int ssd_size;
    int threshold;
    int avg_seg_size;
    int min_seg_size;
    int max_seg_size;
    int cache_size;
    int rabin_window_size;
    char no_dedup;
};

int get_buffer(const char *buffer, int bufferLength);
int put_buffer(char *buffer, int bufferLength);
int cloudfs_start(struct cloudfs_state *state,
                  const char *fuse_runtime_name);
int get_loc(const char *pathname, int *value);
int set_loc(const char *pathname, int value);
int get_dirty(const char *pathname, int *value);
int set_dirty(const char *pathname, int value);
void get_path_t(char *path_t, const char *pathname, int bufsize);
void get_path_s(char *full_path, const char *pathname, int bufsize);
void get_dir_seg(char *full_path, const char *pathname, int bufsize);
void get_path_c(char *path_c, const char *path_s);
int clone_2_proxy(char *path_s, struct stat *statbuf_p);
void cloud_put(char *path_s, char *path_c, long size);
void cloud_get(char *path_s, char *path_c);
bool is_on_cloud(char *pathname);
int put_buffer(char *buffer, int bufferLength);
int get_buffer(const char *buffer, int bufferLength);
int cloudfs_read_de(const char *pathname, char *buf, size_t size, off_t offset, struct fuse_file_info *fi);
int cloudfs_read_node(const char *pathname, char *buf, size_t size, off_t offset, struct fuse_file_info *fi);
int cloudfs_error(const char *error_str);
void cloudfs_get_fullpath(const char *path, char *fullpath);
int cloudfs_getattr(const char *pathname, struct stat *statbuf);
int get_from_proxy(const char *path_s, struct stat *statbuf_p);
#endif
