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

int cloudfs_start(struct cloudfs_state *state,
                  const char *fuse_runtime_name);
int get_loc(const char *pathname, int *value);
int set_loc(const char *pathname, int *value);
int get_dirty(const char *pathname, int *value);
int set_dirty(const char *pathname, int value);
int clone_2_proxy(char *path_s, struct stat *statbuf_p);
int get_from_proxy(char *path_s, struct stat *statbuf_p);
void cloud_put(char *path_s, char *path_c, struct stat *statbuf_p);
bool is_on_cloud(char *pathname);
int put_buffer(char *buffer, int bufferLength);
int get_buffer(const char *buffer, int bufferLength);

void cloudfs_get_fullpath(const char *path, char *fullpath);

#endif
