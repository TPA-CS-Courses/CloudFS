//
// Created by Wilson_Xu on 2021/11/24.
//

#ifndef SRC_MYSNAPSHOT_H
#define SRC_MYSNAPSHOT_H

typedef long timestamp_t;

struct snap_config {
    struct cloudfs_state *fstate;
    FILE *logfile;
};

struct snap_info {
    long timestamp;
    int installed;

    snap_info(long _timestamp, int _installed) : timestamp(_timestamp), installed(_installed) {}
};

typedef struct seg_info_c {
    long seg_size;
    std::string md5;

    seg_info_c(long _seg_size, std::string _md5) : seg_size(_seg_size), md5(_md5) {}
} seg_info_c_t, *seg_info_c_p;


std::string get_path_s_cpp(std::string pathname);

std::string snapmaster_path();

void mysnap_init(struct cloudfs_state *fstate, FILE *logfile);

timestamp_t get_msec();

int mysnap_chmod(const char *path_c, const char *mode_c);

int mysnap_tar(timestamp_t time);

std::string get_snap_key(timestamp_t current_time);

std::string seg_proxy_path();

//add 1 for all the blocks in cloud
void mysnap_cloud_backup(long timestamp);


int mysnap_search(long timestamp);

void mysnap_removedir(std::string dirpath);

int mysnap_restore(long timestamp);

timestamp_t mysnap_create();

int mysnap_install(long timestamp);


void mysnap_store();


void mysnap_rebuild();

int mysnap_uninstall(long timestamp);

int mysnap_delete(long timestamp);

int mysnap_list(long *snapshots_ret);

std::string get_snap_seg_proxy_key(long timestamp);


int _nftw_777(const char *fpath, const struct stat *statbuf, int typeflag, struct FTW *ftwbuf);

int mysnap_chmod2_777(const char *path_c);

int _nftw_RO(const char *fpath, const struct stat *statbuf, int typeflag, struct FTW *ftwbuf);

int mysnap_chmod2_555(const char *path_c);


#endif //SRC_MYSNAPSHOT_H
