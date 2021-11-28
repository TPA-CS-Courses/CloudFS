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


std::string get_path_s_cpp(std::string pathname);

std::string snapmaster_path();

void mysnap_init(struct cloudfs_state *fstate, FILE *logfile);

timestamp_t get_msec();

int mysnap_chmod(const char *path_c, const char *mode_c);

int mysnap_tar(timestamp_t time);

std::string get_snap_name(timestamp_t current_time);

std::string seg_proxy_path();

//add 1 for all the blocks in cloud
void mysnap_cloud_backup();


int mysnap_search(long timestamp);

void mysnap_removedir(std::string dirpath);

int mysnap_restore(long timestamp);

timestamp_t mysnap_create();


void mysnap_store();


void mysnap_rebuild();


#endif //SRC_MYSNAPSHOT_H
