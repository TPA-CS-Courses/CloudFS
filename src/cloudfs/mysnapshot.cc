//
// Created by Wilson_Xu on 2021/11/24.
//
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
#include <sys/time.h>
#include <openssl/md5.h>
#include <time.h>
#include <unistd.h>


#include <vector>
#include <fstream>
#include <string>
#include <map>
#include <unordered_map>

#include "cloudapi.h"
#include "dedup.h"
#include "cloudfs.h"
#include "mydedup.h"
#include "mycache.h"
#include "mysnapshot.h"
#include "snapshot-api.h"

#define BUF_SIZE (1024)
#define SHOWPF

#define UNUSED __attribute__((unused))

#ifdef SHOWPF
#define PF(...) fprintf(sn_cfg->logfile,__VA_ARGS__)
#else
#define PF(...) sizeof(__VA_ARGS__)
#endif

#define NOI() fprintf(sn_cfg->logfile,"INFO: NOT IMPLEMENTED [%s]\t Line:%d \n\n", __func__, __LINE__)

#ifdef SHOWINFO
#define INFO() fprintf(sn_cfg->logfile,"INFO: SSD ONLY [%s]\t Line:%d\n", __func__, __LINE__)
#define INFOF() fprintf(sn_cfg->logfile,"INFO: F [%s]\t Line:%d\n", __func__, __LINE__)
#else
#define INFO() sizeof(void)
#define INFOF() sizeof(void)
#endif

#define FFOPEN__(x, y) ffopen_(__func__, (x), (y))
#define FFCLOSE__(x) ffclose_(__func__, (x))

#define ON_SSD 0
#define ON_CLOUD 1
#define N_DIRTY 0
#define DIRTY 1
#define MAX_SEG_AMOUNT 2048

#define BUCKET ("test")
#define TEMPDIR (".tempfiles")
#define FILEPROXYDIR (".fileproxy")
#define SEGPROXYDIR (".segproxy")
#define TEMPSEGDIR (".tempsegs")
#define SNAPSHOT (".snapshot")
#define SNAPSHOTPATH ("/.snapshot")
#define SNAPPROXY (".snapshotproxy")
#define CACHEDIR (".cache")
#define CACHEMASTER ("cachemaster")

#define SNAPMASTER ("snapmaster")

struct snap_config sn_cfg_s;
struct snap_config *sn_cfg;
std::vector <snap_info*> snapshots;
int installed;

std::string get_path_s_cpp(std::string pathname){
    char path_s[MAX_PATH_LEN];
    get_path_s(path_s, pathname.c_str(), MAX_PATH_LEN);
    std::string ret(path_s);
    return ret;
}

std::string snapmaster_path() {
    char cachemaster_path[MAX_PATH_LEN];
    snprintf(cachemaster_path, MAX_PATH_LEN, "%s%s/%s.snapshotmetadata", sn_cfg->fstate->ssd_path, CACHEDIR, SNAPMASTER);
//    PF("[%s]:cachemaster_path:%s\n", __func__, cachemaster_path);
    std::string ret(cachemaster_path);
    return ret;
}

void mysnap_init(struct cloudfs_state *fstate, FILE *logfile) {

    sn_cfg = &sn_cfg_s;
    sn_cfg->fstate = fstate;
    sn_cfg->logfile = logfile;
    PF("[%s] :", __func__);
    installed = 0;

    char path_s[MAX_PATH_LEN];
    get_path_s(path_s, SNAPSHOT, MAX_PATH_LEN);

    int fd = open(path_s, O_RDONLY | O_CREAT, S_IRUSR | S_IRGRP | S_IROTH);
    close(fd);


}

timestamp_t get_msec() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (timestamp_t)(tv.tv_sec) * 1000 + (timestamp_t)(tv.tv_usec) / 1000;
}

int mysnap_chmod(const char* path_c, const char* mode_c){
    std::string path(path_c);
    std::string mode(mode_c);
    std::string cmd;
    cmd.assign("chmod -R ").append(path).append(" ").append(mode);
//    cmd = "chmod -R ";
//    cmd += path;
//    cmd += " ";
//    cmd += mode;

    PF("[%s] cmd is [%s]\n", __func__, cmd.c_str());
    return system(cmd.c_str());
}

int mysnap_tar(timestamp_t time){
    std::string cmd;
    char path_s[MAX_PATH_LEN];
    std::string tar_path = get_snap_name(time);
    get_path_s(path_s, tar_path.c_str(), MAX_PATH_LEN);

    cmd.assign("tar -zcvf ").append(path_s).append(" -C ").append(sn_cfg->fstate->ssd_path).append(" . --xattrs --exclude lost+found");
//    cmd.assign("tar -zcvf ").append(path_s).append(" -C ").append(sn_cfg->fstate->ssd_path).append(" . --xattrs --exclude lost+found --exclude .cache --exclude .snapshot");
//    cmd = "tar -zcvf ";
//    cmd += path_s;
//
//    cmd += " -C ";
//    cmd += sn_cfg->fstate->ssd_path;
//    cmd += " . --xattrs --exclude lost+found";

//    cmd += " . --xattrs --exclude lost+found --exclude .cache --exclude .snapshot";


    PF("[%s] cmd is [%s]\n", __func__, cmd.c_str());
    return system(cmd.c_str());
}

int mysnap_untar(std::string tar_path_s, std::string dir){
    std::string cmd;
    cmd.assign("tar -zxvf ").append(tar_path_s).append(" -C ").append(dir).append(" --xattrs");
    PF("[%s] cmd is [%s]\n", __func__, cmd.c_str());
    return system(cmd.c_str());
}

std::string get_snap_name(timestamp_t current_time){
    char buf[MAX_PATH_LEN];
    snprintf(buf,MAX_PATH_LEN, ".snapshot.%ld.tar.gz", current_time);
    std::string ret(buf);

    PF("[%s] snap_name is %s\n", __func__, ret.c_str());
    return ret;
}

std::string seg_proxy_path() {
    char retc[MAX_PATH_LEN];
    snprintf(retc, MAX_PATH_LEN, "%s%s", sn_cfg->fstate->ssd_path, SEGPROXYDIR);
//    PF("[%s]: md5: %s\t tempseg_path:%s\n", __func__, md5, seg_proxy_path);
    std::string ret(retc);
    return ret;
}

//add 1 for all the blocks in cloud
void mysnap_cloud_backup(){
    PF("[%s]: \n",__func__);

    std::vector <std::string> segfiles;

    std::string t = seg_proxy_path();
    std::string path = seg_proxy_path();
    DIR * d = opendir(path.c_str());
    if(!d){

        PF("[%s]: d is NULL\n",__func__);
        return;
    }
    struct dirent* ent;
    while((ent=readdir(d)) != NULL){
        std::string f(ent->d_name);
        PF("[%s]: ent: %s\n",__func__, ent->d_name);
        if(f == "." || f == ".."){
            continue;
        }else{
            segfiles.push_back(t.assign(path).append("/").append(f));
        }
    }


    for(int i =0; i<segfiles.size(); i++){
        PF("[%s]: segfiles[%d]: %s\n",__func__, i, segfiles[i].c_str());
    }
    for(int i =0; i<segfiles.size(); i++){
        int refcnt = 0;
        get_ref(segfiles[i].c_str(), &refcnt);
        set_ref(segfiles[i].c_str(), refcnt + 1);//refcnt plus one
    }
}


int mysnap_search(long timestamp){
    int ret = -1;
    for(int i =0; i<snapshots.size();i++){
        if(snapshots[i]->timestamp == timestamp){
            ret = i;
            break;
        }
    }
    return ret;
}
void mysnap_removedir(std::string dirpath){
    DIR *folder_dir = opendir(dirpath.c_str());
    struct dirent *next_file;
    std::string path;

    while ( (next_file = readdir(folder_dir)) != NULL){
        std::string t(next_file->d_name);
        path = get_path_s_cpp(t);
        remove(path.c_str());
    }

    closedir(folder_dir);
}

int mysnap_restore(long timestamp) {

    PF("[%s]:  %ld\n", __func__, timestamp);
    int snapindex = mysnap_search(timestamp);
    if (snapindex < 0){
        PF("[%s]: %ld.snapshot is not in snapshots list\n", __func__, timestamp);
        return -1;
    }

    std::string tar_path = get_snap_name(timestamp);
    std::string tar_path_s= get_path_s_cpp(tar_path);



    for (int i=snapshots.size() - 1; i>snapindex; i--) {
        PF("[%s]: %ld.snapshot removed from cloud\n", __func__, timestamp);
        std::string t = get_snap_name(snapshots[i]->timestamp);
        cloud_delete_object(BUCKET, t.c_str());
    }

    std::string root = sn_cfg->fstate->ssd_path;
    mysnap_removedir(root);


//    outfile=fopen(tar_path_s.c_str(), "wb");

    cloud_get(tar_path_s.c_str(), tar_path.c_str());
//    cloud_get_object(BUCKET, tar_path.c_str(), get_buffer);
    int sysret = mysnap_untar(tar_path_s, root);


    unlink(tar_path_s.c_str());

    sysret = mysnap_chmod(sn_cfg->fstate->ssd_path, "555");

    if (sysret < 0 ) {
        PF("[%s]:sysret < 0 \n", __func__);
        return -1;
    }
    if (!WIFEXITED(sysret)){
        PF("[%s]:!WIFEXITED(sysret)\n", __func__);
        return -1;
    }
    rebuild();
    return 0;
}

timestamp_t mysnap_create() {
    PF("[%s]\n", __func__);
    if (snapshots.size() >= CLOUDFS_MAX_NUM_SNAPSHOTS) {
        PF("[%s] too many snapshots!\n", __func__);
        return -1;
    }
    if (installed > 0) {
        PF("[%s] have snapshot installed!\n", __func__);
        return -2;
    }
    int sysret;
    sysret = mysnap_chmod(sn_cfg->fstate->ssd_path, "+w");

    PF("[%s]:sysret is %d\n", __func__, sysret);


    timestamp_t current_time = get_msec();
    PF("[%s]:current time is %ld\n", __func__, current_time);

    sysret = mysnap_tar(current_time);
    PF("[%s]:sysret is %d\n", __func__, sysret);

    if (sysret < 0 ) {

        PF("[%s]:sysret < 0 \n", __func__);
        return -1;
    }
    if (!WIFEXITED(sysret)){
        PF("[%s]:!WIFEXITED(sysret)\n", __func__);
        return -1;
    }

    std::string tar_path = get_snap_name(current_time);

    std::string tar_path_s= get_path_s_cpp(tar_path);

    struct stat statbuf;
    if (lstat(tar_path_s.c_str(), &statbuf) < 0) {
        return -1;
    }


//
//    infile=FFOPEN__(tar_path_s.c_str(), "rb");
//    cloud_put_object(BUCKET, tar_path.c_str(), statbuf.st_size, put_buffer);
//    FFCLOSE__(infile);

    cloud_put(tar_path_s.c_str(), tar_path.c_str(), statbuf.st_size);
    snap_info *a = new snap_info(current_time, 0);
    snapshots.push_back(a);

    unlink(tar_path_s.c_str());

    mysnap_cloud_backup();
    return current_time;
}


void mysnap_store(){

    std::string snapmaster = snapmaster_path();

    PF("[%s]:into %s\n", __func__, snapmaster.c_str());
    std::ofstream master(snapmaster);
    for (int i = 0; i < snapshots.size(); i++) {
        master << snapshots[i]->timestamp << " " << snapshots[i]->installed << "\n";
    }
    master.close();
}





void mysnap_rebuild(){
    std::string snapmaster = snapmaster_path();
    std::ifstream master(snapmaster);

    PF("[%s]:from %s\n", __func__, snapmaster.c_str());
    snapshots.clear();
    long timestamp_;
    int installed_;
    while(master >> timestamp_ >> installed_){
        snap_info *a = new snap_info(timestamp_, installed_);
        snapshots.push_back(a);
    }

    master.close();
}








