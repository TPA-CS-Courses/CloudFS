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
#include <ftw.h>


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
//#define SHOWPF

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
#define MASTERDIR (".master")
#define CACHEMASTER ("cache.master")


#define SNAPMASTER ("snap.master")

#define SEEFILE(x) debug_showfile((x), __func__, __LINE__)

struct snap_config sn_cfg_s;
struct snap_config *sn_cfg;
std::vector<snap_info *> snapshots;
int installed;

std::string get_path_s_cpp(std::string pathname) {
    char path_s[MAX_PATH_LEN];
    get_path_s(path_s, pathname.c_str(), MAX_PATH_LEN);
    std::string ret(path_s);
    return ret;
}

std::string snapmaster_path() {
    char cachemaster_path[MAX_PATH_LEN];
    snprintf(cachemaster_path, MAX_PATH_LEN, "%s%s/%s.snapshotmetadata", sn_cfg->fstate->ssd_path, MASTERDIR,
             SNAPMASTER);
//    PF("[%s]:cachemaster_path:%s\n", __func__, cachemaster_path);
    std::string ret(cachemaster_path);
    return ret;
}

void mysnap_init(struct cloudfs_state *fstate, FILE *logfile) {


    sn_cfg = &sn_cfg_s;
    sn_cfg->fstate = fstate;
    sn_cfg->logfile = logfile;

    PF("[%s]:\n", __func__);

    char path_s[MAX_PATH_LEN];
    get_path_s(path_s, SNAPSHOT, MAX_PATH_LEN);
    int fd = open(path_s, O_RDONLY | O_CREAT, S_IRUSR | S_IRGRP | S_IROTH);
    close(fd);

    installed = 0;

    mysnap_rebuild();
    mysnap_store();
}

timestamp_t get_msec() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (timestamp_t) (tv.tv_sec) * 1000 + (timestamp_t) (tv.tv_usec) / 1000;
}

std::string get_snap_seg_proxy(long timestamp) {
    std::string ret;
    ret.assign(sn_cfg->fstate->ssd_path).append(MASTERDIR).append("/").append(std::to_string(timestamp)).append(
            ".snapsegproxy");
    PF("[%s] snap_seg_proxy is %s\n", __func__, ret.c_str());
    return ret;
}

std::string get_snap_seg_proxy_key(long timestamp) {
    std::string ret;
    ret.assign("snapshot").append(std::to_string(timestamp)).append(".snapsegproxy");
    PF("[%s] snap_seg_proxy is %s\n", __func__, ret.c_str());
    return ret;
}


int _nftw_777(const char *fpath, const struct stat *statbuf, int typeflag, struct FTW *ftwbuf) {
    chmod(fpath, 0777);
}

int mysnap_chmod2_777(const char *path_c) {


    std::string path(path_c);
    std::string cmd;


    nftw(path.c_str(), _nftw_777, 64, FTW_DEPTH | FTW_PHYS);


    return 0;
}

int _nftw_RO(const char *fpath, const struct stat *statbuf, int typeflag, struct FTW *ftwbuf) {
    chmod(fpath, 0555);
}

int mysnap_chmod2_555(const char *path_c) {


    std::string path(path_c);
    std::string cmd;


    nftw(path.c_str(), _nftw_RO, 64, FTW_DEPTH | FTW_PHYS);


    return 0;
}

int mysnap_chmod(const char *path_c, const char *mode_c) {


    std::string path(path_c);
    std::string mode(mode_c);
    std::string cmd;


//    nftw(path.c_str(), _nftw_RO, 64, FTW_DEPTH | FTW_PHYS);
    cmd.assign("chmod -R ").append(path).append(" ").append(mode);
//    cmd = "chmod -R ";
//    cmd += path;
//    cmd += " ";
//    cmd += mode;

    PF("[%s] cmd is [%s]\n", __func__, cmd.c_str());
    return system(cmd.c_str());
//    return 0;
}

int mysnap_tar(timestamp_t time) {
    std::string cmd;
    char path_s[MAX_PATH_LEN];
    std::string tar_key = get_snap_key(time);
    get_path_s(path_s, tar_key.c_str(), MAX_PATH_LEN);

    cmd.assign("tar -zcvf ").append(path_s).append(" -C ").append(sn_cfg->fstate->ssd_path).append(
            " . --xattrs --exclude lost+found");
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

int mysnap_untar(std::string tar_path_s, std::string dir) {
    std::string cmd;
    cmd.assign("tar -zxvf ").append(tar_path_s).append(" -C ").append(dir).append(" --xattrs");
    PF("[%s] cmd is [%s]\n", __func__, cmd.c_str());
    return system(cmd.c_str());
}

std::string get_snap_key(timestamp_t current_time) {
    char buf[MAX_PATH_LEN];
    snprintf(buf, MAX_PATH_LEN, ".snapshot.%ld.tar.gz", current_time);
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

std::string seg_proxy_path_to_md5(std::string seg_proxy_path) {
    std::string substring1;
    std::string substring2;
    std::string string;
    substring1.assign(sn_cfg->fstate->ssd_path).append(SEGPROXYDIR).append("/");
    substring2.assign(".segproxy");
    string.assign(seg_proxy_path);

    PF("[%s] original string is %s\n", __func__, string.c_str());
    PF("[%s] substring1 is %s\n", __func__, substring1.c_str());
    PF("[%s] substring2 is %s\n", __func__, substring2.c_str());

    string.erase(string.find(substring1), substring1.size());
    string.erase(string.find(substring2), substring2.size());


    PF("[%s] ret string is %s\n", __func__, string.c_str());
    //hopefully this works

    return string;

}


void mysnap_remove_segs(long timestamp) {

    std::string ssppath = get_snap_seg_proxy(timestamp);
    std::string sspkey = get_snap_seg_proxy_key(timestamp);
    PF("[%s] snap_seg_proxy at %s\n", __func__, ssppath.c_str());

    cloud_get(ssppath.c_str(), sspkey.c_str());


    if (!file_exist(ssppath.c_str())) {
        PF("[%s] ERROR %s NOT EXIST\n", __func__, ssppath.c_str());
        return;
    }
    std::ifstream ifs(ssppath);
    std::string segfilepath;
    while (ifs >> segfilepath) {
        PF("[%s]: segfile is  %s\n", __func__, segfilepath.c_str());
        if (!file_exist(segfilepath.c_str())) {
            PF("[%s]: ERROR %s does not exist!!!\n", __func__, segfilepath.c_str());
            return;
        }
        int ref;
        get_ref(segfilepath.c_str(), &ref);
        if (ref > 1) {
            set_ref(segfilepath.c_str(), ref - 1);
            PF("[%s]: set ref of %s as ref-1 = %d\n", __func__, segfilepath.c_str(), ref - 1);
//            PF("[%s]: key is %s for %s \n", __func__, seg_proxy_path_to_md5(segfilepath).c_str(), segfilepath.c_str());
        } else if (ref == 1) {
            cloud_delete_cache(seg_proxy_path_to_md5(segfilepath).c_str());
//        cloud_delete_object(BUCKET, md5);
            remove(segfilepath.c_str());
            PF("[%s]: removed key %s from cloud, removed %s\n", __func__, seg_proxy_path_to_md5(segfilepath).c_str(),
               segfilepath.c_str());
        } else if (ref < 1) {

            PF("[%s]: ERROR %s with 0 refcnt is not deleted!!!\n", __func__, segfilepath.c_str());
            return;
        }

    }

    ifs.close();

    unlink(ssppath.c_str());
    cloud_delete_object(BUCKET, sspkey.c_str());
}

//add 1 for all the blocks in cloud
void mysnap_cloud_backup(long timestamp) {
    PF("[%s]: \n", __func__);

    std::vector <std::string> segfiles;

    std::string temp = seg_proxy_path();
    std::string path = seg_proxy_path();
    DIR *d = opendir(path.c_str());
    if (!d) {
        PF("[%s]: d is NULL\n", __func__);
        return;
    }
    struct dirent *ent;
    std::vector <std::string> segs;
    while ((ent = readdir(d)) != NULL) {
        std::string f(ent->d_name);
        PF("[%s]: ent: %s\n", __func__, ent->d_name);
        if (f == "." || f == "..") {
            continue;
        } else {
            segfiles.push_back(temp.assign(path).append("/").append(f));
        }
    }
    std::string sspkey = get_snap_seg_proxy_key(timestamp);
    std::string ssppath = get_snap_seg_proxy(timestamp);
    FILE *ssproxy = FFOPEN__(ssppath.c_str(), "w");

    for (int i = 0; i < segfiles.size(); i++) {
        PF("[%s]: segfiles[%d]: %s\n", __func__, i, segfiles[i].c_str());
        fprintf(ssproxy, "%s\n", segfiles[i].c_str());
    }

    FFCLOSE__(ssproxy);
    struct stat statbuf;
    lstat(ssppath.c_str(), &statbuf);
    cloud_put(ssppath.c_str(), sspkey.c_str(), statbuf.st_size);

    unlink(ssppath.c_str());


    for (int i = 0; i < segfiles.size(); i++) {
        int refcnt = 0;
        get_ref(segfiles[i].c_str(), &refcnt);
        set_ref(segfiles[i].c_str(), refcnt + 1);//refcnt plus one
    }
}


int mysnap_search(long timestamp) {
    int ret = -1;
    for (int i = 0; i < snapshots.size(); i++) {
        if (snapshots[i]->timestamp == timestamp) {
            ret = i;
            break;
        }
    }
    return ret;
}

void mysnap_removedir(std::string dirpath) {
    rmrf(dirpath.c_str());

//    DIR *folder_dir = opendir(dirpath.c_str());
//    struct dirent *next_file;
//    std::string path;
//
//    while ((next_file = readdir(folder_dir)) != NULL) {
//        std::string t(next_file->d_name);
//        path = get_path_s_cpp(t);
//        remove(path.c_str());
//    }
//
//    closedir(folder_dir);
}

std::string get_install_path(long timestamp) {

    char buf[MAX_PATH_LEN];
    snprintf(buf, MAX_PATH_LEN, "snapshot_%ld/", timestamp);
    std::string ret;
    ret.assign(sn_cfg->fstate->ssd_path).append(buf);
    PF("[%s]: install path is %s\n", __func__, ret.c_str());
    return ret;
}


int mysnap_list(long *snapshots_ret) {
    int i;
    for (i = 0; i < snapshots.size(); i++) {
        snapshots_ret[i] = snapshots[i]->timestamp;
    }
    snapshots_ret[i] = 0;
    return 0;
}

int mysnap_delete(long timestamp) {
    PF("[%s]:  %ld\n", __func__, timestamp);
    int snapindex = mysnap_search(timestamp);
    if (snapindex < 0) {
        PF("[%s]: %ld.snapshot is not in snapshots list\n", __func__, timestamp);
        return -1;
    }
    if (snapshots[snapindex]->installed == 1) {
        PF("[%s]: %ld.snapshot is installed\n", __func__, timestamp);
        if (mysnap_uninstall(timestamp) < 0) {
            return -1;
        }
    }

    mysnap_remove_segs(timestamp);


    std::string tar_path = get_snap_key(timestamp);


    cloud_delete_object(BUCKET, tar_path.c_str());

    snapshots.erase(snapshots.begin() + snapindex);


    if(snapshots.size()==0){
        cloud_delete_object(BUCKET, "snapshotmaster.metadata");
    }

    return 0;

}


int mysnap_uninstall(long timestamp) {
    PF("[%s]:  %ld\n", __func__, timestamp);
    int snapindex = mysnap_search(timestamp);
    if (snapindex < 0) {
        PF("[%s]: %ld.snapshot is not in snapshots list\n", __func__, timestamp);
        return -1;
    }
    if (snapshots[snapindex]->installed == 0) {

        PF("[%s]: %ld.snapshot not installed\n", __func__, timestamp);
        return -1;
    }

    std::string install_path = get_install_path(timestamp);

    int sysret = mysnap_chmod2_777(install_path.c_str());
//    int sysret = mysnap_chmod2_777(install_path.c_str(), "777");

    if (sysret < 0) {
        PF("[%s]:sysret < 0 \n", __func__);
        return -1;
    }
    if (!WIFEXITED(sysret)) {
        PF("[%s]:!WIFEXITED(sysret)\n", __func__);
        return -1;
    }

    mysnap_removedir(install_path);
    installed--;
    snapshots[snapindex]->installed = 0;

    return 0;
}

int mysnap_install(long timestamp) {
    PF("[%s]:  %ld\n", __func__, timestamp);
    int snapindex = mysnap_search(timestamp);
    if (snapindex < 0) {
        PF("[%s]: %ld.snapshot is not in snapshots list\n", __func__, timestamp);
        return -1;
    }
    if (snapshots[snapindex]->installed == 1) {
        PF("[%s]: %ld.snapshot already installed\n", __func__, timestamp);
        return -1;
    }

    std::string install_path = get_install_path(timestamp);
    PF("[%s]: %ld.snapshot install at %s\n", __func__, timestamp, install_path.c_str());
    int ret;
    ret = mkdir(install_path.c_str(), 0777);
    if (ret < 0) {
        return -errno;
    }

    std::string tar_path = get_snap_key(timestamp);
    std::string tar_path_s = get_path_s_cpp(tar_path);

    cloud_get(tar_path_s.c_str(), tar_path.c_str());

    int sysret = mysnap_untar(tar_path_s, install_path);
    if (sysret < 0) {
        PF("[%s]:sysret < 0 \n", __func__);
        return -1;
    }
    if (!WIFEXITED(sysret)) {
        PF("[%s]:!WIFEXITED(sysret)\n", __func__);
        return -1;
    }

    sysret = mysnap_chmod2_555(install_path.c_str());
//    sysret = mysnap_chmod(install_path.c_str(), "555");
    //readonly


    if (sysret < 0) {
        PF("[%s]:sysret < 0 \n", __func__);
        return -1;
    }
    if (!WIFEXITED(sysret)) {
        PF("[%s]:!WIFEXITED(sysret)\n", __func__);
        return -1;
    }
    snapshots[snapindex]->installed = 1;
    installed++;
    return 0;
}

int mysnap_restore(long timestamp) {
//    PF("[%s] testing delete!\n",__func__);
//    mysnap_delete(timestamp);

    if (installed > 0) {
        PF("[%s] You have snapshot installed!\n", __func__);
        return -1;
    }

    PF("[%s]:  %ld\n", __func__, timestamp);
    int snapindex = mysnap_search(timestamp);
    if (snapindex < 0) {
        PF("[%s]: %ld.snapshot is not in snapshots list\n", __func__, timestamp);
        return -1;
    }

    std::string tar_path = get_snap_key(timestamp);
    std::string tar_path_s = get_path_s_cpp(tar_path);


    for (int i = snapshots.size() - 1; i > snapindex; i--) {
        PF("[%s]: %ld.snapshot removed from cloud\n", __func__, timestamp);
        std::string key = get_snap_key(snapshots[i]->timestamp);
        cloud_delete_object(BUCKET, key.c_str());
        mysnap_remove_segs(snapshots[i]->timestamp);
    }

    std::string root = sn_cfg->fstate->ssd_path;
    mysnap_removedir(root);


//    outfile=fopen(tar_path_s.c_str(), "wb");

    cloud_get(tar_path_s.c_str(), tar_path.c_str());
//    cloud_get_object(BUCKET, tar_path.c_str(), get_buffer);
    int sysret = mysnap_untar(tar_path_s, root);

    restore_chmod(root.c_str());
//    restore_chmod(root.c_str());
    if (sysret < 0) {
        PF("[%s]:sysret < 0 \n", __func__);
        return -1;
    }
    if (!WIFEXITED(sysret)) {
        PF("[%s]:!WIFEXITED(sysret)\n", __func__);
        return -1;
    }


    unlink(tar_path_s.c_str());

//    sysret = mysnap_chmod2_555(sn_cfg->fstate->ssd_path);
//    sysret = mysnap_chmod(sn_cfg->fstate->ssd_path, "555");


    if (sysret < 0) {
        PF("[%s]:sysret < 0 \n", __func__);
        return -1;
    }
    if (!WIFEXITED(sysret)) {
        PF("[%s]:!WIFEXITED(sysret)\n", __func__);
        return -1;
    }
    //TODO   WHAT TO DO HERE??
    mycache_rebuild();
    mysnap_rebuild();
//    std::string lscmd;
//    lscmd.assign("ls -l ").append(sn_cfg->fstate->ssd_path).append(" > /tmp/afterrestore.log");
//    system(lscmd.c_str());
    return 0;
}

timestamp_t mysnap_create() {
//    std::string lscmd;
//    lscmd.assign("ls -l ").append(sn_cfg->fstate->ssd_path).append(" > /tmp/prev-ls.log");
//    system(lscmd.c_str());
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
//    store_chmod(sn_cfg->fstate->ssd_path);
//    sysret = mysnap_chmod2_777(sn_cfg->fstate->ssd_path);


//    lscmd.assign("ls -l ").append(sn_cfg->fstate->ssd_path).append(" > /tmp/after777.log");
//    system(lscmd.c_str());


    PF("[%s]:sysret is %d\n", __func__, sysret);


    timestamp_t current_time = get_msec();
    PF("[%s]:current time is %ld\n", __func__, current_time);
    store_chmod(sn_cfg->fstate->ssd_path);
    mysnap_chmod2_777(sn_cfg->fstate->ssd_path);


    sysret = mysnap_tar(current_time);
    PF("[%s]:sysret is %d\n", __func__, sysret);

    if (sysret < 0) {

        PF("[%s]:sysret < 0 \n", __func__);
        return -1;
    }
    if (!WIFEXITED(sysret)) {
        PF("[%s]:!WIFEXITED(sysret)\n", __func__);
        return -1;
    }

    std::string tar_path = get_snap_key(current_time);

    std::string tar_path_s = get_path_s_cpp(tar_path);

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

    mysnap_cloud_backup(current_time);






//    PF("[%s] testing delete!\n",__func__);
//    mysnap_delete(current_time);
    return current_time;
}

void mysnap_store() {

    if(snapshots.size() ==0){
        PF("[%s]Nothing to store here\n", __func__);
        cloud_delete_object(BUCKET, "snapshotmaster.metadata");
        return;
    }
    std::string snapmaster = snapmaster_path();

    PF("[%s]:into %s\n", __func__, snapmaster.c_str());
    std::ofstream master(snapmaster);
    for (int i = 0; i < snapshots.size(); i++) {
        master << snapshots[i]->timestamp << " " << snapshots[i]->installed << "\n";
    }

    master.close();
    struct stat statbuf;
    if (lstat(snapmaster.c_str(), &statbuf) < 0) {
        unlink(snapmaster.c_str());
        return;
    }
    cloud_delete_object(BUCKET, "snapshotmaster.metadata");
    cloud_put(snapmaster.c_str(), "snapshotmaster.metadata", statbuf.st_size);
    unlink(snapmaster.c_str());
}


void mysnap_rebuild() {
    std::string snapmaster = snapmaster_path();

    outfile = FFOPEN__(snapmaster.c_str(), "wb");
    cloud_get_object(BUCKET, "snapshotmaster.metadata", get_buffer);
    FFCLOSE__(outfile);
    struct stat statbuf;
    if (lstat(snapmaster.c_str(), &statbuf) < 0) {
        unlink(snapmaster.c_str());
        return;
    }
    if (statbuf.st_size == 0) {
        PF("[%s] %s not exist\n", __func__, snapmaster.c_str());
        unlink(snapmaster.c_str());
        return;
    }


    std::ifstream master(snapmaster);

    PF("[%s]:from %s\n", __func__, snapmaster.c_str());
    snapshots.clear();
    long timestamp_;
    int installed_;
    while (master >> timestamp_ >> installed_) {
        snap_info *a = new snap_info(timestamp_, installed_);
        snapshots.push_back(a);
    }
    master.close();

    installed = 0;
    for (int i = 0; i < snapshots.size(); i++) {
        if (snapshots[i]->installed == 1) {
            installed += 1;
        }
    }

    unlink(snapmaster.c_str());


}


//void mysnap_store() {
//
//    std::string snapmaster = snapmaster_path();
//
//    PF("[%s]:into %s\n", __func__, snapmaster.c_str());
//    std::ofstream master(snapmaster.c_str());
//    PF("[%s]:create %s\n", __func__, snapmaster.c_str());
//    for (int i = 0; i < snapshots.size(); i++) {
//        master << snapshots[i]->timestamp << " " << snapshots[i]->installed << "\n";
//        PF("[%s]:into %s\n saved %ld, %d\n", __func__, snapmaster.c_str(),snapshots[i]->timestamp, snapshots[i]->installed );
//    }
//    master.close();
//    SEEFILE(snapmaster.c_str());
//
//}
//
//
//void mysnap_rebuild() {
//
//    std::string snapmaster = snapmaster_path();
//    PF("[%s]:from %s\n", __func__, snapmaster.c_str());
//
//    outfile = FFOPEN__(snapmaster.c_str(), "wb");
//    cloud_get_object(BUCKET, "snapshotmaster.metadata", get_buffer);
//    FFCLOSE__(outfile);
//
//    if (!file_exist(snapmaster.c_str())) {
//        PF("[%s] %s not exist\n", __func__, snapmaster.c_str());
//        return;
//    }
//
//    struct stat statbuf;
//    lstat(snapmaster.c_str(), &statbuf);
//    if (statbuf.st_size == 0) {
//        PF("[%s] %s is empty\n", __func__, snapmaster.c_str());
//        return;
//    }
//
//
//    SEEFILE(snapmaster.c_str());
//
//
//    std::ifstream master(snapmaster);
//
//    PF("[%s]:from %s\n", __func__, snapmaster.c_str());
//    snapshots.clear();
//    long timestamp_;
//    int installed_;
//    while (master >> timestamp_ >> installed_) {
//        snap_info *a = new snap_info(timestamp_, installed_);
//        snapshots.push_back(a);
//    }
//    master.close();
//
//    installed = 0;
//    for (int i = 0; i < snapshots.size(); i++) {
//        if (snapshots[i]->installed == 1) {
//            installed += 1;
//        }
//    }
//}


int unlink_(const char *fpath, const struct stat *sb, int typeflag, struct FTW *ftwbuf) {

    int ret = remove(fpath);
    return ret;
}

int rmrf(const char *path) {
    PF("[%s]:\t REMOVING %s\n", __func__, path);
    nftw(path, unlink_, 64, FTW_DEPTH | FTW_PHYS);
    return 0;
}

int store_chmod_(const char *fpath, const struct stat *sb, int typeflag, struct FTW *ftwbuf) {

    struct stat statbuf;
    lstat(fpath, &statbuf);
    chmod(fpath, 0666);
    lsetxattr(fpath, "user.chmod_mode", &(statbuf.st_mode), sizeof(mode_t), 0);
    fprintf(sn_cfg->logfile, "[%s]:\t stored %s mode %zu to user.chmod_mode\n", __func__, fpath, statbuf.st_mode);
    chmod(fpath, statbuf.st_mode);
    return 0;
}

int store_chmod(const char *path) {
    return nftw(path, store_chmod_, 64, FTW_DEPTH | FTW_PHYS);
}

int restore_chmod_(const char *fpath, const struct stat *sb, int typeflag, struct FTW *ftwbuf) {
    mode_t mode;
    int ret = lgetxattr(fpath, "user.chmod_mode", &mode, sizeof(mode_t));
    if (ret < 0) {
        PF("[%s]:\t no mode saved\n", __func__);
        return 0;
    }else{
        fprintf(sn_cfg->logfile, "[%s]:\t restored %s mode %zu to st_mode\n", __func__, fpath, mode);
    }
    chmod(fpath, mode);
    return 0;
}

int restore_chmod(const char *path) {
    return nftw(path, restore_chmod_, 64, FTW_DEPTH | FTW_PHYS);
}





