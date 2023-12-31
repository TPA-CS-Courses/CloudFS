/**
 * @file cloudfs.cc
 * @brief CloudFS main program
 *
 * This is a program simulating the behavior of a FTL controller in an SSD.
 *
 * Mapping scheme: page mapping
 * Mapping is done using two tables
 * pba mapping table (PMT) maps every LBA to PBA(PMT[LBA] = PBA)
 * lba mapping table (LMT) maps every PBA to LBA(LMT[PBA] = LBA)
 * both tables are initialized with values that they should not be able to reach.
 * So we can determine wether the address has been mapped or not.
 *
 * Garbage Collection Policy: Threshold controlled most valid or youngest
 * The Garbage Collection Will choose the youngest block to clean or the block that have least valid pages.
 * Which policy GC will choose is controlled by a POLICY_THRESHOLD.
 * If total writes is less than the threshold, the GC algorithm will choose the block that have least valid pages
 * If total writes exceeds the threshold, the GC algorithm will choose the youngest block
 *
 * Garbage Collection Method: all blocks except one block is reserved for cleaning buffering
 * When all other blocks are filled up. GC will choose one data block to clean.
 * It copies all the valid pages into that reserved block and erase the original block.
 * The original block is marked as the new reserved block and the current block pointer
 * is pointed at the old reserved block so new data will be written there.
 *
 * Wear Leveling: The main point of Wear performance improvement is
 * 1. Finer grain mapping: page mapping
 * 2. Always Choosing Youngest empty block to write
 * 3. Garbage Collection Policy
 *
 * The wear of the blocks are tracked using wear count table WCT[]
 * it takes physical block index and returns the wear of that block
 *
 * @author Liangdong Xu <liangdox@andrew.cmu.edu>
 */
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

#include <vector>
#include <fstream>
#include <string>
#include <map>

#include <openssl/md5.h>
#include <time.h>
#include <unistd.h>

#include "cloudapi.h"
#include "dedup.h"
#include "cloudfs.h"
#include "mydedup.h"
#include "mycache.h"
#include "mysnapshot.h"
#include "snapshot-api.h"


//#define SHOWPF

#define UNUSED __attribute__((unused))

#ifdef SHOWPF
#define PF(...) fprintf(logfile,__VA_ARGS__)
#else
#define PF(...) sizeof(__VA_ARGS__)
#endif

#define NOI() fprintf(logfile,"INFO: NOT IMPLEMENTED [%s]\t Line:%d \n\n", __func__, __LINE__)

#ifdef SHOWINFO
#define INFO() fprintf(logfile,"INFO: SSD ONLY [%s]\t Line:%d\n", __func__, __LINE__)
#define INFOF() fprintf(logfile,"INFO: F [%s]\t Line:%d\n", __func__, __LINE__)
#else
#define INFO() sizeof(void)
#define INFOF() sizeof(void)
#endif

#define TRY(x) \
    if ((ret = x) < 0) { \
        ret = cloudfs_error(__func__); \
    }

#define RUN_M(x) \
    if ((x) < 0) { \
        return cloudfs_error(__func__); \
    }


#define SEEFILE(x) debug_showfile((x), __func__, __LINE__)

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
//#define SNAPPROXY (".snapshotproxy")
#define CACHEDIR (".cache")
#define MASTERDIR (".master")
#define CACHEMASTER ("cache.master")


static struct cloudfs_state state_;
static struct cloudfs_state *fstate;
FILE *logfile;
static struct fuse_operations cloudfs_operations;

FILE *infile;
FILE *outfile;

int get_buffer(const char *buffer, int bufferLength) {

//    PF("[%s]get buffer %d\n",__func__, bufferLength);
    return fwrite(buffer, 1, bufferLength, outfile);
}


int put_buffer(char *buffer, int bufferLength) {
//    PF("[%s]put buffer %d\n",__func__, bufferLength);
    return fread(buffer, 1, bufferLength, infile);
}

//char ssd_path[MAX_PATH_LEN];
//char fuse_path[MAX_PATH_LEN];
//char hostname[MAX_HOSTNAME_LEN];
//int ssd_size;
//int threshold;
//int avg_seg_size;
//int min_seg_size;
//int max_seg_size;
//int cache_size;
//int rabin_window_size;
//char no_dedup;


//struct stat {
//    dev_t     st_dev;         /* ID of device containing file */
//    ino_t     st_ino;         /* Inode number */
//    mode_t    st_mode;        /* File type and mode */
//    nlink_t   st_nlink;       /* Number of hard links */
//    uid_t     st_uid;         /* User ID of owner */
//    gid_t     st_gid;         /* Group ID of owner */
//    dev_t     st_rdev;        /* Device ID (if special file) */
//    off_t     st_size;        /* Total size, in bytes */
//    blksize_t st_blksize;     /* Block size for filesystem I/O */
//    blkcnt_t  st_blocks;      /* Number of 512B blocks allocated */
//
//    /* Since Linux 2.6, the kernel supports nanosecond
//       precision for the following timestamp fields.
//       For the details before Linux 2.6, see NOTES. */
//
//    struct timespec st_atim;  /* Time of last access */
//    struct timespec st_mtim;  /* Time of last modification */
//    struct timespec st_ctim;  /* Time of last status change */
//
//#define st_atime st_atim.tv_sec      /* Backward compatibility */
//#define st_mtime st_mtim.tv_sec
//#define st_ctime st_ctim.tv_sec
//};

int cloudfs_error(const char *error_str) {
    int ret = -errno;

    // TODO:
    //
    // You may want to add your own logging/debugging functions for printing
    // error messages. For example:
    //
    // debug_msg("ERROR happened. %s\n", error_str, strerror(errno));
    //

    fprintf(stderr, "CloudFS Error: %s\n", error_str);
//    fprintf(logfile, "[CloudFS Error]: %s \t[ERRNO]: %d: %s\n", error_str, -ret, strerror(errno));

    /* FUSE always returns -errno to caller (yes, it is negative errno!) */
    return ret;
}

/*
 * Initializes the FUSE file system (cl udfs) by checking if the mount points
 * are valid, and if all is well, it mounts the file system ready for usage.
 *
 */
void *cloudfs_init(struct fuse_conn_info *conn UNUSED) {

    char temp_dir[MAX_PATH_LEN] = TEMPDIR;

    char temp_dir_ssd[MAX_PATH_LEN];


    cloud_init(state_.hostname);
    cloud_print_error();

    cloud_create_bucket(BUCKET);
    cloud_print_error();

    snprintf(temp_dir_ssd, MAX_PATH_LEN, "%s%s", fstate->ssd_path, TEMPDIR);
    mkdir(temp_dir_ssd, S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);

//    temp_dir = FILEPROXYDIR;
    snprintf(temp_dir_ssd, MAX_PATH_LEN, "%s%s", fstate->ssd_path, FILEPROXYDIR);
    mkdir(temp_dir_ssd, S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);


    snprintf(temp_dir_ssd, MAX_PATH_LEN, "%s%s", fstate->ssd_path, SEGPROXYDIR);
    mkdir(temp_dir_ssd, S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);

    snprintf(temp_dir_ssd, MAX_PATH_LEN, "%s%s", fstate->ssd_path, TEMPSEGDIR);
    mkdir(temp_dir_ssd, S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);

    snprintf(temp_dir_ssd, MAX_PATH_LEN, "%s%s", fstate->ssd_path, CACHEDIR);
    mkdir(temp_dir_ssd, S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);

    snprintf(temp_dir_ssd, MAX_PATH_LEN, "%s%s", fstate->ssd_path, MASTERDIR);
    mkdir(temp_dir_ssd, S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);

    mydedup_init(fstate->rabin_window_size, fstate->avg_seg_size, fstate->min_seg_size,
                 fstate->max_seg_size, logfile, fstate);

    mysnap_init(fstate, logfile);



//    std::string cachemaster_path = cachemaster_path_();
//
//    std::ofstream master(cachemaster_path.c_str());
//
//
//    master.close();
//
//
//
//    std::string snapmaster = snapmaster_path();
//
//    std::ofstream master2(snapmaster.c_str());
//
//    master2.close();


    PF("[%s]%d\n",__func__, __LINE__);
//    get_path_s(targetPath, SNAPPROXY, MAX_PATH_LEN);
//    FILE *f = FFOPEN__(targetPath, "w");
//    FFCLOSE__(f);

    return NULL;
}

void get_snapshot_path(char *snapshot_path, int bufsize) {

    snprintf(snapshot_path, bufsize, "%s%s", fstate->ssd_path, SNAPSHOT);
    PF("[%s]: snapshot_path:%s\n", __func__, snapshot_path);
}

void cloudfs_destroy(void *data UNUSED) {


    cloud_delete_bucket(BUCKET);
    cloud_print_error();

    cloud_destroy();
    cloud_print_error();

    mydedup_destroy();
    mycache_destroy();
}

void get_path_s(char *full_path, const char *pathname, int bufsize) {
    INFOF();
    if (pathname[0] == '/') {
        snprintf(full_path, bufsize, "%s%s", fstate->ssd_path, pathname + 1);
    } else {
        snprintf(full_path, bufsize, "%s%s", fstate->ssd_path, pathname);
    }

//    PF("[%s]\t pathname is %s\n", __func__, pathname);
}

//void get_path_f(char *full_path, const char *pathname, int bufsize) {
//    INFOF();
//    if (pathname[0] == '/') {
//        snprintf(full_path, bufsize, "%s%s", fstate->fuse_path, pathname + 1);
//    } else {
//        snprintf(full_path, bufsize, "%s%s", fstate->fuse_path, pathname);
//    }
//
//    PF("[%s]\t pathname is %s\n", __func__, pathname);
//}

int cloudfs_ioctl(const char *path, int cmd, void *arg,
                  struct fuse_file_info *fi, unsigned int flags,
                  void *data) {
    if (strcmp(path, SNAPSHOTPATH) != 0) {
        return -1;
    }
    PF("[%s]\n",__func__);
    if (cmd == CLOUDFS_SNAPSHOT) {
        PF("[%s]cmd == CLOUDFS_SNAPSHOT\n",__func__);
        timestamp_t ret = mysnap_create();
        if(ret < 0){

            return cloudfs_error(__func__);

        }
        mysnap_store();

        *(unsigned long*)data=(unsigned long)ret;
        return 0;
    }
    else if (cmd==CLOUDFS_SNAPSHOT_LIST) {

        PF("[%s]cmd==CLOUDFS_SNAPSHOT_LIST\n",__func__);
        NOI();
        mysnap_list((long*)data);
        return 0;
    } else if (cmd==CLOUDFS_INSTALL_SNAPSHOT) {


        PF("[%s]cmd==CLOUDFS_INSTALL_SNAPSHOT\n",__func__);
        NOI();


        int ret=mysnap_install(*(long*)data);

        PF("[%s] testing uninstall!\n",__func__);
//        mysnap_uninstall(*(long*)data);
//        ret=mysnap_install(*(long*)data);
//        mysnap_store();
        return ret;
    } else if (cmd==CLOUDFS_UNINSTALL_SNAPSHOT) {

        int ret=mysnap_uninstall(*(long*)data);

        PF("[%s]cmd==CLOUDFS_INSTALL_SNAPSHOT\n",__func__);
        NOI();
        mysnap_store();
        return ret;
    } else if (cmd==CLOUDFS_DELETE) {
        int ret=mysnap_delete(*(long*)data);

        PF("[%s]cmd==CLOUDFS_DELETE\n",__func__);
        NOI();
        mysnap_store();
        return ret;
    } else if (cmd==CLOUDFS_RESTORE) {
        PF("[%s]cmd==CLOUDFS_RESTORE\n",__func__);
//        NOI();

        int ret=mysnap_restore(*(long*)data);
        mysnap_store();
        return ret;
    }
}


void get_path_t(char *path_t, const char *pathname, int bufsize) {
    INFOF();
    char path_s[MAX_PATH_LEN];
    char path_c[MAX_PATH_LEN];
    char temp_dir[MAX_PATH_LEN] = TEMPDIR;
    if (pathname[0] == '/') {
        snprintf(path_s, bufsize, "%s%s", fstate->ssd_path, pathname + 1);
    } else {
        snprintf(path_s, bufsize, "%s%s", fstate->ssd_path, pathname);
    }

    strcpy(path_c, path_s);
    for (int i = 0; path_c[i] != '\0'; i++) {
        if (path_c[i] == '/') {
            path_c[i] = '+';
        }
    }


    snprintf(path_t, bufsize, "%s%s/%s", fstate->ssd_path, temp_dir, path_c + 1);


    PF("[%s]\t path_t is %s\n", __func__, path_t);


    PF("[%s]\t pathname is %s\n", __func__, pathname);
}


void get_path_c(char *path_c, const char *path_s) {

    strcpy(path_c, path_s);
    for (int i = 0; path_c[i] != '\0'; i++) {
        if (path_c[i] == '/') {
            path_c[i] = '+';
        }
    }
}

bool is_on_cloud(char *pathname) {
    int loc = ON_SSD;
    get_loc(pathname, &loc);
    if (loc == ON_CLOUD) {
        return true;
    } else {
        return false;
    }
}

int cloudfs_getattr_(const char *pathname, struct stat *statbuf) {

    PF("[%s]:\t pathname: %s\n", __func__, pathname);
    INFOF();
    int ret = 0;
    char path_s[MAX_PATH_LEN];
    get_path_s(path_s, pathname, MAX_PATH_LEN);

//    chmod_recover(path_s);
    ret = lstat(path_s, statbuf);

    if (ret < 0) {
        PF("[%s]:\tret < 0\n", __func__);
        return -errno;
    } else {
        if (is_on_cloud(path_s)) {
            if (fstate->no_dedup) {
                get_from_proxy(path_s, statbuf);
            } else {
                get_from_proxy(path_s, statbuf);
                std::string md5;
                std::ifstream proxy(path_s);
                statbuf->st_size = 0;
                size_t seg_size = 0;
                while (proxy >> md5 >> seg_size) {
                    statbuf->st_size += seg_size;
                }

                proxy.close();
            }
        }
    }

    size_t size_f = statbuf->st_size;

    PF("[%s]:\tfile %s have size: %zu\n", __func__, pathname, size_f);
    PF("[%s]:\tfile %s have mode: %zu\n", __func__, pathname, statbuf->st_mode);
    return ret;
}


int cloudfs_getattr(const char *pathname, struct stat *statbuf) {
    int ret = 0;
    PF("[%s]:\tpathname %s\n", __func__, pathname);
//    if (strcmp(pathname, SNAPSHOTPATH) == 0) {
////        int ret;
////        char path_s[MAX_PATH_LEN];
////        get_path_s(path_s, SNAPPROXY, MAX_PATH_LEN);
////        TRY(lstat(path_s));
////        //ensure snapshot readonly
////        statbuf->st_mode = 0100444;
////        return ret;
//        return 0;
//    }
    if (fstate->no_dedup) {
        ret = cloudfs_getattr_(pathname, statbuf);
    } else {
        ret = cloudfs_getattr_(pathname, statbuf);
    }
    return ret;
}


int cloudfs_open(const char *pathname, struct fuse_file_info *fi) {


    PF("[%s]:\t pathname: %s\n", __func__, pathname);
    INFOF();
//    int ret = 0;
    int fd;

    char path_s[MAX_PATH_LEN];
//    char path_c[MAX_PATH_LEN];
    get_path_s(path_s, pathname, MAX_PATH_LEN);
//    chmod_recover(path_s);

//    get_path_c(path_c, path_s);

    if (is_on_cloud(path_s)) {
        if(fstate->no_dedup){
            PF("[%s]:\t downloading from cloud\n", __func__, pathname);
            char path_c[MAX_PATH_LEN];
            char path_t[MAX_PATH_LEN];
            get_path_c(path_c, path_s);
            get_path_t(path_t, pathname, MAX_PATH_LEN);
            cloud_get(path_t, path_c);
            PF("[%s]:\t get statbuf of %s\n", __func__, path_t);
            struct stat statbuf;
            RUN_M(lstat(path_t, &statbuf));

            size_t size_f = statbuf.st_size;
            ino_t ino_f = statbuf.st_ino;

            PF("[%s]:\t opening %s with flag %d, mode is %zu\n", __func__, path_t, fi->flags, statbuf.st_mode);

            fd = open(path_t, O_RDWR);


            fi->fh = fd;

            PF("[%s]:\t open returned %d\t file is %s \t size is %zu \t st_ino is %zu\n", __func__, fd, path_t, size_f,
               ino_f);
            if (fd < 0) {
                return cloudfs_error(__func__);
            }

            return 0;
        }
    } else {
        PF("[%s]:\t opening %s\n", __func__, path_s);

        fd = open(path_s, fi->flags);
        if (fd < 0) {
            return cloudfs_error(__func__);
        }
        fi->fh = fd;
        return 0;
    }
    return 0;
}


int cloudfs_opendir(const char *pathname UNUSED, struct fuse_file_info *fi UNUSED) {

    PF("[%s]:\t pathname: %s\n", __func__, pathname);
    INFOF();
    DIR *d = NULL;
    char path_s[MAX_PATH_LEN];
    get_path_s(path_s, pathname, MAX_PATH_LEN);
    d = opendir(path_s);
    if (!d) {
        return cloudfs_error(__func__);
    }
    fi->fh = (intptr_t) d;
    return 0;
}


int cloudfs_mkdir(const char *pathname, mode_t mode) {

    PF("[%s]:\t pathname: %s\n", __func__, pathname);
    INFOF();
    int ret = 0;

    char path_s[MAX_PATH_LEN];
    get_path_s(path_s, pathname, MAX_PATH_LEN);
    PF("[mkdir] path_s is %s\n", path_s);
    TRY(mkdir(path_s, mode));
//    ret = mkdir(path_s, mode);
//    if (ret < 0) {
//        return cloudfs_error("mkdir failed");
//    }

    return ret;

}

int cloudfs_utimens(const char *pathname, const struct timespec tv[2]) {

    PF("[%s]:\t pathname: %s\n", __func__, pathname);
    INFOF();
    int ret = 0;

    char path_s[MAX_PATH_LEN];
    get_path_s(path_s, pathname, MAX_PATH_LEN);
    PF("[utimens] path_s is %s\n", path_s);

    TRY(utimensat(0, path_s, tv, AT_SYMLINK_NOFOLLOW));

//    ret = utimensat(0, path_s, tv, AT_SYMLINK_NOFOLLOW);
//    if (ret < 0) {
//        return cloudfs_error("utimens failed");
//    }

    return ret;

}

// copied from https://github.com/libfuse/libfuse/blob/master/example/passthrough.c
int cloudfs_readdir(const char *pathname UNUSED, void *buf UNUSED, fuse_fill_dir_t filler UNUSED, off_t offset UNUSED,
                    struct fuse_file_info *fi UNUSED) {

    int ret = 0;
    DIR *dp;
    struct dirent *de;
    char ignore1[MAX_PATH_LEN];
    char ignore2[MAX_PATH_LEN];
    char ignore3[MAX_PATH_LEN];
    char ignore4[MAX_PATH_LEN];
    char ignore5[MAX_PATH_LEN];
    char ignore6[MAX_PATH_LEN];
    char ignore7[MAX_PATH_LEN];
    char ignore8[MAX_PATH_LEN];
    get_path_s(ignore1, "/lost+found", MAX_PATH_LEN);
    get_path_s(ignore2, TEMPDIR, MAX_PATH_LEN);
    get_path_s(ignore3, FILEPROXYDIR, MAX_PATH_LEN);
    get_path_s(ignore4, SEGPROXYDIR, MAX_PATH_LEN);
    get_path_s(ignore5, TEMPSEGDIR, MAX_PATH_LEN);
    get_path_s(ignore6, CACHEDIR, MAX_PATH_LEN);
    get_path_s(ignore7, SNAPSHOT, MAX_PATH_LEN);
    get_path_s(ignore8, MASTERDIR, MAX_PATH_LEN);


    dp = (DIR * )(uintptr_t)
    fi->fh;
    de = readdir(dp);
    if (de == 0) {
        cloudfs_error(__func__);
        //supress enomem
        return 0;
    }
    do {
        char dirpath[MAX_PATH_LEN];
        get_path_s(dirpath, de->d_name, MAX_PATH_LEN);

        if (!strcmp(dirpath, ignore1)) {
            continue;
        }
        if (!strcmp(dirpath, ignore2)) {
            continue;
        }
        if (!strcmp(dirpath, ignore3)) {
            continue;
        }
        if (!strcmp(dirpath, ignore4)) {
            continue;
        }
        if (!strcmp(dirpath, ignore5)) {
            continue;
        }
        if (!strcmp(dirpath, ignore6)) {
            continue;
        }
        if (!strcmp(dirpath, ignore7)) {
            continue;
        }
        if (!strcmp(dirpath, ignore8)) {
            continue;
        }

        if (filler(buf, de->d_name, NULL, 0) != 0) {
            return -ENOMEM;
        }
    } while ((de = readdir(dp)) != NULL);
    return ret;
}

int cloudfs_getxattr(const char *pathname, const char *name, char *value,
                     size_t size) {

    PF("[%s]:\t pathname: %s\n", __func__, pathname);
    INFOF();
    int ret = 0;
    char path_s[MAX_PATH_LEN];
//    if (strcmp(pathname, SNAPSHOTPATH)==0) {
//        get_path_s(path_s, SNAPPROXY, MAX_PATH_LEN);
//    }else{
    get_path_s(path_s, pathname, MAX_PATH_LEN);
//    }
    PF("[%s] path_s : %s\t name : %s \tsize : %zu\n", __func__, path_s, name, size);


    ret = lgetxattr(path_s, name, value, size);
    if (ret < 0) {
        return cloudfs_error("getxattr failed");
    }
    return ret;
}


int cloudfs_setxattr(const char *pathname UNUSED, const char *name UNUSED, const char *value UNUSED,
                     size_t size UNUSED, int flags UNUSED) {
    PF("[%s]:\t pathname: %s\n", __func__, pathname);
    INFOF();
    int ret = 0;
    char path_s[MAX_PATH_LEN];
//    if (strcmp(pathname, SNAPSHOTPATH)==0) {
//        get_path_s(path_s, SNAPPROXY, MAX_PATH_LEN);
//    }else{
    get_path_s(path_s, pathname, MAX_PATH_LEN);
//    }
    TRY(lsetxattr(path_s, name, value, size, flags));
    return ret;
}

//0 for ssd
//1 for cloud
int set_loc(const char *pathname, int value) {
    int location = value;
    int ret;
    PF("[%s]: \tset loc of %s as %d\n", __func__, pathname, value);
    ret = lsetxattr(pathname, "user.location", &location, sizeof(int), 0);
    return ret;
}


int set_dirty(const char *pathname, int value) {
    int is_dirty = value;
    int ret;
    PF("[%s]: \tset isdirty of %s as %d\n", __func__, pathname, value);
    ret = lsetxattr(pathname, "user.isdirty", &is_dirty, sizeof(int), 0);
    return ret;
}

int get_loc(const char *pathname, int *value) {
    int ret;
    ret = lgetxattr(pathname, "user.location", value, sizeof(int));
    PF("[%s]: \t%s loc is %d\n", __func__, pathname, *value);
    return ret;
}

int get_dirty(const char *pathname, int *value) {
    int ret;
    ret = lgetxattr(pathname, "user.isdirty", value, sizeof(int));
    PF("[%s]: \t%s isdirty is %d\n", __func__, pathname, *value);
    return ret;
}

int cloudfs_mknod(const char *pathname, mode_t mode UNUSED, dev_t dev UNUSED) {

    PF("[%s]:\t pathname: %s\n", __func__, pathname);

    INFOF();
    int ret = 0;
    char path_s[MAX_PATH_LEN];
    get_path_s(path_s, pathname, MAX_PATH_LEN);
    TRY(mknod(path_s, mode, dev));
    //set as not dirty and on ssd
    RUN_M(set_loc(path_s, ON_SSD));
    RUN_M(set_dirty(path_s, N_DIRTY));
//    int set_l_ret = set_loc(path_s, ON_SSD);
//    if(set_l_ret < 0){
//        return -errno;
//    }
    return ret;
}


int cloudfs_read_node(const char *pathname, char *buf, size_t size, off_t offset, struct fuse_file_info *fi) {
    PF("[%s]:\t pathname: %s\n", __func__, pathname);
    INFOF();
    int ret = 0;
    char path_s[MAX_PATH_LEN];
    get_path_s(path_s, pathname, MAX_PATH_LEN);

    PF("[%s]:\t pread(fd: %d)\t file is %s\n", __func__, fi->fh, path_s);
    TRY(pread(fi->fh, buf, size, offset));
    return ret;
}


int cloudfs_read_de(const char *pathname, char *buf, size_t size, off_t offset, struct fuse_file_info *fi) {
    NOI();

//    int loc = ON_SSD;
//    char path_s[MAX_PATH_LEN];
//    get_path_s(path_s, pathname, MAX_PATH_LEN);
//
//    PF("[%s]:\t getting loc\n", __func__);
//    RUN_M(get_loc(path_s, &loc));
//    if (loc == ON_SSD) {
//        PF("[%s]:\t return cloudfs_read_node(pathname, buf, size, offset, fi);\n", __func__);
//        return cloudfs_read_node(pathname, buf, size, offset, fi);
//    } else {
//
//        PF("[%s]:\t loc == ON_CLOUD\n", __func__);
//        seg_info_p segs[MAX_SEG_AMOUNT];
////        int num_seg = mydedup_readsegs_from_proxy(path_s, segs);
//        long seg_offset = 0;
//        for (int i = 0; i < num_seg; i++) {
//            if (i > MAX_SEG_AMOUNT) {
//                PF("[%s] ERROR i %d > MAX_SEG_AMOUNT\n", __func__, i);
//            }
////            PF("i = %d\n", i);
////            PF("[%s]: read seg[%d]: md5:%s, size:%ld, offset:%ld\n", __func__, i, segs[i]->md5, segs[i]->seg_size,
////               seg_offset);
//            seg_offset += segs[i]->seg_size;
//        }
//
//        ret_range_t range;
//        ret_range_p rangep = &range;
//        long offset_change;
//        offset_change = mydedup_determine_range(rangep, size, offset, segs, num_seg);
//
//        char temp_file_path[MAX_PATH_LEN];
//        get_tempfile_path_dedup(temp_file_path, path_s, MAX_PATH_LEN);
//        mydedup_down_segs(temp_file_path, segs, rangep->start, rangep->end);
//
////        SEEFILE(path_s);
//        int fd = open(temp_file_path, O_RDWR);
//
//        struct stat statbuf;
//        lstat(temp_file_path, &statbuf);
//
//
//
//        long newoffset = offset - offset_change;
//
//        PF("[%s]: line: %d, size is %zu\n", __func__, __LINE__, statbuf.st_size);
//
//        long newsize = size;
//
//        PF("[%s]  %d = open(%s)\n", __func__, fd, temp_file_path);
//        int ret = 0;
//        TRY(pread(fd, buf, newsize, newoffset));
//        PF("[%s]  pread(%d, buf, size = %zu, offset = %zu) returned %d\n", __func__, fd, newsize, offset - offset_change, ret);
//
//
//        close(fd);
//        remove(temp_file_path);
//        return ret;
//    }
    return 0;
}





//int cloudfs_read_de2(const char *pathname, char *buf, size_t size, off_t offset, struct fuse_file_info *fi) {
//    NOI();
//
//    int loc = ON_SSD;
//    char path_s[MAX_PATH_LEN];
//    get_path_s(path_s, pathname, MAX_PATH_LEN);
//
//    PF("[%s]:\t getting loc\n", __func__);
//    RUN_M(get_loc(path_s, &loc));
//    if (loc == ON_SSD) {
//        PF("[%s]:\t return cloudfs_read_node(pathname, buf, size, offset, fi);\n", __func__);
//        return cloudfs_read_node(pathname, buf, size, offset, fi);
//    } else {
//
//        PF("[%s]:\t loc == ON_CLOUD\n", __func__);
//        seg_info_p segs[MAX_SEG_AMOUNT];
//        int num_seg = mydedup_readsegs_from_proxy(path_s, segs);
//        long seg_offset = 0;
//        for (int i = 0; i < num_seg; i++) {
//            if (i > MAX_SEG_AMOUNT) {
//                PF("[%s] ERROR i %d > MAX_SEG_AMOUNT\n", __func__, i);
//            }
//            seg_offset += segs[i]->seg_size;
//        }
//
//        ret_range_t range;
//        ret_range_p rangep = &range;
//        long offset_change;
//        offset_change = mydedup_determine_range(rangep, size, offset, segs, num_seg);
//        mydedup_read_seg();
//
//
//
////
////        char temp_file_path[MAX_PATH_LEN];
////        get_tempfile_path_dedup(temp_file_path, path_s, MAX_PATH_LEN);
////        mydedup_down_segs(temp_file_path, segs, rangep->start, rangep->end);
////
//////        SEEFILE(path_s);
////        int fd = open(temp_file_path, O_RDWR);
////
////        struct stat statbuf;
////        lstat(temp_file_path, &statbuf);
////
////
////
////        long newoffset = offset - offset_change;
////
////        PF("[%s]: line: %d, size is %zu\n", __func__, __LINE__, statbuf.st_size);
////
////        long newsize = size;
////
////        PF("[%s]  %d = open(%s)\n", __func__, fd, temp_file_path);
////        int ret = 0;
//////        TRY(pread(fd, buf, newsize, newoffset));
////        PF("[%s]  pread(%d, buf, size = %zu, offset = %zu) returned %d\n", __func__, fd, newsize, offset - offset_change, ret);
////
////
////        close(fd);
////        remove(temp_file_path);
//    }
//    return 0;
//}



int cloudfs_read(const char *pathname UNUSED, char *buf UNUSED, size_t size UNUSED, off_t offset UNUSED,
                 struct fuse_file_info *fi) {
//    if (strcmp(pathname, SNAPSHOTPATH) == 0) {
//        return 0;
//    }
    size_t ret = 0;
    if (fstate->no_dedup) {
        ret = cloudfs_read_node(pathname, buf, size, offset, fi);
        PF("[OUTPUT] cloudfs_read, %s, buf, %zu, %zu return %d\n", pathname, size, offset, ret);
    } else {
        ret = mydedup_read(pathname, buf, size, offset, fi);

        PF("[OUTPUT] buf is %s\n", buf);

    }

    return ret;
}


int cloudfs_write_node(const char *pathname UNUSED, const char *buf UNUSED, size_t size UNUSED, off_t offset UNUSED,
                       struct fuse_file_info *fi) {
    PF("[%s]:\t pathname: %s\n", __func__, pathname);

    INFOF();
    int ret = 0;
    char path_s[MAX_PATH_LEN];
    get_path_s(path_s, pathname, MAX_PATH_LEN);

    TRY(pwrite(fi->fh, buf, size, offset));

    PF("[%s]:\t pathname: %s\n", __func__, pathname);
    int loc = ON_SSD;
    RUN_M(get_loc(path_s, &loc));
    if (loc == ON_CLOUD) {
        RUN_M(set_dirty(path_s, DIRTY));
    }

    return ret;
}

//
//int cloudfs_write_de(const char *pathname UNUSED, const char *buf UNUSED, size_t size UNUSED, off_t offset UNUSED,
//                     struct fuse_file_info *fi) {
//    PF("[%s]:\t pathname: %s\t offset: %zu\n", __func__, pathname, offset);
//    int ret = 0;
//
////    char path_s[MAX_PATH_LEN];
////    get_path_s(path_s, pathname, MAX_PATH_LEN);
////    int loc = ON_SSD;
////    RUN_M(get_loc(path_s, &loc));
////    if (loc == ON_CLOUD) {
////
//////        NOI();
//////        FILE *fp_fileproxy = FFOPEN__(path_s, "r");
////        seg_info_p segs[MAX_SEG_AMOUNT];
////        int num_seg = 0;
////        num_seg = mydedup_readsegs_from_proxy(path_s, segs);
////
////        PF("[%s] returned from mydedup_readsegs_from_proxy, read %d segs\n", __func__, num_seg);
////        long seg_offset = 0;
////        for (int i = 0; i < num_seg; i++) {
////            if (i > MAX_SEG_AMOUNT) {
////                PF("[%s] ERROR i %d > MAX_SEG_AMOUNT\n", __func__, i);
////            }
//////            PF("i = %d\n", i);
//////            PF("[%s]: read seg[%d]: md5:%s, size:%ld, offset:%ld\n", __func__, i, segs[i]->md5, segs[i]->seg_size,
//////               seg_offset);
////            seg_offset += segs[i]->seg_size;
////        }
////
////        long file_size = seg_offset;
////        long write_start = offset;
////        long write_end = size + offset;
////        if (size == 0) {
////            PF("[%s]:WHY WRITE 0 BYTE?\n", __func__);
////            return 0;
////        }
////
////        PF("[%s]:offset is %ld, file_size is %ld\n", __func__, offset, file_size);
////
////        int start = 0;
////        int end = 0;
////
////
////        if (offset >= file_size) {
////            PF("[%s]:offset >= file_size\n", __func__);
////
////            start = num_seg;//means write append to the end!
////
////            char temp_file_path[MAX_PATH_LEN];
////
////            get_tempfile_path_dedup(temp_file_path, path_s, MAX_PATH_LEN);
////
////
////            PF("[%s]:temp_file_path is %s\n", __func__, temp_file_path);
//////            FILE *fp2 = FFOPEN__(temp_file_path, "w");
//////
//////            FFCLOSE__(fp2);
////
////            seg_down(temp_file_path, segs[num_seg - 1]->md5);
////
////            mydedup_remove_seg(segs[num_seg - 1]->md5);
////
////
//////
//////            PF("[%s]:%d\n",__func__,__LINE__);
////
////            int fd = open(temp_file_path, O_RDWR);
////
////            PF("[%s]:pwrite at %s\n", __func__, temp_file_path);
////            struct stat statbuf;
////            lstat(path_s, &statbuf);
////            size_t size_1 = statbuf.st_size;
//////            SEEFILE(temp_file_path);
////            TRY(pwrite(fd, buf, size, segs[num_seg - 1]->seg_size));
////            lstat(path_s, &statbuf);
////            size_t size_2 = statbuf.st_size;
////
////            PF("[%s]: size before is %zu\t size after is %zu\n", __func__, size_1, size_2);
////
////            close(fd);
////
//////            SEEFILE(temp_file_path);
////            mydedup_uploadfile_append(temp_file_path, path_s, segs, num_seg);
////
////        } else {
////
////            PF("[%s]:offset < file_size\n", __func__);
////            NOI();
////            seg_offset = 0;
////            for (int i = 0; i < num_seg; i++) {
////                if (offset >= seg_offset && offset < seg_offset + segs[i]->seg_size) {
////                    start = i;
////                }
////
////            }
////        }
////
////
////        PF("[%s]: writing %ld to %ld\n", __func__, offset, write_end);
////
//////        int first_seg =
////
////
////
////
////        RUN_M(set_dirty(path_s, DIRTY));
////
////
////        for (int i = 0; i < num_seg; i++) {
//////            PF("i = %d\n", i);
//////            PF("[%s]: read seg[%d]: md5:%s, size:%ld, offset:%ld\n", __func__, i, segs[i]->md5, segs[i]->seg_size,
//////               seg_offset);
////            free(segs[i]);
////        }
////        return size;
////    } else {
////        PF("[%s]:\t pathname: %s\t is on ssd\n", __func__, pathname);
////        if (offset + size > fstate->threshold) {
////
//////            NOI();
////            PF("[%s]:\t offset: %zu > fstate->threshold %d\n", __func__, offset, fstate->threshold);
////            TRY(pwrite(fi->fh, buf, size, offset));
////
////            PF("[%s]:\t pathname: %s\n", __func__, pathname);
////
////
////            close(fi->fh);//finish this write first
////            set_loc(path_s, ON_CLOUD);
////            // in dedup mode ON_CLOUD means FIFH IS CLOSED!
////
////            //
////
////
////
//////            seg_info_p segs[MAX_SEG_AMOUNT];
//////            int num_seg = 0;
//////            mydedup_segmentation(path_s, &num_seg, segs);
////            mydedup_uploadfile(path_s);
////
////            PF("[%s]:\t RETURNED %d, size = %zu\n", __func__, ret, size);
////            return ret;
////
////        } else {
////            PF("[%s]:\t offset: %zu <= fstate->threshold %d\n", __func__, offset, fstate->threshold);
////
////            TRY(pwrite(fi->fh, buf, size, offset));
////
////            PF("[%s]:\t pathname: %s\n", __func__, pathname);
////
////
////            PF("[%s]:\t RETURNED %d, size = %zu\n", __func__, ret, size);
////            return ret;
////        }
////    }
////
////    return ret;
//}


int cloudfs_write(const char *pathname UNUSED, const char *buf UNUSED, size_t size UNUSED, off_t offset UNUSED,
                  struct fuse_file_info *fi) {
    char path_s[MAX_PATH_LEN];
    get_path_s(path_s, pathname, MAX_PATH_LEN);
//    chmod_recover(path_s);

    struct stat statbuf;
    lstat(path_s, &statbuf);


    PF("[%s]:\t mode is %zu\n", __func__, statbuf.st_mode);
    int ret2 = access(path_s, W_OK);
    PF("[%s]:\t access ret is %d\n", __func__, ret2);

    if(ret2!=0){
        return -errno;
    }
    int ret = 0;
    if (fstate->no_dedup) {
        ret = cloudfs_write_node(pathname, buf, size, offset, fi);
    } else {
        ret = mydedup_write(pathname, buf, size, offset, fi);
    }

    return ret;
}


int cloudfs_release2(const char *pathname UNUSED, struct fuse_file_info *fi UNUSED) {

//    PF("[%s]: pathname: %s\n", __func__, pathname);
//    INFO();
//
//    char path_s[MAX_PATH_LEN];
//    char path_c[MAX_PATH_LEN];
//    get_path_s(path_s, pathname, MAX_PATH_LEN);
//    get_path_c(path_c, path_s);
//    size_t size_f = 0;
//
//    struct stat statbuf;
//    RUN_M(lstat(path_s, &statbuf));
//    size_f = statbuf.st_size;
//    if (size_f > fstate->threshold) {
//        PF("[%s]: placing %s into cloud at %s\n", __func__, pathname, path_c);
//
//    }
//    RUN_M(close(fi->fh));

    int ret = 0;
    ret = close(fi->fh);
    return ret;
}

void cloud_put(const char *path_s, const char *path_c, long size) {

    infile = FFOPEN__(path_s, "rb");
    cloud_put_object(BUCKET, path_c, size, put_buffer);
    cloud_print_error();

    PF("[%s]:\t put %s on cloud with key:[%s], size : %zu\n", __func__, path_s, path_c, size);
    FFCLOSE__(infile);

//    FILE *fp;
//    char ch;
//    fp=FFOPEN__(path_s,"r");
//    int i;
//    PF("showing file content %s: ", path_s);
//    while((ch=fgetc(fp))!=EOF && i < 25)
//    {
//        PF("%c", ch);
//    }
//
//    PF("\n");
//    FFCLOSE__(fp);

//    outfile = FFOPEN__("/tmp/peek", "wb");
//    cloud_get_object(BUCKET, path_c, get_buffer);
//    cloud_print_error();
//    FFCLOSE__(outfile);
//
//    fp=FFOPEN__("/tmp/peek","r");
//    PF("PEEKING file content %s @ cloud %s: ", path_s, path_c);
//    while((ch=fgetc(fp))!=EOF && i < 25)
//    {
//        PF("%c", ch);
//    }
//
//    PF("\n");
//    FFCLOSE__(fp);


}


void cloud_put_old(char *path_s, char *path_c, struct stat *statbuf_p) {

    infile = FFOPEN__(path_s, "rb");
    cloud_put_object(BUCKET, path_c, statbuf_p->st_size, put_buffer);
    cloud_print_error();

    PF("[%s]:\t put %s on cloud with key:[%s], size : %zu\n", __func__, path_s, path_c, statbuf_p->st_size);
    FFCLOSE__(infile);

//    FILE *fp;
//    char ch;
//    fp=FFOPEN__(path_s,"r");
//    int i;
//    PF("showing file content %s: ", path_s);
//    while((ch=fgetc(fp))!=EOF && i < 25)
//    {
//        PF("%c", ch);
//    }
//
//    PF("\n");
//    FFCLOSE__(fp);

//    outfile = FFOPEN__("/tmp/peek", "wb");
//    cloud_get_object(BUCKET, path_c, get_buffer);
//    cloud_print_error();
//    FFCLOSE__(outfile);
//
//    fp=FFOPEN__("/tmp/peek","r");
//    PF("PEEKING file content %s @ cloud %s: ", path_s, path_c);
//    while((ch=fgetc(fp))!=EOF && i < 25)
//    {
//        PF("%c", ch);
//    }
//
//    PF("\n");
//    FFCLOSE__(fp);


}

void cloud_get(const char *path_s, const char *path_c) {

    PF("[%s]:\t path_s = [%s]\n", __func__, path_s);
    outfile = FFOPEN__(path_s, "wb");
    cloud_get_object(BUCKET, path_c, get_buffer);
    cloud_print_error();
    PF("[%s]:\t get %s(FD:%d) from cloud with key:[%s]\n", __func__, path_s, outfile, path_c);
    PF("[%s]:\t return\n", __func__);
    FFCLOSE__(outfile);


//    FILE *fp;
//    char ch;
//    fp = FFOPEN__(path_s, "r");
//    int i = 0;
//    PF("[%s]: showing file content %s: ", __func__, path_s);
//    while ((ch = fgetc(fp)) != EOF && i < 25) {
//        PF("%c", ch);
//        i++;
//    }
//
//    PF("\n");
//    FFCLOSE__(fp);
}


int clone_2_proxy(char *path_s, struct stat *statbuf_p) {
    PF("[%s]: save attr to %s\n", __func__, path_s);
    int ret = 0;
//    struct stat {
//        dev_t     st_dev;         /* ID of device containing file */
//        ino_t     st_ino;         /* Inode number */
//        mode_t    st_mode;        /* File type and mode */
//        nlink_t   st_nlink;       /* Number of hard links */
//        uid_t     st_uid;         /* User ID of owner */
//        gid_t     st_gid;         /* Group ID of owner */
//        dev_t     st_rdev;        /* Device ID (if special file) */
//        off_t     st_size;        /* Total size, in bytes */
//        blksize_t st_blksize;     /* Block size for filesystem I/O */
//        blkcnt_t  st_blocks;      /* Number of 512B blocks allocated */
    TRY(lsetxattr(path_s, "user.st_dev", &statbuf_p->st_dev, sizeof(dev_t), 0));
    TRY(lsetxattr(path_s, "user.st_ino", &statbuf_p->st_ino, sizeof(ino_t), 0));
//    TRY(lsetxattr(path_s, "user.st_mode", &statbuf_p->st_mode, sizeof(mode_t), 0));
    TRY(lsetxattr(path_s, "user.st_nlink", &statbuf_p->st_nlink, sizeof(nlink_t), 0));
    TRY(lsetxattr(path_s, "user.st_uid", &statbuf_p->st_uid, sizeof(uid_t), 0));
    TRY(lsetxattr(path_s, "user.st_gid", &statbuf_p->st_gid, sizeof(gid_t), 0));
    TRY(lsetxattr(path_s, "user.st_rdev", &statbuf_p->st_rdev, sizeof(dev_t), 0));
    TRY(lsetxattr(path_s, "user.st_size", &statbuf_p->st_size, sizeof(off_t), 0));

    PF("[%s]: save size = %zu to %s\n", __func__, statbuf_p->st_size, path_s);
    TRY(lsetxattr(path_s, "user.st_blksize", &statbuf_p->st_blksize, sizeof(blksize_t), 0));
    TRY(lsetxattr(path_s, "user.st_blocks", &statbuf_p->st_blocks, sizeof(blkcnt_t), 0));
    return ret;
}

int get_from_proxy(const char *path_s, struct stat *statbuf_p) {
    int ret = 0;
//    struct stat {
//        dev_t     st_dev;         /* ID of device containing file */
//        ino_t     st_ino;         /* Inode number */
//        mode_t    st_mode;        /* File type and mode */
//        nlink_t   st_nlink;       /* Number of hard links */
//        uid_t     st_uid;         /* User ID of owner */
//        gid_t     st_gid;         /* Group ID of owner */
//        dev_t     st_rdev;        /* Device ID (if special file) */
//        off_t     st_size;        /* Total size, in bytes */
//        blksize_t st_blksize;     /* Block size for filesystem I/O */
//        blkcnt_t  st_blocks;      /* Number of 512B blocks allocated */
    lstat(path_s, statbuf_p);
    TRY(lgetxattr(path_s, "user.st_dev", &statbuf_p->st_dev, sizeof(dev_t)));
    TRY(lgetxattr(path_s, "user.st_ino", &statbuf_p->st_ino, sizeof(ino_t)));
//    TRY(lgetxattr(path_s, "user.st_mode", &statbuf_p->st_mode, sizeof(mode_t)));
    TRY(lgetxattr(path_s, "user.st_nlink", &statbuf_p->st_nlink, sizeof(nlink_t)));
    TRY(lgetxattr(path_s, "user.st_uid", &statbuf_p->st_uid, sizeof(uid_t)));
    TRY(lgetxattr(path_s, "user.st_gid", &statbuf_p->st_gid, sizeof(gid_t)));
    TRY(lgetxattr(path_s, "user.st_rdev", &statbuf_p->st_rdev, sizeof(dev_t)));
    TRY(lgetxattr(path_s, "user.st_size", &statbuf_p->st_size, sizeof(off_t)));


    PF("[%s]: get size = %zu from %s\n", __func__, statbuf_p->st_size, path_s);
//    fprintf(logfile, "[%s]: setting %s has mode :%zu\n", __func__, path_s, statbuf_p->st_mode);
    TRY(lgetxattr(path_s, "user.st_blksize", &statbuf_p->st_blksize, sizeof(blksize_t)));
    TRY(lgetxattr(path_s, "user.st_blocks", &statbuf_p->st_blocks, sizeof(blkcnt_t)));
    return ret;
}

int cloudfs_release_node(const char *pathname UNUSED, struct fuse_file_info *fi UNUSED) {

    PF("[%s]:\t pathname: %s\n", __func__, pathname);
    INFOF();

    char path_s[MAX_PATH_LEN];
    char path_c[MAX_PATH_LEN];
    char path_t[MAX_PATH_LEN];
    get_path_s(path_s, pathname, MAX_PATH_LEN);
    get_path_c(path_c, path_s);
    get_path_t(path_t, pathname, MAX_PATH_LEN);

    PF("[%s]:\t pathname = %s \t path_s = %s \t path_c = %s \t path_t = %s\n", __func__, pathname, path_s, path_c,
       path_t);
//    size_t size_f = 0;


    if (close(fi->fh) < 0) {
        return cloudfs_error("release failed");
    }

    int loc = ON_SSD;
    PF("[%s]:\t getting loc\n", __func__);
    RUN_M(get_loc(path_s, &loc));
    if (loc == ON_SSD) {
        PF("[%s]:\t file %s is ON_SSD\n", __func__, pathname);
        int dirty = N_DIRTY;
        RUN_M(get_dirty(path_s, &dirty));
        if (dirty) {//file is dirty

            PF("[%s]:\t loc on ssd and dirty??  This should never happen!!!!!!\n", __func__);

            PF("[%s]:\t file %s is dirty\n", __func__, pathname);

        } else {//not dirty

            PF("[%s]:\t file %s not dirty\n", __func__, pathname);
            struct stat statbuf;
            RUN_M(lstat(path_s, &statbuf));

            size_t size_f = statbuf.st_size;
            PF("[%s]:\t file %s size: %zu\n", __func__, pathname, size_f);
            if (size_f > fstate->threshold) {//larger than threshold, need to move to cloud
                //
                PF("[%s]:\t clean file %s need to be put on cloud, cloud path is %s\n", __func__, pathname, path_c);

                PF("[line: %d]:\t", __LINE__);

                cloud_put(path_s, path_c, statbuf.st_size);
                PF("[line: %d]:\t", __LINE__);
                FILE *fd = FFOPEN__(path_s, "w");
                FFCLOSE__(fd);
                //clear all content;
                set_loc(path_s, ON_CLOUD);
                set_dirty(path_s, N_DIRTY);

                RUN_M(clone_2_proxy(path_s, &statbuf));

            } else {//remain on ssd
                PF("[%s]:\t clean file %s put on ssd\n", __func__, pathname);
                PF("[line: %d]:\t", __LINE__);
                set_loc(path_s, ON_SSD);
                set_dirty(path_s, N_DIRTY);
            }

        }

    } else if (loc == ON_CLOUD) {

        PF("[%s]:\t file %s is ON_CLOUD\n", __func__, pathname);
        int dirty = N_DIRTY;

        RUN_M(get_dirty(path_s, &dirty));
        if (dirty) {//file is dirty
            PF("[%s]:\t file %s is dirty\n", __func__, pathname);

            struct stat statbuf;
            RUN_M(lstat(path_t, &statbuf));

            size_t size_f = statbuf.st_size;
            PF("[%s]: temp file size is %zu\n", __func__, size_f);
            if (size_f <= fstate->threshold) {//smaller than threshold, need to move back to ssd
//                NOI();
                cloud_delete_object(BUCKET, path_c);

                PF("[%s]: move file from cloud to ssd at %s. key:[%s]", __func__, path_s, path_c);
                PF("[%s]: rename %s to %s. key:[%s]", __func__, path_t, path_s, path_c);

                RUN_M(rename(path_t, path_s));
                set_loc(path_s, ON_SSD);
                set_dirty(path_s, N_DIRTY);

            } else {//remain on cloud
//                NOI();
                cloud_delete_object(BUCKET, path_c);
                cloud_put(path_t, path_c, statbuf.st_size);

                PF("[%s]: uploading %s to cloud to replace old file. key:[%s]", __func__, path_t, path_c);

                set_loc(path_s, ON_CLOUD);
                set_dirty(path_s, N_DIRTY);
                RUN_M(clone_2_proxy(path_s, &statbuf));
                // TODO
                // TODO
                // TODO
                // TODO
                // TODO
            }
        } else {//not dirty

            PF("[%s]:\t clean file %s stay on cloud\n", __func__, pathname);
//            NOI();


//            struct stat statbuf;
//            RUN_M(lstat(path_s, &statbuf));

            RUN_M(remove(path_t));
            set_loc(path_s, ON_CLOUD);
            set_dirty(path_s, N_DIRTY);
        }
    } else {
        return cloudfs_error("release failed");
    }

}


int cloudfs_release_de(const char *pathname UNUSED, struct fuse_file_info *fi UNUSED) {
    PF("[%s]:\t pathname: %s\n", __func__, pathname);
    INFOF();

}

int cloudfs_release(const char *pathname UNUSED, struct fuse_file_info *fi UNUSED) {
    int ret = 0;
    if (fstate->no_dedup) {
        ret = cloudfs_release_node(pathname, fi);
    } else {
        ret = cloudfs_release_de(pathname, fi);
    }

    return ret;
}


int cloudfs_access(const char *pathname UNUSED, int mask UNUSED) {

    PF("[%s]:\t pathname: %s, %d\n", __func__, pathname, mask);
//    INFOF();
    int ret = 0;
    char path_s[MAX_PATH_LEN];
    get_path_s(path_s, pathname, MAX_PATH_LEN);


    TRY(access(path_s, mask));


    PF("[%s]:\t ret is %d\n", __func__, ret);
//    ret = access(path_s, mask);
//    if (ret < 0) {
//        return cloudfs_error("access failed");
//    }

    return ret;
}

int cloudfs_chmod(const char *pathname UNUSED, mode_t mode UNUSED) {
//    NOI();

//    PF("[%s]:\t pathname: %s\n", __func__, pathname);
//    fprintf(logfile,"[%s]: pathname: %s\n", __func__, pathname);
//    INFOF();
//    int ret = 0;
//
//    char path_s[MAX_PATH_LEN];
//    get_path_s(path_s, pathname, MAX_PATH_LEN);
//    if(is_on_cloud(path_s)){
//        fprintf(logfile,"[%s]: path_s: %s is on cloud\n", __func__, path_s);
//    }
//    fprintf(logfile,"[%s]: path_s: %s is not on cloud\n", __func__, path_s);
//    PF("[utimens] path_s is %s\n", path_s);
//
//    TRY(chmod(path_s, mode));




    PF("[%s]:\t pathname: %s\n", __func__, pathname);

    fprintf(logfile, "[%s]: pathname: %s\n", __func__, pathname);
    INFOF();
    int ret = 0;

    char path_s[MAX_PATH_LEN];
    get_path_s(path_s, pathname, MAX_PATH_LEN);

    if (is_on_cloud(path_s)) {
        fprintf(logfile, "[%s]: path_s: %s is on cloud\n", __func__, path_s);
    }else{
        fprintf(logfile, "[%s]: path_s: %s is not on cloud\n", __func__, path_s);
    }

    fprintf(logfile, "[%s]: setting %s mode as %zu\n", __func__, path_s, mode);
//    PF("[utimens] path_s is %s\n", path_s);
//    TRY(chmod(path_s, mode | 0666));
//    TRY(lsetxattr(path_s, "user.chmod_mode", &mode, sizeof(mode_t), 0));
    TRY(chmod(path_s, mode));


//    ret = utimensat(0, path_s, tv, AT_SYMLINK_NOFOLLOW);
//    if (ret < 0) {
//        return cloudfs_error("utimens failed");
//    }

    return ret;
}

void chmod_recover(const char *path_s) {
//    PF("[%s] recover chmod for %s\n", __func__, path_s);
//    PF("[%s] check if %s exist: %d\n", __func__, path_s, file_exist(path_s));
//
//    if(file_exist(path_s)){
//        mode_t mode;
//        int ret = lgetxattr(path_s, "user.chmod_mode", &mode, sizeof(mode_t));
//        if(ret !=0){
//            PF("[%s]:\t no mode saved\n", __func__);
//            return;
//        }
//        PF("[%s]:\t mode_t is: %zu\n", __func__, mode);
//        chmod(path_s, mode);
//    }
}

int cloudfs_rmdir(const char *pathname UNUSED) {
    //    NOI();

    PF("[%s]:\t pathname: %s\n", __func__, pathname);
    INFOF();
    int ret = 0;

    char path_s[MAX_PATH_LEN];
    get_path_s(path_s, pathname, MAX_PATH_LEN);
    PF("[utimens] path_s is %s\n", path_s);

    ret = rmdir(path_s);
    if (ret == -1) {
        return -errno;

    }

//    ret = utimensat(0, path_s, tv, AT_SYMLINK_NOFOLLOW);
//    if (ret < 0) {
//        return cloudfs_error("utimens failed");
//    }

    return 0;
}

int cloudfs_unlink(const char *pathname UNUSED) {
    int ret = 0;
    if (fstate->no_dedup) {
        ret = cloudfs_unlink_node(pathname);
    } else {
        ret = mydedup_unlink(pathname);
    }
    return ret;
}

int cloudfs_unlink_node(const char *pathname UNUSED) {
    PF("[%s]:\t pathname: %s\n", __func__, pathname);
    INFOF();
    int ret = 0;

    char path_s[MAX_PATH_LEN];
    get_path_s(path_s, pathname, MAX_PATH_LEN);
    PF("[utimens] path_s is %s\n", path_s);
    if (is_on_cloud(path_s)) {

        char path_c[MAX_PATH_LEN];
        get_path_c(path_c, path_s);
        cloud_delete_object(BUCKET, path_c);
        cloud_print_error();
    }
    TRY(unlink(path_s));

//    ret = utimensat(0, path_s, tv, AT_SYMLINK_NOFOLLOW);
//    if (ret < 0) {
//        return cloudfs_error("utimens failed");
//    }

    return ret;
}

int cloudfs_truncate_node(const char *pathname UNUSED, off_t newsize UNUSED) {

    PF("[%s]:\t pathname: %s\n", __func__, pathname);
    INFOF();
    int ret = 0;

    char path_s[MAX_PATH_LEN];
    get_path_s(path_s, pathname, MAX_PATH_LEN);
    if (is_on_cloud(path_s)) {
        char path_t[MAX_PATH_LEN];
        get_path_t(path_t, pathname, MAX_PATH_LEN);
        PF("[utimens] path_s is on cloud, truncating %s\n", path_t);
        RUN_M(set_dirty(path_s, DIRTY));//only when on cloud
        TRY(truncate(path_t, newsize));
    } else {

        PF("[utimens] path_s is on ssd, truncating %s\n", path_s);
        TRY(truncate(path_s, newsize));

    }

//    PF("[utimens] path_s is %s\n", path_s);
//
//    int loc = ON_SSD;
//    RUN_M(get_loc(path_s, &loc));
//    if (loc == ON_CLOUD) {
//        RUN_M(set_dirty(path_s, DIRTY));
//    }
//
//    TRY(truncate(path_s, newsize));

//    ret = utimensat(0, path_s, tv, AT_SYMLINK_NOFOLLOW);
//    if (ret < 0) {
//        return cloudfs_error("utimens failed");
//    }

    return ret;
}

int cloudfs_truncate(const char *pathname UNUSED, off_t newsize UNUSED) {
    int ret = 0;
    if (fstate->no_dedup) {
        ret = cloudfs_truncate_node(pathname, newsize);
    } else {
        ret = mydedup_truncate(pathname, newsize);
    }
    return ret;
}

int cloudfs_link(const char *pathname UNUSED, const char *newpath UNUSED) {
    PF("[%s]:\t pathname: %s\n", __func__, pathname);
    NOI();
    int ret = 0;

    fprintf(logfile, "[%s]:\tpathname:%s\tnewpath:%s\n", __func__, pathname, newpath);

    char path_s[MAX_PATH_LEN];
    char path_s_n[MAX_PATH_LEN];
    get_path_s(path_s, pathname, MAX_PATH_LEN);
    get_path_s(path_s_n, newpath, MAX_PATH_LEN);
    if (is_on_cloud(path_s)) {
        fprintf(logfile, "[%s]:\tpath_s:%s is on cloud\n", __func__, path_s);
    } else {
        fprintf(logfile, "[%s]:\tpath_s:%s is not on cloud\n", __func__, path_s);

    }
    if (is_on_cloud(path_s_n)) {
        fprintf(logfile, "[%s]:\tpath_s_n:%s is on cloud\n", __func__, path_s_n);
    } else {
        fprintf(logfile, "[%s]:\tpath_s_n:%s is not on cloud\n", __func__, path_s_n);

    }
    TRY(link(path_s, path_s_n));
    return ret;



//    return 0;

//    int ret = 0;
//
//    char path_s[MAX_PATH_LEN];
//    char path_s_n[MAX_PATH_LEN];
//    get_path_s(path_s, pathname, MAX_PATH_LEN);
//    get_path_s(path_s_n, newpath, MAX_PATH_LEN);
////    if (is_on_cloud(path_s)) {
////        char path_t[MAX_PATH_LEN];
////        get_path_t(path_t, pathname, MAX_PATH_LEN);
////        PF("[utimens] path_s is on cloud, truncating %s\n", path_t);
////        RUN_M(set_dirty(path_s, DIRTY));//only when on cloud
////        TRY(truncate(path_t, newsize));
////    }else{
////
////        PF("[utimens] path_s is on ssd, truncating %s\n", path_s);
////        TRY(truncate(path_s, newsize));
////
////    }
//
//    TRY(link(pathname, newpath));
//    return ret;


}

int cloudfs_symlink(const char *pathname UNUSED, const char *newpath UNUSED) {
    NOI();
    int ret = 0;

    fprintf(logfile, "[%s]:\tpathname:%s\tnewpath:%s\n", __func__, pathname, newpath);

    char path_s_n[MAX_PATH_LEN];
    get_path_s(path_s_n, newpath, MAX_PATH_LEN);
    if (is_on_cloud(path_s_n)) {
        fprintf(logfile, "[%s]:\tpath_s_n:%s is on cloud\n", __func__, path_s_n);
    } else {
        fprintf(logfile, "[%s]:\tpath_s_n:%s is not on cloud\n", __func__, path_s_n);

    }
    fprintf(logfile, "[%s]:\tsymlink(pathname:%s,\tpath_s_n:%s)\n", __func__, pathname, path_s_n);
    ret = symlink(pathname, path_s_n);
    if (ret == -1) {
        return -errno;
    }

    return 0;

//    return 0;

//    PF("[%s]:\t pathname: %s\n", __func__, pathname);
//    INFOF();
//    int ret = 0;
//    char path_s[MAX_PATH_LEN];
//    char path_s_n[MAX_PATH_LEN];
//    get_path_s(path_s, pathname, MAX_PATH_LEN);
//    get_path_s(path_s_n, newpath, MAX_PATH_LEN);
//    if (access(pathname, F_OK) == -1){
//        return -errno;
//    }
//    TRY(symlink(pathname, newpath));
//    return ret;
}

int cloudfs_readlink(const char *pathname UNUSED, char *buf UNUSED, size_t bufsize UNUSED) {
//    NOI();
//
//    fprintf(logfile,"[%s]:\tpathname:%s\n", __func__, pathname);
//
//    char path_s[MAX_PATH_LEN];
//    get_path_s(path_s, pathname, MAX_PATH_LEN);
//    if (is_on_cloud(path_s)) {
//        fprintf(logfile,"[%s]:\tpath_s:%s is on cloud\n", __func__, path_s);
//    }else{
//        fprintf(logfile,"[%s]:\tpath_s:%s is not on cloud\n", __func__, path_s);
//    }
//    return 0;

    PF("[%s]:\t pathname: %s\n", __func__, pathname);
    INFOF();
    int ret = 0;

    char path_s[MAX_PATH_LEN];
    get_path_s(path_s, pathname, MAX_PATH_LEN);
    PF("[utimens] path_s is %s\n", path_s);

    ret = readlink(path_s, buf, bufsize);
    if (ret == -1) {
        return -errno;
    }

    buf[ret] = '\0';
    return 0;
}

void show_fuse_state() {
    PF("[start] fstate->ssd_path is %s\n", fstate->ssd_path);
    PF("fstate->fuse_path is %s\n", fstate->fuse_path);
    PF("fstate->hostname is %s\n", fstate->hostname);
    PF("fstate->ssd_size is %d\n", fstate->ssd_size);
    PF("fstate->thresholdis %d\n", fstate->threshold);
    PF("fstate->avg_seg_size is %d\n", fstate->avg_seg_size);
    PF("fstate->min_seg_size is %d\n", fstate->min_seg_size);
    PF("fstate->max_seg_size is %d\n", fstate->max_seg_size);
    PF("fstate->cache_size is %d\n", fstate->cache_size);
    PF("fstate->no_dedup is %d\n", fstate->no_dedup);
    PF("fstate->rabin_window_size is %d\n", fstate->rabin_window_size);

}


int cloudfs_start(struct cloudfs_state *state,
                  const char *fuse_runtime_name) {
    // This is where you add the VFS functions for your implementation of CloudFS.
    // You are NOT required to implement most of these operations, see the writeup
    //
    // Different operations take different types of parameters, see
    // /usr/include/fuse/fuse.h for the most accurate information
    //
    // In addition to this, the documentation for the latest version of FUSE
    // can be found at the following URL:
    // --- https://libfuse.github.io/doxygen/structfuse__operations.html

//    cloudfs_operations.init = cloudfs_init;
    cloudfs_operations.destroy = cloudfs_destroy;
    cloudfs_operations.getattr = cloudfs_getattr;
    cloudfs_operations.mkdir = cloudfs_mkdir;
    cloudfs_operations.utimens = cloudfs_utimens;
    cloudfs_operations.open = cloudfs_open;
    cloudfs_operations.readdir = cloudfs_readdir;
    cloudfs_operations.opendir = cloudfs_opendir;
    cloudfs_operations.getxattr = cloudfs_getxattr;
    cloudfs_operations.setxattr = cloudfs_setxattr;
    cloudfs_operations.mknod = cloudfs_mknod;
    cloudfs_operations.read = cloudfs_read;
    cloudfs_operations.write = cloudfs_write;
    cloudfs_operations.release = cloudfs_release;
    cloudfs_operations.init = cloudfs_init;
    cloudfs_operations.access = cloudfs_access;
    cloudfs_operations.chmod = cloudfs_chmod;
    cloudfs_operations.link = cloudfs_link;
    cloudfs_operations.symlink = cloudfs_symlink;
    cloudfs_operations.readlink = cloudfs_readlink;
    cloudfs_operations.unlink = cloudfs_unlink;
    cloudfs_operations.rmdir = cloudfs_rmdir;
    cloudfs_operations.truncate = cloudfs_truncate;
    cloudfs_operations.ioctl = cloudfs_ioctl;

    int argc = 0;
    char *argv[10];
    argv[argc] = (char *) malloc(strlen(fuse_runtime_name) + 1);
    strcpy(argv[argc++], fuse_runtime_name);
    argv[argc] = (char *) malloc(strlen(state->fuse_path) + 1);
    strcpy(argv[argc++], state->fuse_path);
    argv[argc] = (char *) malloc(strlen("-s") + 1);
    strcpy(argv[argc++], "-s"); // set the fuse mode to single thread
    // argv[argc] = (char *) malloc(sizeof("-f") * sizeof(char));
    // argv[argc++] = "-f"; // run fuse in foreground

    state_ = *state;
    fstate = &state_;
    char log_path[MAX_PATH_LEN];
    time_t now;
    time(&now);

    struct tm *timeinfo;


    timeinfo = localtime(&now);


    strftime(log_path, sizeof(log_path), "/tmp/cloudfs%Y-%m-%d-%H-%M.log", timeinfo);
    logfile = fopen("/tmp/cloudfs.log", "w");
//    logfile = fopen(log_path, "a");
    PF("\n\n\n\n\n\n\n\n\nRuntime is %s\n", ctime(&now));


    setvbuf(logfile, NULL, _IOLBF, 0);
    INFOF();
//    fprintf(logfile,"\n[%s\t]Line:\n", __func__, __LINE__)

    show_fuse_state();

    int fuse_stat = fuse_main(argc, argv, &cloudfs_operations, NULL);

    return fuse_stat;
}
