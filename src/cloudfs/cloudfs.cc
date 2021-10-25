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
#include <time.h>
#include <unistd.h>
#include "cloudapi.h"
#include "dedup.h"
#include "cloudfs.h"

#define UNUSED __attribute__((unused))
#define PF(...) fprintf(logfile,__VA_ARGS__)
#define INFO() fprintf(logfile,"INFO: SSD ONLY [%s] Line:%d\n", __func__, __LINE__)
#define INFOF() fprintf(logfile,"INFO: F [%s] Line:%d\n", __func__, __LINE__)
#define INFON() fprintf(logfile,"INFO: NOT IMPLEMENTED [%s] Line:%d \n\n", __func__, __LINE__)
#define TRY(x, y) \
    if ((ret = x) < 0) { \
        ret = cloudfs_error(y); \
    }


static struct cloudfs_state state_;
static struct cloudfs_state *fstate;
FILE *logfile;
static struct fuse_operations cloudfs_operations;

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

static int cloudfs_error(char *error_str) {
    int ret = -errno;

    // TODO:
    //
    // You may want to add your own logging/debugging functions for printing
    // error messages. For example:
    //
    // debug_msg("ERROR happened. %s\n", error_str, strerror(errno));
    //

    fprintf(stderr, "CloudFS Error: %s\n", error_str);

    /* FUSE always returns -errno to caller (yes, it is negative errno!) */
    return ret;
}

/*
 * Initializes the FUSE file system (cl udfs) by checking if the mount points
 * are valid, and if all is well, it mounts the file system ready for usage.
 *
 */
void *cloudfs_init(struct fuse_conn_info *conn UNUSED) {
    cloud_init(state_.hostname);
    cloud_print_error();

    cloud_create_bucket("test");
    cloud_print_error();
    return NULL;
}

void cloudfs_destroy(void *data UNUSED) {

    cloud_delete_bucket("test");
    cloud_print_error();
    
    cloud_destroy();
    cloud_print_error();
}

void ssd_path(char *full_path, const char *pathname, int bufsize) {
    INFOF();
    snprintf(full_path, bufsize, "%s%s", fstate->ssd_path, pathname + 1);
    PF("[ssd_path] pathname is %s\n", pathname);
}

int cloudfs_getattr(const char *pathname, struct stat *statbuf) {
    INFOF();
    int ret = 0;

    //
    // TODO:
    //
    // Implement this function to do whatever it is supposed to do!
    //
    char file_path[MAX_PATH_LEN];
    ssd_path(file_path, pathname, MAX_PATH_LEN);

    TRY(lstat(file_path, statbuf), "getattr failed");
//    ret = lstat(file_path, statbuf);
//    if (ret < 0) {
//        ret = cloudfs_error("getattr failed");
//        INFO();
//    }
    return ret;
}

int cloudfs_open(const char *pathname, struct fuse_file_info *fi) {
    INFO();
//    int ret = 0;
    int fd;

    char file_path[MAX_PATH_LEN];
    ssd_path(file_path, pathname, MAX_PATH_LEN);

    PF("[open] file_path is %s\n", file_path);
    fd = open(file_path, fi->flags);
    if (fd < 0) {
        return cloudfs_error("open failed");
    }
    fi->fh = fd;
    return 0;
}



int cloudfs_opendir(const char *pathname UNUSED, struct fuse_file_info *fi UNUSED) {
    INFO();
    DIR *d = NULL;
    char file_path[MAX_PATH_LEN];
    ssd_path(file_path, pathname, MAX_PATH_LEN);
    d = opendir(file_path);
    if (!d) {
        return cloudfs_error("opendir failed");
    }
    fi->fh = (intptr_t) d;
    return 0;
}


int cloudfs_mkdir(const char *pathname, mode_t mode) {
    INFO();
    int ret = 0;

    char file_path[MAX_PATH_LEN];
    ssd_path(file_path, pathname, MAX_PATH_LEN);
    PF("[mkdir] file_path is %s\n", file_path);
    TRY(mkdir(file_path, mode), "mkdir failed");
//    ret = mkdir(file_path, mode);
//    if (ret < 0) {
//        return cloudfs_error("mkdir failed");
//    }

    return ret;

}

int cloudfs_utimens(const char *pathname, const struct timespec tv[2]) {
    INFO();
    int ret = 0;

    char file_path[MAX_PATH_LEN];
    ssd_path(file_path, pathname, MAX_PATH_LEN);
    PF("[utimens] file_path is %s\n", file_path);

    TRY(utimensat(0, file_path, tv, AT_SYMLINK_NOFOLLOW), "utimens failed");

//    ret = utimensat(0, file_path, tv, AT_SYMLINK_NOFOLLOW);
//    if (ret < 0) {
//        return cloudfs_error("utimens failed");
//    }

    return ret;

}

int cloudfs_readdir(const char *pathname UNUSED, void *buf UNUSED, fuse_fill_dir_t filler UNUSED, off_t offset UNUSED,
                    struct fuse_file_info *fi UNUSED) {
    INFO();
    DIR *d;
    struct dirent *de;
    struct stat s;
    d = (DIR * )(uintptr_t)
    fi->fh;
    de = readdir(d);
    if (de == NULL) {
        return cloudfs_error("readdir failed");;
    }
    while (1) {
        memset(&s, 0, sizeof(s));
        s.st_ino = de->d_ino;
        s.st_mode = de->d_type << 12;
        if (filler(buf, de->d_name, &s, 0) != 0) {
            return -ENOMEM;
        }
        if ((de = readdir(d)) == NULL) {
            break;
        }
    }
    return 0;
}

int cloudfs_getxattr(const char *pathname UNUSED, const char *name UNUSED, char *value UNUSED,
                     size_t size UNUSED) {
    INFO();
    int ret = 0;
    char file_path[MAX_PATH_LEN];
    ssd_path(file_path, pathname, MAX_PATH_LEN);
    PF("[getxattr] file_path is %s\n", file_path);


    TRY(lgetxattr(file_path, name, value, size), "getxattr failed");
//    ret = lgetxattr(file_path, name, value, size);
//    if (ret < 0) {
//        return cloudfs_error("getxattr failed");;
//    }
    return ret;
}


int cloudfs_setxattr(const char *path UNUSED, const char *name UNUSED, const char *value UNUSED,
                     size_t size UNUSED, int flags UNUSED) {
    INFON();
}

int cloudfs_mknod(const char *pathname UNUSED, mode_t mode UNUSED, dev_t dev UNUSED) {

    INFO();
    int ret = 0;
    char file_path[MAX_PATH_LEN];
    ssd_path(file_path, pathname, MAX_PATH_LEN);
    TRY(mknod(file_path, mode, dev), "mknod failed");
//    ret = mknod(file_path, mode, dev);
//    PF("[mknod] file_path is %s\n", file_path);
//    if (ret < 0) {
//        return cloudfs_error("mknod failed");
//    }
    return ret;
}

int cloudfs_read(const char *path UNUSED, char *buf UNUSED, size_t size UNUSED, off_t offset UNUSED,
                 struct fuse_file_info *fi) {
    INFON();
}

int cloudfs_write(const char *pathname UNUSED, const char *buf UNUSED, size_t size UNUSED, off_t offset UNUSED,
                  struct fuse_file_info *fi) {

    INFO();
    int ret = 0;
    char file_path[MAX_PATH_LEN];
    ssd_path(file_path, pathname, MAX_PATH_LEN);

    TRY(pwrite(fi->fh, buf, size, offset), "write failed");
//    ret = pwrite(fi->fh, buf, size, offset);
//    if (ret < 0) {
//        ret = cloudfs_error("write failed");
//
//    }
    return ret;
}

int cloudfs_release(const char *pathname UNUSED, struct fuse_file_info *fi UNUSED) {
    INFO();

    int ret = 0;
    ret = close(fi->fh);
    return ret;
}


int cloudfs_access(const char *pathname UNUSED, int mask UNUSED) {
    INFO();
    int ret = 0;
    char file_path[MAX_PATH_LEN];
    ssd_path(file_path, pathname, MAX_PATH_LEN);
    TRY(access(file_path, mask), "access failed");
//    ret = access(file_path, mask);
//    if (ret < 0) {
//        return cloudfs_error("access failed");
//    }
    return ret;
}

int cloudfs_chmod(const char *path UNUSED, mode_t mode UNUSED) {
    INFON();
}

int cloudfs_rmdir(const char *path UNUSED) {
    INFON();
}

int cloudfs_unlink(const char *path UNUSED) {
    INFON();
}

int cloudfs_truncate(const char *path UNUSED, off_t newsize UNUSED) {
    INFON();
}

int cloudfs_link(const char *path UNUSED, const char *newpath UNUSED) {
    INFON();
}

int cloudfs_symlink(const char *dst UNUSED, const char *path UNUSED) {
    INFON();
}

int cloudfs_readlink(const char *path UNUSED, char *buf UNUSED, size_t bufsize UNUSED) {
    INFON();
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

    cloudfs_operations.init = cloudfs_init;
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
    cloudfs_operations.destroy = cloudfs_destroy;
    cloudfs_operations.access = cloudfs_access;
    cloudfs_operations.chmod = cloudfs_chmod;
    cloudfs_operations.link = cloudfs_link;
    cloudfs_operations.symlink = cloudfs_symlink;
    cloudfs_operations.readlink = cloudfs_readlink;
    cloudfs_operations.unlink = cloudfs_unlink;
    cloudfs_operations.rmdir = cloudfs_rmdir;
    cloudfs_operations.truncate = cloudfs_truncate;

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
    logfile = fopen("/tmp/cloudfs.log", "w");
    setvbuf(logfile, NULL, _IOLBF, 0);
    INFO();
//    fprintf(logfile,"\n[%s]Line:\n", __func__, __LINE__)
    time_t now;
    time(&now);
    PF("Runtime is %s\n", ctime(&now));
    show_fuse_state();


    int fuse_stat = fuse_main(argc, argv, &cloudfs_operations, NULL);

    return fuse_stat;
}
