// ref: https://github.com/libfuse/libfuse/blob/master/example/passthrough.c
// ref: https://github.com/libfuse/libfuse/blob/master/example/passthrough.c
// ref: https://github.com/libfuse/libfuse/blob/master/example/passthrough.c
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

#define SHOWINFO

#define UNUSED __attribute__((unused))
#define PF(...) fprintf(logfile,__VA_ARGS__)


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


#define ON_SSD 0
#define ON_CLOUD 1
#define N_DIRTY 0
#define DIRTY 1

#define BUCKET ("test")

static struct cloudfs_state state_;
static struct cloudfs_state *fstate;
FILE *logfile;
static struct fuse_operations cloudfs_operations;

static FILE *outfile;

int get_buffer(const char *buffer, int bufferLength) {

    PF("[%s]get buffer %d\n",__func__, bufferLength);
    return fwrite(buffer, 1, bufferLength, outfile);
}

static FILE *infile;

int put_buffer(char *buffer, int bufferLength) {
    PF("[%s]put buffer %d\n",__func__, bufferLength);
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

static int cloudfs_error(const char *error_str) {
    int ret = -errno;

    // TODO:
    //
    // You may want to add your own logging/debugging functions for printing
    // error messages. For example:
    //
    // debug_msg("ERROR happened. %s\n", error_str, strerror(errno));
    //

    fprintf(stderr, "CloudFS Error: %s\n", error_str);
    fprintf(logfile, "[CloudFS Error]: %s \t[ERRNO]: %d: %s\n", error_str, -ret, strerror(errno));

    /* FUSE always returns -errno to caller (yes, it is negative errno!) */
    return ret;
}

/*
 * Initializes the FUSE file system (cl udfs) by checking if the mount points
 * are valid, and if all is well, it mounts the file system ready for usage.
 *
 */
void *cloudfs_init(struct fuse_conn_info *conn UNUSED) {

    char temp_dir[MAX_PATH_LEN] = ".tempfiles";

    char temp_dir_ssd[MAX_PATH_LEN];


    snprintf(temp_dir_ssd, MAX_PATH_LEN, "%s%s", fstate->ssd_path, temp_dir);
    cloud_init(state_.hostname);
    cloud_print_error();

    cloud_create_bucket(BUCKET);
    cloud_print_error();
    mkdir(temp_dir_ssd, S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
    return NULL;
}

void cloudfs_destroy(void *data UNUSED) {

    cloud_delete_bucket(BUCKET);
    cloud_print_error();

    cloud_destroy();
    cloud_print_error();
}

void get_path_s(char *full_path, const char *pathname, int bufsize) {
    INFOF();
    if (pathname[0] == '/') {
        snprintf(full_path, bufsize, "%s%s", fstate->ssd_path, pathname + 1);
    } else {
        snprintf(full_path, bufsize, "%s%s", fstate->ssd_path, pathname);
    }

    PF("[%s]\t pathname is %s\n", __func__, pathname);
}

void get_path_t(char *path_t, const char *pathname, int bufsize) {
    INFOF();
    char path_s[MAX_PATH_LEN];
    char path_c[MAX_PATH_LEN];
    char temp_dir[MAX_PATH_LEN] = ".tempfiles";
    if (pathname[0] == '/') {
        snprintf(path_s, bufsize, "%s%s", fstate->ssd_path, pathname + 1);
    } else {
        snprintf(path_s, bufsize, "%s%s", fstate->ssd_path, pathname);
    }

    strcpy(path_c, path_s);
    for (int i = 0; path_c[i] != '\0' ; i++) {
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

int cloudfs_getattr(const char *pathname, struct stat *statbuf) {

    PF("[%s]:\t pathname: %s\n", __func__, pathname);
    INFOF();
    int ret = 0;

    //
    // TODO:
    //
    // Implement this function to do whatever it is supposed to do!
    //
    char path_s[MAX_PATH_LEN];
    get_path_s(path_s, pathname, MAX_PATH_LEN);

    ret = lstat(path_s, statbuf);
    if (ret < 0) {
        return -errno;
    } else {
        if (is_on_cloud(path_s)) {
            get_from_proxy(path_s, statbuf);
        }
    }



//    ret = lstat(path_s, statbuf);
//    if (ret < 0) {
//        ret = cloudfs_error("getattr failed");
//    }
    return ret;
}

int cloudfs_open(const char *pathname, struct fuse_file_info *fi) {

    PF("[%s]:\t pathname: %s\n", __func__, pathname);
    INFO();
//    int ret = 0;
    int fd;

    char path_s[MAX_PATH_LEN];
//    char path_c[MAX_PATH_LEN];
    get_path_s(path_s, pathname, MAX_PATH_LEN);

//    get_path_c(path_c, path_s);

    if (is_on_cloud(path_s)) {
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

        PF("[%s]:\t opening %s with flag %d\n", __func__, path_t, fi->flags);
        fd = open(path_t, O_RDWR);

        fi->fh = fd;

        PF("[%s]:\t open returned %d\t file is %s \t size is %zu \t st_ino is %zu\n", __func__, fd, path_t, size_f, ino_f);
        if (fd < 0) {
            return cloudfs_error(__func__);
        }

        return 0;
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

//    PF("[%s]:\t pathname: %s\n", __func__, pathname);
//    INFOF();
//    DIR *d;
//    struct dirent *de;
//    char lost_found[MAX_PATH_LEN];
//    get_path_s(lost_found, "/lost+found", MAX_PATH_LEN);
//    d = (DIR * )(uintptr_t)
//    fi->fh;
//    de = readdir(d);
//    if (de == NULL) {
//        return cloudfs_error("readdir failed");;
//    }

//    PF("[%s]: pathname: %s\n", __func__, pathname);
//    INFOF();
//    DIR *d;
//    char lost_found[MAX_PATH_LEN];
//    get_path_s(lost_found, "/lost+found", MAX_PATH_LEN);
//    struct dirent *de;
//    struct stat s;
//    d = (DIR * )(uintptr_t)
//    fi->fh;
//    de = readdir(d);
//    if (de == NULL) {
//        return cloudfs_error("readdir failed");;
//    }
//    while (1) {
//
//        char dirpath[MAX_PATH_LEN];
//        get_path_s(dirpath, de->d_name, MAX_PATH_LEN);
//        if (!strcmp(dirpath, lost_found)) {
//            //get rid of the annoying lost+found path
//            continue;
//        }
//        memset(&s, 0, sizeof(s));
//        s.st_ino = de->d_ino;
//        s.st_mode = de->d_type << 12;
//        if (filler(buf, de->d_name, &s, 0) != 0) {
//            return -ENOMEM;
//        }
//        if ((de = readdir(d)) == NULL) {
//            break;
//        }
//    }


//    while ((de = readdir(d)) != NULL) {
//
//        char dirpath[MAX_PATH_LEN];
//        get_path_s(dirpath, de->d_name, MAX_PATH_LEN);
//        if (!strcmp(dirpath, lost_found)) {
//            //get rid of the annoying lost+found path
//            continue;
//        }
//
//        struct stat st;
//        memset(&st, 0, sizeof(st));
//        st.st_ino = de->d_ino;
//        st.st_mode = de->d_type << 12;
//        if (filler(buf, de->d_name, &st, 0)){
//            break;
//        }
//        PF("[%s]:\t buf: %s\n", __func__, buf);
//
//    }
//    closedir(d);
//    return 0;


    int ret = 0;
    DIR *dp;
    struct dirent *de;
    char lost_found[MAX_PATH_LEN];
    get_path_s(lost_found, "/lost+found", MAX_PATH_LEN);

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
        if (!strcmp(dirpath, lost_found)) {
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
    get_path_s(path_s, pathname, MAX_PATH_LEN);
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
    get_path_s(path_s, pathname, MAX_PATH_LEN);
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

int cloudfs_mknod(const char *pathname UNUSED, mode_t mode UNUSED, dev_t dev UNUSED) {

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

int cloudfs_read(const char *pathname UNUSED, char *buf UNUSED, size_t size UNUSED, off_t offset UNUSED,
                 struct fuse_file_info *fi) {

    PF("[%s]:\t pathname: %s\n", __func__, pathname);
    INFO();
    int ret = 0;
    char path_s[MAX_PATH_LEN];
    get_path_s(path_s, pathname, MAX_PATH_LEN);

    PF("[%s]:\t pread(fd: %d)\t file is %s\n", __func__, fi->fh, path_s);
    TRY(pread(fi->fh, buf, size, offset));
    return ret;
}

int cloudfs_write(const char *pathname UNUSED, const char *buf UNUSED, size_t size UNUSED, off_t offset UNUSED,
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

//    ret = pwrite(fi->fh, buf, size, offset);
//    if (ret < 0) {
//        ret = cloudfs_error("write failed");
//
//    }
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


void cloud_put(char *path_s, char *path_c, struct stat *statbuf_p) {

    infile = fopen(path_s, "rb");
    cloud_put_object(BUCKET, path_c, statbuf_p->st_size, put_buffer);
    cloud_print_error();

    PF("[%s]:\t put %s on cloud with key:[%s], size : %zu\n", __func__, path_s, path_c, statbuf_p->st_size);
    fclose(infile);

//    FILE *fp;
//    char ch;
//    fp=fopen(path_s,"r");
//    int i;
//    PF("showing file content %s: ", path_s);
//    while((ch=fgetc(fp))!=EOF && i < 25)
//    {
//        PF("%c", ch);
//    }
//
//    PF("\n");
//    fclose(fp);

//    outfile = fopen("/tmp/peek", "wb");
//    cloud_get_object(BUCKET, path_c, get_buffer);
//    cloud_print_error();
//    fclose(outfile);
//
//    fp=fopen("/tmp/peek","r");
//    PF("PEEKING file content %s @ cloud %s: ", path_s, path_c);
//    while((ch=fgetc(fp))!=EOF && i < 25)
//    {
//        PF("%c", ch);
//    }
//
//    PF("\n");
//    fclose(fp);


}

void cloud_get(char *path_s, char *path_c) {

    PF("[%s]:\t path_s = [%s]\n", __func__, path_s);
    outfile = fopen(path_s, "wb");
    cloud_get_object(BUCKET, path_c, get_buffer);
    cloud_print_error();
    PF("[%s]:\t get %s(FD:%d) from cloud with key:[%s]\n", __func__, path_s, outfile, path_c);
    PF("[%s]:\t return\n", __func__);
    fclose(outfile);


    FILE *fp;
    char ch;
    fp=fopen(path_s,"r");
    int i;
    PF("[%s]: showing file content %s: ", __func__, path_s);
    while((ch=fgetc(fp))!=EOF && i < 25)
    {
        PF("%c", ch);
    }

    PF("\n");
    fclose(fp);
}


int clone_2_proxy(char *path_s, struct stat *statbuf_p) {
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
    TRY(lsetxattr(path_s, "user.st_mode", &statbuf_p->st_mode, sizeof(mode_t), 0));
    TRY(lsetxattr(path_s, "user.st_nlink", &statbuf_p->st_nlink, sizeof(nlink_t), 0));
    TRY(lsetxattr(path_s, "user.st_uid", &statbuf_p->st_uid, sizeof(uid_t), 0));
    TRY(lsetxattr(path_s, "user.st_gid", &statbuf_p->st_gid, sizeof(gid_t), 0));
    TRY(lsetxattr(path_s, "user.st_rdev", &statbuf_p->st_rdev, sizeof(dev_t), 0));
    TRY(lsetxattr(path_s, "user.st_size", &statbuf_p->st_size, sizeof(off_t), 0));
    TRY(lsetxattr(path_s, "user.st_blksize", &statbuf_p->st_blksize, sizeof(blksize_t), 0));
    TRY(lsetxattr(path_s, "user.st_blocks", &statbuf_p->st_blocks, sizeof(blkcnt_t), 0));
    return ret;
}

int get_from_proxy(char *path_s, struct stat *statbuf_p) {
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
    TRY(lgetxattr(path_s, "user.st_dev", &statbuf_p->st_dev, sizeof(dev_t)));
    TRY(lgetxattr(path_s, "user.st_ino", &statbuf_p->st_ino, sizeof(ino_t)));
    TRY(lgetxattr(path_s, "user.st_mode", &statbuf_p->st_mode, sizeof(mode_t)));
    TRY(lgetxattr(path_s, "user.st_nlink", &statbuf_p->st_nlink, sizeof(nlink_t)));
    TRY(lgetxattr(path_s, "user.st_uid", &statbuf_p->st_uid, sizeof(uid_t)));
    TRY(lgetxattr(path_s, "user.st_gid", &statbuf_p->st_gid, sizeof(gid_t)));
    TRY(lgetxattr(path_s, "user.st_rdev", &statbuf_p->st_rdev, sizeof(dev_t)));
    TRY(lgetxattr(path_s, "user.st_size", &statbuf_p->st_size, sizeof(off_t)));
    TRY(lgetxattr(path_s, "user.st_blksize", &statbuf_p->st_blksize, sizeof(blksize_t)));
    TRY(lgetxattr(path_s, "user.st_blocks", &statbuf_p->st_blocks, sizeof(blkcnt_t)));
    return ret;
}

int cloudfs_release(const char *pathname UNUSED, struct fuse_file_info *fi UNUSED) {

    PF("[%s]:\t pathname: %s\n", __func__, pathname);
    INFOF();

    char path_s[MAX_PATH_LEN];
    char path_c[MAX_PATH_LEN];
    char path_t[MAX_PATH_LEN];
    get_path_s(path_s, pathname, MAX_PATH_LEN);
    get_path_c(path_c, path_s);
    get_path_t(path_t, pathname, MAX_PATH_LEN);

    PF("[%s]:\t pathname = %s \t path_s = %s \t path_c = %s \t path_t = %s\n", __func__, pathname, path_s, path_c, path_t);
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
            PF("[%s]:\t file %s is dirty\n", __func__, pathname);

            struct stat statbuf;
            RUN_M(lstat(path_s, &statbuf));

            size_t size_f = statbuf.st_size;

            PF("[%s]:\t file %s size: %zu\n", __func__, pathname, size_f);

            if (size_f > fstate->threshold) {//larger than threshold, need to move to cloud
                PF("[%s]:\t dirty file %s need to be moved to cloud\n", __func__, pathname);
                NOI();
                // TODO
                // TODO
                // TODO
                // TODO
                // TODO
            } else {//remain on ssd
                PF("[%s]:\t dirty file %s remain on ssd\n", __func__, pathname);
                NOI();
                // TODO
                // TODO
                // TODO
                // TODO
                // TODO
            }
        } else {//not dirty
            PF("[%s]:\t file %s not dirty\n", __func__, pathname);
            struct stat statbuf;
            RUN_M(lstat(path_s, &statbuf));

            size_t size_f = statbuf.st_size;
            PF("[%s]:\t file %s size: %zu\n", __func__, pathname, size_f);
            if (size_f > fstate->threshold) {//larger than threshold, need to move to cloud
                PF("[%s]:\t clean file %s need to be put on cloud, cloud path is %s\n", __func__, pathname, path_c);
//                NOI();
//                infile = fopen(path_s, "rb");
//                cloud_put_object(BUCKET, path_c, statbuf.st_size, put_buffer);
//                cloud_print_error();
//                fclose(infile);

                PF("[line: %d]:\t", __LINE__);

                cloud_put(path_s, path_c, &statbuf);
                PF("[line: %d]:\t", __LINE__);
                FILE *fd = fopen(path_s, "w");
                fclose(fd);
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
            RUN_M(lstat(path_s, &statbuf));

            size_t size_f = statbuf.st_size;

            if (size_f <= fstate->threshold) {//smaller than threshold, need to move back to ssd
                NOI();
                // TODO
                // TODO
                // TODO
                // TODO
                // TODO

            } else {//remain on cloud
                NOI();
                // TODO
                // TODO
                // TODO
                // TODO
                // TODO

            }
        } else {//not dirty

            PF("[%s]:\t clean file %s stay on cloud\n", __func__, pathname);
//            NOI();
            // TODO
            // TODO
            // TODO
            // TODO
            // TODO


            struct stat statbuf;
            RUN_M(lstat(path_s, &statbuf));

            FILE *fd = fopen(path_s, "w");
            fclose(fd);
            set_loc(path_s, ON_CLOUD);
            set_dirty(path_s, N_DIRTY);
            RUN_M(clone_2_proxy(path_s, &statbuf));
        }
    } else {
        return cloudfs_error("release failed");
    }

}


int cloudfs_access(const char *pathname UNUSED, int mask UNUSED) {

    PF("[%s]:\t pathname: %s\n", __func__, pathname);
//    INFOF();
    int ret = 0;
    char path_s[MAX_PATH_LEN];
    get_path_s(path_s, pathname, MAX_PATH_LEN);
    TRY(access(path_s, mask));
//    ret = access(path_s, mask);
//    if (ret < 0) {
//        return cloudfs_error("access failed");
//    }
    return ret;
}

int cloudfs_chmod(const char *pathname UNUSED, mode_t mode UNUSED) {
//    NOI();

    PF("[%s]:\t pathname: %s\n", __func__, pathname);
    INFOF();
    int ret = 0;

    char path_s[MAX_PATH_LEN];
    get_path_s(path_s, pathname, MAX_PATH_LEN);
    PF("[utimens] path_s is %s\n", path_s);

    TRY(cloudfs_chmod(path_s, mode));

//    ret = utimensat(0, path_s, tv, AT_SYMLINK_NOFOLLOW);
//    if (ret < 0) {
//        return cloudfs_error("utimens failed");
//    }

    return ret;
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
    PF("[%s]:\t pathname: %s\n", __func__, pathname);
    INFOF();
    int ret = 0;

    char path_s[MAX_PATH_LEN];
    get_path_s(path_s, pathname, MAX_PATH_LEN);
    PF("[utimens] path_s is %s\n", path_s);

    TRY(unlink(path_s));

//    ret = utimensat(0, path_s, tv, AT_SYMLINK_NOFOLLOW);
//    if (ret < 0) {
//        return cloudfs_error("utimens failed");
//    }

    return ret;
}

int cloudfs_truncate(const char *path UNUSED, off_t newsize UNUSED) {
    NOI();
}

int cloudfs_link(const char *path UNUSED, const char *newpath UNUSED) {
    NOI();
}

int cloudfs_symlink(const char *dst UNUSED, const char *path UNUSED) {
    NOI();
}

int cloudfs_readlink(const char *path UNUSED, char *buf UNUSED, size_t bufsize UNUSED) {
    NOI();
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
    char log_path[MAX_PATH_LEN];
    time_t now;
    time(&now);

    struct tm *timeinfo;


    timeinfo = localtime(&now);


    strftime(log_path, sizeof(log_path), "/tmp/cloudfs%Y-%m-%d-%H-%M-%S.log", timeinfo);
    logfile = fopen(log_path, "w");
    PF("Runtime is %s\n", ctime(&now));

    setvbuf(logfile, NULL, _IOLBF, 0);
    INFOF();
//    fprintf(logfile,"\n[%s\t]Line:\n", __func__, __LINE__)

    show_fuse_state();


    int fuse_stat = fuse_main(argc, argv, &cloudfs_operations, NULL);

    return fuse_stat;
}
