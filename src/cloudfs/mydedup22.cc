//
// Created by Wilson_Xu on 2021/11/4.
//
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
#include <openssl/md5.h>
#include <time.h>
#include <unistd.h>



#include <vector>
#include <fstream>
#include <string>
#include <map>

#include "cloudapi.h"
#include "dedup.h"
#include "cloudfs.h"
#include "mydedup.h"

#define BUF_SIZE (1024)


#define SHOWPF


#define UNUSED __attribute__((unused))

#ifdef SHOWPF
#define PF(...) fprintf(de_cfg->logfile,__VA_ARGS__)
#else
#define PF(...) sizeof(__VA_ARGS__)
#endif

#define NOI() fprintf(de_cfg->logfile,"INFO: NOT IMPLEMENTED [%s]\t Line:%d \n\n", __func__, __LINE__)

#ifdef SHOWINFO
#define INFO() fprintf(de_cfg->logfile,"INFO: SSD ONLY [%s]\t Line:%d\n", __func__, __LINE__)
#define INFOF() fprintf(de_cfg->logfile,"INFO: F [%s]\t Line:%d\n", __func__, __LINE__)
#else
#define INFO() sizeof(void)
#define INFOF() sizeof(void)
#endif

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

static rabinpoly_t *rp;
struct dedup_config de_cfg_s;
struct dedup_config *de_cfg;
#define SEEFILE(x) debug_showfile((x), __func__, __LINE__)
#define FFOPEN__(x, y) ffopen_(__func__, (x), (y))
#define FFCLOSE__(x) ffclose_(__func__, (x))


FILE *ffopen_(const char *func, const char *filename, const char *mode) {
    FILE *r = fopen(filename, mode);
    PF("[%s]: opend %s at %p\n", func, filename, r);
    return r;
}

int ffclose_(const char *func, FILE *s) {
    PF("[%s]: closed %p\n", func, s);
    return fclose(s);
}


void mydedup_init(int window_size, int avg_seg_size, int min_seg_size, int max_seg_size, FILE *logfile,
                  struct cloudfs_state *fstate) {
    de_cfg = &de_cfg_s;
    de_cfg->window_size = window_size;
    de_cfg->avg_seg_size = avg_seg_size;
    de_cfg->min_seg_size = min_seg_size;
    de_cfg->max_seg_size = max_seg_size;
    de_cfg->logfile = logfile;
    de_cfg->fstate = fstate;

    PF("[%s]:\n", __func__);
}


void get_fileproxy_path(char *fileproxy_path, char *path_s, int bufsize) {

    PF("[%s]: %s\n", __func__, path_s);
    char path_t[MAX_PATH_LEN];
    strcpy(path_t, path_s);
    for (int i = 0; path_t[i] != '\0'; i++) {
        if (path_t[i] == '/') {
            path_t[i] = '+';
        }
    }
    snprintf(fileproxy_path, bufsize, "%s%s/%s.fileproxy", de_cfg->fstate->ssd_path, FILEPROXYDIR, path_t + 1);
    PF("[%s]\t fileproxy_path is %s\n", __func__, fileproxy_path);
}

void get_seg_proxy_path(char *seg_proxy_path, const char *md5, int bufsize) {
    snprintf(seg_proxy_path, bufsize, "%s%s/%s.segproxy", de_cfg->fstate->ssd_path, SEGPROXYDIR, md5);
    PF("[%s]: md5: %s\t tempseg_path:%s\n", __func__, md5, seg_proxy_path);
}

void get_tempseg_path(char *tempseg_path, const char *md5, int bufsize) {

    snprintf(tempseg_path, bufsize, "%s%s/%s.tempseg", de_cfg->fstate->ssd_path, TEMPSEGDIR, md5);
    PF("[%s]: md5: %s\t tempseg_path:%s\n", __func__, md5, tempseg_path);
}


void debug_showsegs(seg_info_p segs[], int seg_count) {
    long seg_offset = 0;
    for (int i = 0; i < seg_count; i++) {
        PF("[%s]: seg[%d]: md5:%s, size:%ld, offset:%ld\n", __func__, i, segs[i]->md5, segs[i]->seg_size, seg_offset);
        seg_offset += segs[i]->seg_size;
    }
}


int mydedup_segmentation(char *fpath, int &num_seg, std::vector <seg_info_p> & segs){
    PF("[%s]: %s\n", __func__, fpath);


//    SEEFILE(fpath);
    int ret = 0;
    int fd;
    fd = open(fpath, O_RDONLY);
    if (fd < 0) {
        ret = cloudfs_error(__func__);
        return ret;
    }

    int i = 0;

    rp = rabin_init(de_cfg->window_size, de_cfg->avg_seg_size, de_cfg->min_seg_size, de_cfg->max_seg_size);

    if (!rp) {
        ret = cloudfs_error(__func__);
        return ret;
    }

    MD5_CTX ctx;
    unsigned char md5[MD5_DIGEST_LENGTH];
    int new_segment = 0;
    int len, segment_len = 0, b;
    char buf[BUF_SIZE];
    int bytes;

    MD5_Init(&ctx);
    PF("\n\n[%s]: _____________________________________\n", __func__);

    while ((bytes = read(fd, buf, sizeof buf)) > 0) {
//        PF("[%s]: bytes = %d, buf:[%s]\n", __func__, bytes, buf);
        char *buftoread = (char *) &buf[0];
        while ((len = rabin_segment_next(rp, buftoread, bytes,
                                         &new_segment)) > 0) {
            MD5_Update(&ctx, buftoread, len);
            segment_len += len;

            if (new_segment) {
                MD5_Final(md5, &ctx);

                char md5string[2 * MD5_DIGEST_LENGTH + 1];
                PF("[%s]: %u ", __func__, segment_len);
                for (b = 0; b < MD5_DIGEST_LENGTH; b++) {
                    PF("%02x", md5[b]);
                    sprintf(md5string + b * 2, "%02x", md5[b]);
                }
                PF("\n[%s]: _____________________________________\n", __func__);

                if (i > MAX_SEG_AMOUNT) {
                    PF("[%s] ERROR i>MAX_SEG_AMOUNT", __func__);
                }


                seg_info_p new_seg;
                new_seg = (seg_info_p) malloc(sizeof(seg_info_t));

                new_seg->seg_size = segment_len;

                memset(new_seg->md5, '\0', 2 * MD5_DIGEST_LENGTH + 1);
                memcpy(new_seg->md5, md5string, 2 * MD5_DIGEST_LENGTH);

                segs.push_back(new_seg);

                i++;

                MD5_Init(&ctx);
                segment_len = 0;
                free(new_seg);


            }

            buftoread += len;
            bytes -= len;

            if (!bytes) {
                break;
            }
        }

        if (len == -1) {
//            fprintf(stderr, "Failed to process the segment\n");
            cloudfs_error(__func__);
            exit(2);
        }
    }
    if (segment_len > 0) {
        MD5_Final(md5, &ctx);
        char md5string[2 * MD5_DIGEST_LENGTH + 1];
        PF("%u ", segment_len);
        for (b = 0; b < MD5_DIGEST_LENGTH; b++) {
            PF("%02x", md5[b]);
            sprintf(md5string + b * 2, "%02x", md5[b]);
        }
        if (i > MAX_SEG_AMOUNT) {
            PF("[%s] ERROR i>MAX_SEG_AMOUNT", __func__);
        }
        PF("\n");

        if (i > MAX_SEG_AMOUNT) {
            PF("[%s] ERROR i>MAX_SEG_AMOUNT", __func__);
        }


        seg_info_p new_seg = (seg_info_p) malloc(sizeof(seg_info_t));

        new_seg->seg_size = segment_len;

        memset(new_seg->md5, '\0', 2 * MD5_DIGEST_LENGTH + 1);
        memcpy(new_seg->md5, md5string, 2 * MD5_DIGEST_LENGTH);

        segs.push_back(new_seg);
        free(new_seg);

        i++;
    }

    num_seg = i;
    PF("[%s] number of seg is %d\n", __func__, (*num_seg));
    close(fd);
}


//int write_FILEPROXY(char *proxy, int num_seg, seg_info_p segs[]) {
//    int ret = 0;
//    int i = 0;
//    FILE *fp = FFOPEN__(proxy, "wb");
//    if (fp == NULL) {
//        ret = cloudfs_error(__func__);
//        return ret;
//    }
//
//    for (i = 0; i < num_seg; i++) {
//        fprintf(fp, "%s,%ld\n", segs[i]->md5, segs[i]->seg_size);
//    }
//
//}

//read from SEGPROXY
int ref_of_md5(char *md5) {
    char seg_proxy_path[MAX_PATH_LEN];
    get_seg_proxy_path(seg_proxy_path, md5, MAX_PATH_LEN);
    int ref;
    get_loc(seg_proxy_path, &ref);
    return ref;
}

void debug_showfile(char *file, const char *functionname, int line) {
    PF("[%s] is called by %s at line %d to show the content of %s\n", __func__, functionname, line, file);

    FILE *f;
    char c;
    f = FFOPEN__(file, "rt");

    while ((c = fgetc(f)) != EOF) {
        PF("%c", c);
    }

    FFCLOSE__(f);

    PF("\n[%s]: END OF %s\n\n\n", __func__, file);
}

long mydedup_determine_range(ret_range_p range, size_t size, off_t offset, std::vector <seg_info_p> & segs, int num_seg) {
    PF("\n[%s]: find range for size: %zu, offset: %zu\n", __func__, size, offset);
    long ret = 0;
    long upper_bound = 0;
    long lower_bound = 0;

    long total_size = 0;
    int i;

    for (i = 0; i < num_seg; i++) {
        total_size += segs[i]->seg_size;
        range->end = num_seg - 1;

    }
    if(offset >= total_size){
        PF("\n[%s]: line %d ERROR??? offset >= total_size %ld\n", __func__, __LINE__, total_size);
        range->start = num_seg - 1;
        range->end = num_seg - 1;
        return total_size - segs[num_seg - 1]->seg_size;
    }


    for (i = 0; i < num_seg; i++) {
        lower_bound = upper_bound;
        upper_bound += segs[i]->seg_size;
        if (offset < upper_bound && offset >= lower_bound) {
            range->start = i;
            ret = lower_bound;
            PF("\n[%s]: start is %d in [%ld-%ld]\n", __func__, i, lower_bound, upper_bound);
            break;
        }
    }


    upper_bound = 0;
    lower_bound = 0;


    if ((size + offset) > total_size) {
        range->end = num_seg - 1;
        PF("\n[%s]: end is %d \n", __func__, range->end);
    } else {
        for (i = 0; i < num_seg; i++) {
            lower_bound = upper_bound;
            upper_bound += segs[i]->seg_size;
            if ((size + offset) <= upper_bound && (size + offset) > lower_bound) {
                range->end = i;

                PF("\n[%s]: end is %d in [%ld-%ld]\n", __func__, i, lower_bound, upper_bound);
                break;
            }
        }
    }
    if(range->end > num_seg){
        PF("\n[%s]: line %d ERROR!!!!! end too large\n", __func__, __LINE__);
    }


    if(range->start > num_seg){
        PF("\n[%s]: line %d ERROR!!!!! start too large\n", __func__, __LINE__);
    }

    PF("\n[%s]: RET IS %ld\n", __func__, ret);
    return ret;
}

int mydedup_readsegs_from_proxy(char *file_proxy, seg_info_p segs[]) {

//    SEEFILE(file_proxy);


    PF("[%s]: reading seglist from file\n", __func__);
    FILE *fp_fileproxy = FFOPEN__(file_proxy, "r");
    int num_seg = -1;
    while (!feof(fp_fileproxy)) {

        char line[MAX_PATH_LEN] = "";
        fgets(line, MAX_PATH_LEN, fp_fileproxy);
        if (!line[0]) {
            break;
        }

        num_seg += 1;
//        PF("[%s]: [%s]\n", __func__, line);
        segs[num_seg] = (seg_info_p) malloc(sizeof(seg_info_t));//freed

        memset(segs[num_seg]->md5, '\0', 2 * MD5_DIGEST_LENGTH + 1);
        memcpy(segs[num_seg]->md5, line, 2 * MD5_DIGEST_LENGTH);
        char *num_line = line + (2 * MD5_DIGEST_LENGTH + 1);


        int i = sscanf(num_line, "%ldn", &segs[num_seg]->seg_size);
//        PF("[%s]: ld from [%s] return %d\n", __func__, num_line, i);

//        PF("[%s]: %d line is [%s], md5: %s, size: %ld\n", __func__, num_seg, line, segs[num_seg]->md5,
//           segs[num_seg]->seg_size);
    }
    FFCLOSE__(fp_fileproxy);
    PF("[%s]: read %d segments from file\n", __func__, num_seg + 1);
    return num_seg + 1;
}


int mydedup_uploadfile_append(char *temp_file_path, char *path_s, std::vector <seg_info_p> & segs_old) {
    int num_seg_old = segs_old

    PF("[%s]: temp_file_path: %s, path_s:%s\n", __func__, temp_file_path, path_s);
    int ret = 0;
    PF("[%s]: Showing original segs\n", __func__);
//    debug_showsegs(segs_old, num_seg_old);


    std::vector <seg_info_p> segs;
    int num_seg = 0;
    PF("[%s]: %d\n", __func__, __LINE__);
    ret = mydedup_segmentation(temp_file_path, &num_seg, segs);

//    PF("[%s]: Showing new segs\n", __func__);
//    debug_showsegs(segs, num_seg);


//    SEEFILE(path_s);

    char file_proxy[MAX_PATH_LEN];

    get_fileproxy_path(file_proxy, path_s, MAX_PATH_LEN);

    PF("[%s]: %d\n", __func__, __LINE__);

    FILE *fp_fileproxy = FFOPEN__(file_proxy, "w");//closed


    if (fp_fileproxy == NULL) {
//        errNum = errno;
        PF("[%s]:open %s at line %d failed with reason [%s] ERROR\n", __func__, file_proxy, __LINE__, strerror(errno));
        PF("[%s]:open %s %d ERROR\n", __func__, file_proxy, __LINE__);
    }

    PF("[%s]: %d\n", __func__, __LINE__);


    int i = 0;

    PF("[%s]: %d\n", __func__, __LINE__);

    for (i = 0; i < num_seg_old - 1; i++) {
//        segs[i]->ref_count = 1;
        fprintf(fp_fileproxy, "%s-%ldn\n", segs_old[i]->md5, segs_old[i]->seg_size);
    }

    PF("[%s]: %d\n", __func__, __LINE__);


    long offset = 0;


    for (i = 0; i < num_seg; i++) {
//        segs[i]->ref_count = 1;
        fprintf(fp_fileproxy, "%s-%ldn\n", segs[i]->md5, segs[i]->seg_size);

//        get_fileproxy_path(file_proxy, path_s, MAX_PATH_LEN);

        mydedup_extract_and_upload_seg(temp_file_path, segs[i], offset);
        offset += segs[i]->seg_size;
    }

    PF("[%s]: line %d\n", __func__, __LINE__);
//    SEEFILE(file_proxy);
    FFCLOSE__(fp_fileproxy);
    struct stat statbuf;
    get_from_proxy(path_s, &statbuf);

    struct stat statbuf2;
    lstat(file_proxy, &statbuf2);

    PF("[%s]: line: %d, size is %zu, size2 is %zu\n", __func__, __LINE__, statbuf.st_size, statbuf2.st_size);
    statbuf.st_size += statbuf2.st_size;

    remove(path_s);


    set_loc(file_proxy, ON_CLOUD);
    set_dirty(file_proxy, DIRTY);

    rename(file_proxy, path_s);
    PF("[%s]: line: %d, size is %zu\n", __func__, __LINE__, statbuf.st_size);
    clone_2_proxy(path_s, &statbuf);
    return 0;
}

void mydedup_remove_seg(char *md5) {
    int ref;
    char seg_proxy_path[MAX_PATH_LEN];

    get_seg_proxy_path(seg_proxy_path, md5, MAX_PATH_LEN);
    if (!file_exist(seg_proxy_path)) {
        PF("[%s]: ERROR %s does not exist!!!\n", __func__, seg_proxy_path);
        return;
    }
    get_ref(seg_proxy_path, &ref);

    if (ref > 1) {
        set_ref(seg_proxy_path, ref - 1);
        PF("[%s]: set ref of %s as ref-1 = %d\n", __func__, seg_proxy_path, ref - 1);
    } else if (ref == 1) {
        cloud_delete_object(BUCKET, md5);
        remove(seg_proxy_path);
        PF("[%s]: removed key %s from cloud, removed %s\n", __func__, md5, seg_proxy_path);
    } else if (ref < 1) {

        PF("[%s]: ERROR %s with 0 refcnt is not deleted!!!\n", __func__, seg_proxy_path);
        return;
    }
}

int mydedup_uploadfile(char *path_s) {

    PF("[%s]: %s\n", __func__, path_s);
    int ret = 0;

    int num_seg = 0;
    std::vector <seg_info_p> segs;

    ret = mydedup_segmentation(path_s, num_seg, segs);

    char file_proxy[MAX_PATH_LEN];

    get_fileproxy_path(file_proxy, path_s, MAX_PATH_LEN);

    FILE *fp_fileproxy = FFOPEN__(file_proxy, "w");//closed


    int i = 0;
    long offset = 0;

    for (i = 0; i < num_seg; i++) {
//        segs[i]->ref_count = 1;
        fprintf(fp_fileproxy, "%s-%ld\n", segs[i]->md5, segs[i]->seg_size);

//        get_fileproxy_path(file_proxy, path_s, MAX_PATH_LEN);

        mydedup_extract_and_upload_seg(path_s, segs[i], offset);
        offset += segs[i]->seg_size;
    }
    FFCLOSE__(fp_fileproxy);
    struct stat statbuf;
    lstat(path_s, &statbuf);
    remove(path_s);


    set_loc(file_proxy, ON_CLOUD);
    set_dirty(file_proxy, DIRTY);

    PF("[%s]: line %d, %s, size is %zu\n", __func__, __LINE__, path_s, statbuf.st_size);
    rename(file_proxy, path_s);
    clone_2_proxy(path_s, &statbuf);
    return 0;
}

bool file_exist(const char *path_s) {
    return (access(path_s, 0) == 0);
}

int mydedup_read_seg(char *buf, seg_info_p seg, int start, int end){

}

int mydedup_download_all(char *path_s, seg_info_p segs[], int end) {
    return mydedup_download_and_combine(path_s, segs, 0, end);
}


int mydedup_download_and_combine(char *path_s, seg_info_p segs[], int start, int end) {
    PF("[%s]:path_s: %s\t start: %d\t end:%d\n", __func__, path_s, start, end);
    long ret = 0;
    FILE *tfp = FFOPEN__(path_s, "wb");//closed
    char *buf;
    long offset = 0;

    if (tfp == NULL) {
        ret = cloudfs_error(__func__);
        FFCLOSE__(tfp);
        return ret;
    }

    for (int i = start; i <= end; i++) {
        char tempseg_path[MAX_SEG_AMOUNT];
        get_tempseg_path(tempseg_path, segs[i]->md5, MAX_SEG_AMOUNT);
        if(!file_exist(tempseg_path)){
            seg_down(tempseg_path, segs[i]->md5);
        }

        FILE *fp = FFOPEN__(tempseg_path, "rb");//closed
        if (fp == NULL) {
            ret = cloudfs_error(__func__);
            FFCLOSE__(fp);
            FFCLOSE__(tfp);
            return ret;
        }

        buf = (char *) malloc(sizeof(char) * segs[i]->seg_size);
        ret = fread(buf, 1, segs[i]->seg_size, fp);
        ret = fwrite(buf, 1, segs[i]->seg_size, tfp);
        PF("[%s]:read from %s and put into %s\n", __func__, tempseg_path, path_s);
        FFCLOSE__(fp);
        free(buf);
        if(i == end){
//            SEEFILE(tempseg_path);
//            SEEFILE(path_s);
        }
    }

    for (int i = start; i < end; i++) {
        char tempseg_path[MAX_SEG_AMOUNT];
        get_tempseg_path(tempseg_path, segs[i]->md5, MAX_SEG_AMOUNT);
        if(file_exist(tempseg_path)){
            remove(tempseg_path);
        }
    }

    FFCLOSE__(tfp);
    tfp = FFOPEN__(path_s, "rb");
    fseek(tfp, 0, SEEK_END);
    ret = ftell(tfp);
    FFCLOSE__(tfp);
    return ret;
//    ret = fseek(fp, offset, SEEK_SET);
//    if (ret < 0) {
//        ret = cloudfs_error(__func__);
//        FFCLOSE__(fp);
//        FFCLOSE__(tfp);
//        return ret;
//    }
//    buf = (char *) malloc(sizeof(char) * len);
//    ret = fread(buf, 1, len, fp);
//    ret = fwrite(buf, 1, len, tfp);
//    free(buf);
//
//    FFCLOSE__(fp);
//    FFCLOSE__(tfp);


}

int mydedup_extract_and_upload_seg(char *path_s, seg_info_p seg, long offset) {
    int ret = 0;

    char seg_proxy_path[MAX_PATH_LEN];
    get_seg_proxy_path(seg_proxy_path, seg->md5, MAX_PATH_LEN);
    FILE *fp;
    if (!file_exist(seg_proxy_path)) {// not in hash table
        //UPLOAD TO CLOUD

        fp = FFOPEN__(seg_proxy_path, "w+");
        FFCLOSE__(fp);

        set_ref(seg_proxy_path, 1);//initial reference value

        char tempseg_path[MAX_PATH_LEN];

        get_tempseg_path(tempseg_path, seg->md5, MAX_PATH_LEN);
        extract_seg(path_s, offset, seg->seg_size, tempseg_path);
        seg_upload(tempseg_path, seg->md5, seg->seg_size);
        remove(tempseg_path);
    } else {
        //already have same segment in cloud
        int refcnt = 0;
        get_ref(seg_proxy_path, &refcnt);
        if (refcnt == 0) {
            PF("[%s]:ERROR!! \tFile %s have 0 refcnt and is not deleted!!!\n", __func__, seg_proxy_path);
        }
        set_ref(seg_proxy_path, refcnt + 1);//refcnt plus one
    }
    return ret;
}

int seg_upload_one(char *pathname, long offset, char *key, long len) {

}

//extract a seg from segpathname and put it in target_file
long extract_seg(char *pathname, long offset, long len, char *target_file) {
    PF("[%s]:pathname: %s\t offset: %ld\t len: %ld\t target_file:%s\n", __func__, pathname, offset, len, target_file);
    long ret = 0;
    FILE *fp = FFOPEN__(pathname, "rb");
    FILE *tfp = FFOPEN__(target_file, "wb");
    char *buf;
    if (fp == NULL) {
        ret = cloudfs_error(__func__);
        FFCLOSE__(fp);
        FFCLOSE__(tfp);
        return ret;
    }
    if (tfp == NULL) {
        ret = cloudfs_error(__func__);
        FFCLOSE__(fp);
        FFCLOSE__(tfp);
        return ret;
    }
    ret = fseek(fp, offset, SEEK_SET);
    if (ret < 0) {
        ret = cloudfs_error(__func__);
        FFCLOSE__(fp);
        FFCLOSE__(tfp);
        return ret;
    }
    buf = (char *) malloc(sizeof(char) * len);
    ret = fread(buf, 1, len, fp);
    ret = fwrite(buf, 1, len, tfp);
    free(buf);

    FFCLOSE__(fp);
    FFCLOSE__(tfp);

    tfp = FFOPEN__(target_file, "rb");
    fseek(tfp, 0, SEEK_END);
    ret = ftell(tfp);
    FFCLOSE__(tfp);

    return ret;
}


int set_ref(const char *pathname, int value) {

    FILE *fp;
//    fp = FFOPEN__(pathname, "r");
    if (!file_exist(pathname)) {// file not exist
        //
        PF("[%s]:ERROR!! \tFile %s does not exist!\n", __func__, pathname);
        return cloudfs_error(__func__);
    }
    int ref_count = value;
    int ret;
    PF("[%s]: \tset refcount of %s as %d\n", __func__, pathname, value);
    ret = lsetxattr(pathname, "user.ref_cnt", &ref_count, sizeof(int), 0);
    return ret;
}

int get_ref(const char *pathname, int *value) {
    int ret;
    ret = lgetxattr(pathname, "user.ref_cnt", value, sizeof(int));
    PF("[%s]: \t%s ref_count is %d\n", __func__, pathname, *value);
    return ret;

}

void ref_plus_one(char *md5) {
    char seg_proxy_path[MAX_PATH_LEN];
    get_seg_proxy_path(seg_proxy_path, md5, MAX_PATH_LEN);

    FILE *fp;
//    fp = FFOPEN__(seg_proxy_path, "r");
    if (file_exist(seg_proxy_path)) {// not in hash table
        PF("[%s]\t create SEGPROXY %s for md5: %s\n", __func__, seg_proxy_path, md5);
        fp = FFOPEN__(seg_proxy_path, "w+");
        FFCLOSE__(fp);
        set_ref(seg_proxy_path, 1);//initial reference value
    } else {
        int refcnt = 0;
        get_ref(seg_proxy_path, &refcnt);
        if (refcnt == 0) {
            PF("[%s]:ERROR!! \tproxy File %s have 0 refcnt and is not deleted!!!\n", __func__, seg_proxy_path);
        }

        PF("[%s]\t SEGPROXY %s for md5: %s already exist, increase old refcnt(%d) by 1\n", __func__, seg_proxy_path,
           md5, refcnt);
        set_ref(seg_proxy_path, refcnt + 1);//refcnt plus one

    }
}


void seg_down(char *pathname, char *key) {
    cloud_get(pathname, key);

//    oufile = FFOPEN__(pathname, "wb");
//    cloud_get_object(BUCKET, key, get_buffer);
//    cloud_print_error();
//    FFCLOSE__(oufile);


}


void seg_upload(char *pathname, char *key, long size) {

    cloud_put(pathname, key, size);
//    infile = FFOPEN__(pathname, "rb");
//    cloud_put_object(BUCKET, key, len, put_buffer);
//    cloud_print_error();
//    FFCLOSE__(infile);
}