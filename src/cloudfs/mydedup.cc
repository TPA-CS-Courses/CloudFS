//
// Created by Wilson_Xu on 2021/11/14.
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
#include "mycache.h"

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
#define SNAPSHOT (".snapshot")
#define CACHEDIR (".cache")

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




void debug_showfile(const char *file, const char *functionname, int line) {
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

int ffclose_(const char *func, FILE *s) {
    PF("[%s]: closed %p\n", func, s);
    return fclose(s);
}

void debug_pseg(const char *segname, std::vector <seg_info_p> segs) {
    for (int i = 0; i < segs.size(); i++) {
        PF("[%s] %s  %d seg md5: %s, size: %zu\n", __func__, segname, i, segs[i]->md5, segs[i]->seg_size);
    }
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
    mycache_init(logfile, fstate);

}

void mydedup_destroy() {


}

void get_tempfile_path_dedup(char *tempfile_path, char *path_s, int bufsize) {

    PF("[%s]: %s\n", __func__, path_s);
    char path_t[MAX_PATH_LEN];
    strcpy(path_t, path_s);
    for (int i = 0; path_t[i] != '\0'; i++) {
        if (path_t[i] == '/') {
            path_t[i] = '+';
        }
    }
    snprintf(tempfile_path, bufsize, "%s%s/%s.tempfile", de_cfg->fstate->ssd_path, TEMPDIR, path_t + 1);
    PF("[%s]\t fileproxy_path is %s\n", __func__, tempfile_path);
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
//    PF("[%s]: md5: %s\t tempseg_path:%s\n", __func__, md5, seg_proxy_path);
}

void get_tempseg_path(char *tempseg_path, const char *md5, int bufsize) {

    snprintf(tempseg_path, bufsize, "%s%s/%s.tempseg", de_cfg->fstate->ssd_path, TEMPSEGDIR, md5);
//    PF("[%s]: md5: %s\t tempseg_path:%s\n", __func__, md5, tempseg_path);
}

void debug_showsegs(std::vector <seg_info_p> segs) {
    long seg_offset = 0;
    for (int i = 0; i < segs.size(); i++) {
//        PF("[%s]: seg[%d]: md5:%s, size:%ld, offset:%ld\n", __func__, i, segs[i]->md5, segs[i]->seg_size, seg_offset);
        seg_offset += segs[i]->seg_size;
    }
}

bool file_exist(const char *path_s) {
    return (access(path_s, 0) == 0);
}

int mydedup_segmentation(char *fpath, std::vector <seg_info_p> &segs) {
    PF("[%s]: %s\n", __func__, fpath);
    int ret = 0;
    int fd;
    fd = open(fpath, O_RDONLY);
    if (fd < 0) {
        ret = cloudfs_error(__func__);
        return ret;
    }

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
//    PF("\n\n[%s]: _____________________________________\n", __func__);

    while ((bytes = read(fd, buf, sizeof buf)) > 0) {
        char *buftoread = (char *) &buf[0];
        while ((len = rabin_segment_next(rp, buftoread, bytes,
                                         &new_segment)) > 0) {
            MD5_Update(&ctx, buftoread, len);
            segment_len += len;

            if (new_segment) {
                MD5_Final(md5, &ctx);

                char md5string[2 * MD5_DIGEST_LENGTH + 1];
//                PF("[%s]: %u ", __func__, segment_len);
                for (b = 0; b < MD5_DIGEST_LENGTH; b++) {
//                    PF("%02x", md5[b]);
                    sprintf(md5string + b * 2, "%02x", md5[b]);
                }
//                PF("\n[%s]: _____________________________________\n", __func__);


                seg_info_p new_seg;
                new_seg = (seg_info_p) malloc(sizeof(seg_info_t));

                new_seg->seg_size = segment_len;

                memset(new_seg->md5, '\0', 2 * MD5_DIGEST_LENGTH + 1);
                memcpy(new_seg->md5, md5string, 2 * MD5_DIGEST_LENGTH);

                segs.push_back(new_seg);

                MD5_Init(&ctx);
                segment_len = 0;
            }

            buftoread += len;
            bytes -= len;

            if (!bytes) {
                break;
            }
        }

        if (len == -1) {
            cloudfs_error(__func__);
            exit(2);
        }
    }
    if (segment_len > 0) {
        MD5_Final(md5, &ctx);
        char md5string[2 * MD5_DIGEST_LENGTH + 1];
        PF("%u\n", segment_len);
        for (b = 0; b < MD5_DIGEST_LENGTH; b++) {
            PF("%02x", md5[b]);
            sprintf(md5string + b * 2, "%02x", md5[b]);
        }

        seg_info_p new_seg = (seg_info_p) malloc(sizeof(seg_info_t));
        new_seg->seg_size = segment_len;
        memset(new_seg->md5, '\0', 2 * MD5_DIGEST_LENGTH + 1);
        memcpy(new_seg->md5, md5string, 2 * MD5_DIGEST_LENGTH);
        segs.push_back(new_seg);

    }

    PF("[%s] number of seg is %zu\n", __func__, segs.size());
//    debug_pseg(__func__, segs);
    close(fd);
}


int mydedup_down_segs(char *path_s, std::vector <seg_info_p> &segs) {
    PF("[%s]:path_s: %s\n", __func__, path_s);
    long ret = 0;
    outfile = FFOPEN__(path_s, "wb");//closed


    if (outfile == NULL) {
        ret = cloudfs_error(__func__);
        FFCLOSE__(outfile);
        return ret;
    }
    PF("[%s]: size is %zu\n", __func__, segs.size());
    for (int i = 0; i < segs.size(); i++) {
        PF("[%s]: i: %d   cloud_get_cache(BUCKET, %s, get_buffer);\n", __func__, i, segs[i]->md5);
//        cloud_get_object(BUCKET, segs[i]->md5, get_buffer);

        cloud_get_cache(segs[i]->md5, segs[i]->seg_size);
        PF("[%s]: i: %d   cloud_get_cache(BUCKET, %s, get_buffer);\n", __func__, i, segs[i]->md5);
    }
    PF("[%s]: FFCLOSE__\n", __func__);
    FFCLOSE__(outfile);
    PF("[%s]: returned\n", __func__);
    return 0;
}




void mydedup_upload_segs(char *path_s, std::vector <seg_info_p> &segs) {
    int ret = 0;
    infile = FFOPEN__(path_s, "rb");

    for (int i = 0; i < segs.size(); i++) {
        char seg_proxy_path[MAX_PATH_LEN];
        get_seg_proxy_path(seg_proxy_path, segs[i]->md5, MAX_PATH_LEN);
        PF("[%s] seg[%d]->md5 = %s\n", __func__, i, segs[i]->md5);

//        PF("[%s] %d:  \n", __func__, __LINE__);
//        debug_pseg(__func__,segs);
        if (!file_exist(seg_proxy_path)) {

            FILE *fp = FFOPEN__(seg_proxy_path, "w");
            FFCLOSE__(fp);
            set_ref(seg_proxy_path, 1);//initial reference value

//            cloud_put_object(BUCKET, segs[i]->md5, segs[i]->seg_size, put_buffer);
            cloud_put_cache(segs[i]->md5, segs[i]->seg_size);
            PF("[%s] cloud_put_cache with key %s\n", __func__, segs[i]->md5);
        } else {
            //already in cloud or cache
//            cloud_access_cache(segs[i]->md5, segs[i]->seg_size);
//            cache_get(segs[i]->md5);
            // T    O DO
            // IS THIS THE BEST WAY?
            int refcnt = 0;
            get_ref(seg_proxy_path, &refcnt);
            set_ref(seg_proxy_path, refcnt + 1);//refcnt plus one
            fseek(infile, segs[i]->seg_size, SEEK_CUR);
        }


//        PF("[%s] %d:  \n", __func__, __LINE__);
//        debug_pseg(__func__,segs);

    }
    FFCLOSE__(infile);

}


void mydedup_upload_file(char *path_s) {
    std::vector <seg_info_p> segs;
    mydedup_segmentation(path_s, segs);
    mydedup_upload_segs(path_s, segs);

    FILE *fp_fileproxy = FFOPEN__(path_s, "w");//closed


    if (fp_fileproxy == NULL) {
        PF("[%s]:open %s at line %d failed with reason [%s] ERROR\n", __func__, path_s, __LINE__, strerror(errno));
        PF("[%s]:open %s %d ERROR\n", __func__, path_s, __LINE__);
    }

    for (int i = 0; i < segs.size(); i++) {
        fprintf(fp_fileproxy, "%s %ld\n", segs[i]->md5, segs[i]->seg_size);
    }

    FFCLOSE__(fp_fileproxy);
}

int mydedup_getattr(const char *pathname, struct stat *statbuf) {
    PF("[%s]:\t pathname: %s\n", __func__, pathname);
    INFOF();
    int ret = 0;

    char path_s[MAX_PATH_LEN];
    get_path_s(path_s, pathname, MAX_PATH_LEN);

    ret = lstat(path_s, statbuf);

    if (ret < 0) {
        return -errno;
    } else {
        if (is_on_cloud(path_s)) {
            size_t total = 0;
//            size_t segsize = 0;
            get_from_proxy(path_s, statbuf);
            std::vector <seg_info_p> segs;
            mydedup_get_seginfo(path_s, segs);
            for (int j = 0; j < segs.size(); j++) {
                total += segs[j]->seg_size;
            }
            statbuf->st_size = total;
            PF("[%s]:\tfile %s have size: %zu\n", __func__, pathname, statbuf->st_size);
        }
    }

    size_t size_f = statbuf->st_size;

    PF("[%s]:\tfile %s have size: %zu\n", __func__, pathname, size_f);
}

void mydedup_get_seginfo(const char *path_s, std::vector <seg_info_p> &segs) {
    PF("[%s]: reading seglist from file\n", __func__);
    FILE *fp_fileproxy = FFOPEN__(path_s, "r");
//    SEEFILE(path_s);
    int num_seg = -1;
    while (!feof(fp_fileproxy)) {

        char line[MAX_PATH_LEN] = "";
        fgets(line, MAX_PATH_LEN, fp_fileproxy);
        if (!line[0]) {
            break;
        }

        num_seg += 1;
//        PF("[%s]: [%s]\n", __func__, line);
        seg_info_p new_seg = (seg_info_p) malloc(sizeof(seg_info_t));//freed

        memset(new_seg->md5, '\0', 2 * MD5_DIGEST_LENGTH + 1);
        memcpy(new_seg->md5, line, 2 * MD5_DIGEST_LENGTH);
        char *num_line = line + (2 * MD5_DIGEST_LENGTH + 1);


        sscanf(num_line, "%ldn", &(new_seg->seg_size));

        segs.push_back(new_seg);
//        PF("[%s]: md5: %s\t seg_size: %zu\n", __func__, new_seg->md5, new_seg->seg_size);
    }
    FFCLOSE__(fp_fileproxy);
    PF("[%s]: read %d segments from file\n", __func__, num_seg + 1);
}


int mydedup_write(const char *pathname UNUSED, const char *buf UNUSED, size_t size UNUSED, off_t offset UNUSED,
                  struct fuse_file_info *fi) {

    PF("[%s]:\t pathname: %s\t offset: %zu\n", __func__, pathname, offset);
    PF("[%s]:\t pathname: %s\t offset: %zu\n", __func__, pathname, offset);
    int ret = 0;
    char path_s[MAX_PATH_LEN];
    get_path_s(path_s, pathname, MAX_PATH_LEN);
    int loc = ON_SSD;
    get_loc(path_s, &loc);

    if (loc == ON_SSD) {
        PF("[%s]:\t pathname: %s is local\n", __func__, path_s, offset);
//        ret = cloudfs_write_node(pathname, buf, size, offset, fi);
        int fd = open(path_s, O_WRONLY);
        ret = pwrite(fd, buf, size, offset);

        //read stat
        struct stat statbuf;
        lstat(path_s, &statbuf);
        //upload and save stat to proxy
        if (offset + size > de_cfg->fstate->threshold) {
            PF("[%s] uploading %s", __func__, path_s);
            mydedup_upload_file(path_s);
            clone_2_proxy(path_s, &statbuf);

            set_loc(path_s, ON_CLOUD);
        }
    } else {

        std::vector <seg_info_p> segs;
        mydedup_get_seginfo(path_s, segs);

        PF("[%s]:\t pathname: %s is on cloud\n", __func__, path_s, offset);
        size_t upperbound = 0;
        size_t lowerbound = 0;
        size_t offset_change = 0;
        std::vector <seg_info_p> oldseg1, related_segs, oldseg2;
        int after = 1;
        int prev = 1;

        for (int i = 0; i < segs.size(); i++) {
            lowerbound = upperbound;
            upperbound += segs[i]->seg_size;
            if (upperbound < offset) {
                oldseg1.push_back(segs[i]);
                offset_change = upperbound;
//                PF("[%s]:\t oldseg1.push_back(%s);\n", __func__, segs[i]->md5);
            } else if (lowerbound >= offset + size) {
                if (after > 0) {//N+2 SCHEME
                    related_segs.push_back(segs[i]);

//                    PF("[%s]:\t related_segs.push_back(%s);\n", __func__, segs[i]->md5);
                    after--;
                } else {
                    oldseg2.push_back(segs[i]);
//                    PF("[%s]:\t oldseg2.push_back(%s);\n", __func__, segs[i]->md5);
                }
            } else {
                if (prev > 0) {
                    if (offset_change > 0) {
                        related_segs.push_back(oldseg1[oldseg1.size() - 1]);

//                        PF("[%s]:\t related_segs.push_back(%s);\n", __func__, oldseg1[oldseg1.size() - 1]->md5);
//                        PF("[%s]:\t oldseg1.pop_back(%s);\n", __func__, oldseg1[oldseg1.size() - 1]->md5);
                        offset_change -= oldseg1[oldseg1.size() - 1]->seg_size;
                        oldseg1.pop_back();
                    }
                    prev--;
                }
                related_segs.push_back(segs[i]);
//                PF("[%s]:\t related_segs.push_back(%s);\n", __func__, segs[i]->md5);
            }
        }


        char temp_file_path[MAX_PATH_LEN];
        get_tempfile_path_dedup(temp_file_path, path_s, MAX_PATH_LEN);

        mydedup_down_segs(temp_file_path, related_segs);

        int fd = open(temp_file_path, O_WRONLY);
        PF("[%s] offset is %zu, offset_change is %zu\n", __func__, offset, offset_change);
        ret = pwrite(fd, buf, size, offset - offset_change);

        PF("[%s] pwrite(%s, buf, %zu, %zu) returned %d\n", __func__, temp_file_path, size, offset - offset_change, ret);

        close(fd);

        std::vector <seg_info_p> updated_segs;

        mydedup_segmentation(temp_file_path, updated_segs);

        mydedup_upload_segs(temp_file_path, updated_segs);

        mydedup_remove_segs(related_segs);


        remove(temp_file_path);
        struct stat statbuf;
        get_from_proxy(path_s, &statbuf);
        statbuf.st_size = 0;
        FILE *fp_fileproxy = FFOPEN__(path_s, "w");//closed



        for (int i = 0; i < oldseg1.size(); i++) {
            fprintf(fp_fileproxy, "%s %ld\n", oldseg1[i]->md5, oldseg1[i]->seg_size);
            statbuf.st_size += oldseg1[i]->seg_size;
        }


        for (int i = 0; i < updated_segs.size(); i++) {
            fprintf(fp_fileproxy, "%s %ld\n", updated_segs[i]->md5, updated_segs[i]->seg_size);
            statbuf.st_size += updated_segs[i]->seg_size;
        }


        for (int i = 0; i < oldseg2.size(); i++) {
            fprintf(fp_fileproxy, "%s %ld\n", oldseg2[i]->md5, oldseg2[i]->seg_size);
            statbuf.st_size += oldseg2[i]->seg_size;
        }




        FFCLOSE__(fp_fileproxy);

//        SEEFILE(path_s);


        clone_2_proxy(path_s, &statbuf);

        set_loc(path_s, ON_CLOUD);
    }
    if (ret < 0) {
        return cloudfs_error(__func__);
    }
    return ret;
}


int mydedup_read(const char *pathname, char *buf, size_t size, off_t offset, struct fuse_file_info *fi) {
    int ret = 0;
    PF("[%s]:reading %s at offset %zu, size %zu\n", __func__, pathname, offset, size);
    int loc = ON_SSD;
    char path_s[MAX_PATH_LEN];
    get_path_s(path_s, pathname, MAX_PATH_LEN);

    PF("[%s]:\t getting loc\n", __func__);
    get_loc(path_s, &loc);
    if (loc == ON_SSD) {
        PF("[%s]:\t return cloudfs_read_node(pathname, buf, size, offset, fi);\n", __func__);
        return cloudfs_read_node(pathname, buf, size, offset, fi);
    } else {

        PF("[%s]:\t loc == ON_CLOUD\n", __func__);


        std::vector <seg_info_p> segs;

        mydedup_get_seginfo(path_s, segs);


        size_t upperbound = 0;
        size_t lowerbound = 0;
        size_t offset_change = 0;

        std::vector <seg_info_p> related_segs;

        for (int i = 0; i < segs.size(); i++) {
            lowerbound = upperbound;
            upperbound += segs[i]->seg_size;
            if (upperbound < offset) {
                offset_change += segs[i]->seg_size;
            } else if (lowerbound >= offset + size) {
                break;
            } else {
                related_segs.push_back(segs[i]);
            }
        }

//        debug_pseg("segs", segs);
//        debug_pseg("related_segs", related_segs);
        char temp_file_path[MAX_PATH_LEN];
        get_tempfile_path_dedup(temp_file_path, path_s, MAX_PATH_LEN);
        mydedup_down_segs(temp_file_path, related_segs);

        int fd = open(temp_file_path, O_RDONLY);
        PF("[%s] offset is %zu, offset_change is %zu\n", __func__, offset, offset_change);
        ret = pread(fd, buf, size, offset - offset_change);

        close(fd);

        remove(temp_file_path);
        PF("[%s] pread(%zu, %zu) RETURNED %d\n", __func__, size, offset - offset_change, ret);

        if (ret < 0) {

            return cloudfs_error(__func__);
        }
        return ret;
    }


}

void mydedup_remove_segs(std::vector <seg_info_p> &segs) {
    int i;
    for (i = 0; i < segs.size(); i++) {
        mydedup_remove_one_seg(segs[i]->md5);
    }
}

void mydedup_remove_one_seg(char *md5) {
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
//        PF("[%s]: set ref of %s as ref-1 = %d\n", __func__, seg_proxy_path, ref - 1);
    } else if (ref == 1) {
        cloud_delete_cache(md5);
//        cloud_delete_object(BUCKET, md5);
        remove(seg_proxy_path);
        PF("[%s]: removed key %s from cloud, removed %s\n", __func__, md5, seg_proxy_path);
    } else if (ref < 1) {

        PF("[%s]: ERROR %s with 0 refcnt is not deleted!!!\n", __func__, seg_proxy_path);
        return;
    }
}

int mydedup_release(const char *pathname UNUSED, struct fuse_file_info *fi UNUSED) {
    PF("[%s]:\t pathname: %s\n", __func__, pathname);
    char path_s[MAX_PATH_LEN];
    get_path_s(path_s, pathname, MAX_PATH_LEN);
    PF("[%s]:\t pathname = %s \t path_s = %s \n", __func__, pathname, path_s);

    int ret = close(fi->fh);
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
//    PF("[%s]: \tset refcount of %s as %d\n", __func__, pathname, value);
    FILE *fp2 = fopen(pathname, "w");
    fprintf(fp2, "%d\n", value);

    fclose(fp2);
//    ret = lsetxattr(pathname, "user.ref_cnt", &ref_count, sizeof(int), 0);
    return ret;
}

int get_ref(const char *pathname, int *value_p) {
    int ret;
    FILE *fp = fopen(pathname, "r");
    fscanf(fp, "%d", value_p);
    fclose(fp);
//    ret = lgetxattr(pathname, "user.ref_cnt", value_p, sizeof(int));
//    PF("[%s]: \t%s ref_count is %d\n", __func__, pathname, *value_p);
    return ret;

}


int mydedup_unlink(const char *pathname) {
    int ret = 0;
    PF("[%s]:\t pathname: %s\n", __func__, pathname);
    char path_s[MAX_PATH_LEN];
    get_path_s(path_s, pathname, MAX_PATH_LEN);
    std::vector <seg_info_p> segs;
    if (is_on_cloud(path_s)) {

        mydedup_get_seginfo(path_s, segs);
        mydedup_remove_segs(segs);
    }
    ret = unlink(path_s);
    if (ret < 0) {
        return -errno;
    }
    return ret;
}


int mydedup_truncate(const char *pathname UNUSED, off_t newsize UNUSED) {
    NOI();
    int ret = 0;
    PF("[%s]:\t pathname: %s\n", __func__, pathname);
    char path_s[MAX_PATH_LEN];
    get_path_s(path_s, pathname, MAX_PATH_LEN);
    if (!is_on_cloud(path_s)) {
        PF("[%s]:\t pathname: %s is on SSD\n", __func__, pathname);
        ret = truncate(path_s, newsize);
        if (ret < 0) {
            return -errno;
        }
    } else {
//        if(access(path_s,W_OK) < 0){
//            PF("[%s]: cannot write");
//            return -EACCES;
//        }


        if (newsize <= de_cfg->fstate->threshold) {
            std::vector <seg_info_p> segs;
            mydedup_get_seginfo(path_s, segs);
            size_t lowerbound = 0;
            std::vector <seg_info_p> related_segs;

            for (int i = 0; i < segs.size(); i++) {
                if (lowerbound < newsize) {
                    related_segs.push_back(segs[i]);
                } else {
                    break;
                }
                lowerbound += segs[i]->seg_size;
            }
            mydedup_down_segs(path_s, related_segs);
            ret = truncate(path_s, newsize);
            mydedup_remove_segs(segs);
            set_loc(path_s, ON_SSD);
        } else {
            std::vector <seg_info_p> segs;
            mydedup_get_seginfo(path_s, segs);
            size_t lowerbound = 0;
            size_t upperbound = 0;

            size_t offset_change = 0;
            std::vector <seg_info_p> oldseg1, related_segs, oldseg2;
            int after = 1;

            for (int i = 0; i < segs.size(); i++) {
                lowerbound = upperbound;
                upperbound += segs[i]->seg_size;
                if (upperbound < newsize) {
                    oldseg1.push_back(segs[i]);
                    offset_change = upperbound;
                } else if (lowerbound >= newsize) {
                    if (after == 0) {
                        oldseg2.push_back(segs[i]);
                    } else {
                        related_segs.push_back(segs[i]);
                        after--;
                    }
                } else {
                    related_segs.push_back(segs[i]);
//                    PF("[%s]:\t related_segs.push_back(%s);\n", __func__, segs[i]->md5);
                }
            }

            char temp_file_path[MAX_PATH_LEN];
            get_tempfile_path_dedup(temp_file_path, path_s, MAX_PATH_LEN);
            mydedup_down_segs(temp_file_path, related_segs);

            PF("[%s] original size is %zu, offset_change is %zu\n", __func__, newsize, offset_change);
            ret = truncate(temp_file_path, newsize - offset_change);

            std::vector <seg_info_p> updated_segs;
            mydedup_segmentation(temp_file_path, updated_segs);
            mydedup_upload_segs(temp_file_path, updated_segs);

            mydedup_remove_segs(related_segs);
            mydedup_remove_segs(oldseg2);


            remove(temp_file_path);
            struct stat statbuf;
            get_from_proxy(path_s, &statbuf);
            statbuf.st_size = 0;
            FILE *fp_fileproxy = FFOPEN__(path_s, "w");//closed



            for (int i = 0; i < oldseg1.size(); i++) {
                fprintf(fp_fileproxy, "%s %ld\n", oldseg1[i]->md5, oldseg1[i]->seg_size);
                statbuf.st_size += oldseg1[i]->seg_size;
            }


            for (int i = 0; i < updated_segs.size(); i++) {
                fprintf(fp_fileproxy, "%s %ld\n", updated_segs[i]->md5, updated_segs[i]->seg_size);

                statbuf.st_size += updated_segs[i]->seg_size;
            }


            FFCLOSE__(fp_fileproxy);


            clone_2_proxy(path_s, &statbuf);

            set_loc(path_s, ON_CLOUD);
        }
    }
    return ret;
}






