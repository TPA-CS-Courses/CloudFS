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

#define BUF_SIZE (1024)


//#define SHOWPF

#define UNUSED __attribute__((unused))

#ifdef SHOWPF
#define PF(...) fprintf(ca_cfg->logfile,__VA_ARGS__)
#else
#define PF(...) sizeof(__VA_ARGS__)
#endif

#define NOI() fprintf(ca_cfg->logfile,"INFO: NOT IMPLEMENTED [%s]\t Line:%d \n\n", __func__, __LINE__)

#ifdef SHOWINFO
#define INFO() fprintf(ca_cfg->logfile,"INFO: SSD ONLY [%s]\t Line:%d\n", __func__, __LINE__)
#define INFOF() fprintf(ca_cfg->logfile,"INFO: F [%s]\t Line:%d\n", __func__, __LINE__)
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
#define CACHEDIR (".cache")
#define MASTERDIR (".master")
#define CACHEMASTER ("cache.master")

struct cache_config ca_cfg_s;
struct cache_config *ca_cfg;

DLinkedNode *head; // pseudo head
DLinkedNode *tail; // pseudo head
size_t total_size;
int cache_cnt;

int get, put;
size_t get_size, put_size;

//std::unordered_map<std::string, DLinkedNode *> cachemap;

//struct cloudfs_state {
//    char ssd_path[MAX_PATH_LEN];
//    char fuse_path[MAX_PATH_LEN];
//    char hostname[MAX_HOSTNAME_LEN];
//    int ssd_size;
//    int threshold;
//    int avg_seg_size;
//    int min_seg_size;
//    int max_seg_size;
//    int cache_size;
//    int rabin_window_size;
//    char no_dedup;
//};

FILE *infile_c;
FILE *outfile_c;


int get_buffer_c(const char *buffer, int bufferLength) {

//    PF("[%s]get buffer %d\n",__func__, bufferLength);
    return fwrite(buffer, 1, bufferLength, outfile_c);
}


int put_buffer_c(char *buffer, int bufferLength) {
//    PF("[%s]put buffer %d\n",__func__, bufferLength);
    return fread(buffer, 1, bufferLength, infile_c);
}


void get_cache_path(char *cache_path, const char *md5, int bufsize) {

    snprintf(cache_path, bufsize, "%s%s/%s.cache", ca_cfg->fstate->ssd_path, CACHEDIR, md5);
    PF("[%s]: md5: %s\t cache_path:%s\n", __func__, md5, cache_path);
}

void get_cachemaster_path(char *cachemaster_path, int bufsize) {

    snprintf(cachemaster_path, bufsize, "%s%s/%s", ca_cfg->fstate->ssd_path, MASTERDIR, CACHEMASTER);
//    PF("[%s]:cachemaster_path:%s\n", __func__, cachemaster_path);
}

std::string cachemaster_path_() {
    std::string ret;
    ret.assign(ca_cfg->fstate->ssd_path).append(MASTERDIR).append("/").append(CACHEMASTER);
    PF("[%s]RET is %s\n",__func__,ret.c_str());
//    snprintf(cachemaster_path, bufsize, "%s%s/%s", ca_cfg->fstate->ssd_path, MASTERDIR, CACHEMASTER);
//    PF("[%s]:cachemaster_path:%s\n", __func__, cachemaster_path);
    return ret;
}

void cache_download(std::string key) {
    cache_download_c(key.c_str());
}

void cache_download(const char *key) {
    cache_download_c(key);
}

void cache_download_c(const char *key) {
    char path_cache[MAX_PATH_LEN];
    get_cache_path(path_cache, key, MAX_PATH_LEN);

    outfile_c = FFOPEN__(path_cache, "wb");
    cloud_get_object(BUCKET, key, get_buffer_c);
    get++;
    cloud_print_error();
    PF("[%s]:\t get %s(FD:%d) from cloud with key:[%s]\n", __func__, path_cache, outfile_c, key);
    PF("[%s]:\t return\n", __func__);
    FFCLOSE__(outfile_c);
}

void cache_upload(std::string key, long size) {
    cache_upload_c(key.c_str(), size);
}




void cache_upload(const char *key, long size) {
    cache_upload_c(key, size);
}

void cache_upload(DLinkedNode *node) {
    cache_upload_c(node->key.c_str(), node->size);
}

void cache_upload_c(const char *key, long size) {
    char path_cache[MAX_PATH_LEN];
    get_cache_path(path_cache, key, MAX_PATH_LEN);

    if (!file_exist(path_cache)) {
        PF("[%s]:\t path_cache %s not exist!\n", __func__, path_cache);
    }
    infile_c = FFOPEN__(path_cache, "rb");
    cloud_put_object(BUCKET, key, size, put_buffer_c);
    PF("put[%s]\n", key);
    put++;
    put_size += size;

    PF("get[%d]put[%d]getsize[%zu]putsize[%zu]\n", get, put, get_size, put_size);
    cloud_print_error();

    PF("[%s]:\t put %s(fp:%p) into cloud with key:[%s]\n", __func__, path_cache, infile_c, key);
    PF("[%s]:\t return\n", __func__);
    FFCLOSE__(infile_c);
}


void mycache_init(FILE *logfile, struct cloudfs_state *fstate) {

    ca_cfg = &ca_cfg_s;
    ca_cfg->logfile = logfile;
    ca_cfg->fstate = fstate;
    get = 0;
    put = 0;
    get_size = 0;
    put_size = 0;

//    head = new DLinkedNode();
//    tail = new DLinkedNode();
//    head->next = tail;
//    tail->prev = head;
//    total_size = 0;

    mycache_rebuild();
//    cachemap = new std::unordered_map<std::string, DLinkedNode *>();
    std::string a = "";
//    cachemap.insert(std::pair<std::string, DLinkedNode *>(a, head));
    PF("[%s]:\n", __func__);
    mycache_store();
}

void mycache_destroy() {

    PF("[%s] \n", __func__);

}




int cloud_put_cache(char *key_c, size_t size) {

    //FILE *infile;

    std::string key(key_c);

    PF("[%s] key %s, size %zu\n", __func__, key.c_str(), size);

    char path_cache[MAX_PATH_LEN];
    get_cache_path(path_cache, key.c_str(), MAX_PATH_LEN);
    if (ca_cfg->fstate->cache_size == 0) {
        cloud_put_object(BUCKET, key.c_str(), size, put_buffer);
        return 1;
        PF("[%s] cache not enabled, saved %zu into cache\n", __func__, size);
    } else {

        DLinkedNode *n = cache_get(key);
        if (n == nullptr) {
            //not in cache
            cache_put(key, size, 1);

            FILE *tfp = FFOPEN__(path_cache, "wb");


            char *buf;
            buf = (char *) malloc(sizeof(char) * size);

            fread(buf, 1, size, infile);

            fwrite(buf, 1, size, tfp);

            free(buf);
            FFCLOSE__(tfp);

            PF("[%s], %s not in cache, saved %zu into cache\n", __func__, key.c_str(), size);



        } else {
            PF("[%s] key already in cache\n", __func__);
            //This should not happen
        }
        PF("[%s], returned\n", __func__, key.c_str(), size);


        mycache_store();
        return 1;
    }

}


int cloud_get_cache(char *key_c, size_t size) {


//    FILE *outfile;
    std::string key(key_c);

    PF("[%s] key %s, size %zu\n", __func__, key.c_str(), size);

    char path_cache[MAX_PATH_LEN];
    get_cache_path(path_cache, key.c_str(), MAX_PATH_LEN);

    if (ca_cfg->fstate->cache_size == 0) {
        cloud_get_object(BUCKET, key.c_str(), get_buffer);


        return 1;
    } else {

        DLinkedNode *n = cache_get(key);
        if (n == nullptr) {//not in cache
            //download from cloud

//            struct stat statbuf;
//            lstat(path_cache, &statbuf);
            get_size += size;
            PF("get[%s]\n", key.c_str());
            PF("get[%d]put[%d]getsize[%zu]putsize[%zu]\n", get, put, get_size, put_size);
            cache_put(key, size, 0);

            cache_download(key);
        }
//        else{
//        }



        FILE *fp = FFOPEN__(path_cache, "rb");
        char *buf;
        buf = (char *) malloc(sizeof(char) * size);
        fread(buf, 1, size, fp);
        fwrite(buf, 1, size, outfile);
        free(buf);
        FFCLOSE__(fp);


        mycache_store();
        return 1;
    }

}

void cloud_delete_cache(const char *key_c) {
    //    FILE *outfile;
    std::string key(key_c);
    PF("[%s] key %s\n", __func__, key.c_str());

    if (ca_cfg->fstate->cache_size == 0) {
        cloud_delete_object(BUCKET, key.c_str());
    } else {
        DLinkedNode *n = cache_find(key);
        if (n == nullptr) {//not in cache
            cloud_delete_object(BUCKET, key.c_str());
            PF("[%s] not in cache\n", __func__);
        } else {
            if (n->dirty == 1) {
                //on cache, not on cloud yet
                n->dirty = 0;

            }else{

                cloud_delete_object(BUCKET, key.c_str());
            }
            cut_node(n);
            cache_evict(n,false);
        }


    }

    mycache_store();
}

DLinkedNode *cache_find(std::string key) {
    DLinkedNode *n;
    for (n = head->next; n != tail; n = n->next) {
        if (n->key == key) {
            return n;
        }
    }

    mycache_store();
    return nullptr;
}

DLinkedNode *cache_get(std::string key) {

    PF("[%s] key %s\n", __func__, key.c_str());
    DLinkedNode *n = cache_find(key);
    if (n == nullptr) {
        mycache_store();
        return nullptr;
    } else {
        DLinkedNode *node = n;
        move_to_head(node);
        return node;
    }
}

DLinkedNode *cache_get(char *key_c) {
    std::string key(key_c);
    PF("[%s] key %s\n", __func__, key.c_str());

    DLinkedNode *n = cache_find(key);
    if (n == nullptr) {
        mycache_store();
        return nullptr;
    } else {
        DLinkedNode *node = n;
        move_to_head(node);

        return node;
    }
}


void cache_evict(DLinkedNode *removed, bool upload) {
    PF("[%s] key %s, size %zu, upload: %d\n", __func__, removed->key.c_str(), removed->size, upload);
    cache_cnt--;
    total_size -= removed->size;
//    cachemap.erase(removed->key);
    if (removed->dirty == 1 && upload) {
        cache_upload(removed);
    }
    char path_cache[MAX_PATH_LEN];
    get_cache_path(path_cache, removed->key.c_str(), MAX_PATH_LEN);

    remove(path_cache);
    delete removed;

    mycache_store();
}


void cache_put(std::string key, size_t size, int dirty) {

    PF("[%s] key %s, size %zu, dirty %d\n", __func__, key.c_str(), size, dirty);

//    PF("[%s] cachemap.count(key) = %llu\n", __func__, cachemap.count(key));
    DLinkedNode *n = cache_find(key);
    if (n == nullptr) {
        PF("[%s] not in cachemap\n", __func__);
        DLinkedNode *node = new DLinkedNode(key, size, dirty);


//        cachemap[key] = node;



        add_to_head(node);

        cache_cnt++;

        total_size += size;


        if (total_size > ca_cfg->fstate->cache_size) {
            PF("[%s] total_size %zu > cache_size: %d\n", __func__, total_size, ca_cfg->fstate->cache_size);
            while (total_size > ca_cfg->fstate->cache_size) {
                DLinkedNode *removed = remove_tail();
                cache_evict(removed, true);
//                cache_cnt--;
//                total_size -= removed->size;
//                cachemap.erase(removed->key);
//                delete removed;
            }
        }
        PF("[%s] put %s into cache\n", __func__, key.c_str());
        mycache_store();
    } else {
        PF("[%s] in cachemap\n");
        DLinkedNode *node = n;
        node->size = size;
        node->dirty = dirty;
        move_to_head(node);
    }


}

void add_to_head(DLinkedNode *node) {


    PF("[%s] add %s to head\n", __func__, node->key.c_str());
    node->next = head->next;

    node->prev = head;

    head->next->prev = node;

    head->next = node;

    mycache_store();

}

void cut_node(DLinkedNode *node) {
    node->prev->next = node->next;
    node->next->prev = node->prev;

}

void move_to_head(DLinkedNode *node) {
    cut_node(node);
    add_to_head(node);
    mycache_store();
}

DLinkedNode *remove_tail() {
    auto node = tail->prev;
    cut_node(node);

    return node;
}



//void mycache_rebuild() {
//
//    total_size = 0;
//
//    head = new DLinkedNode();
//    tail = new DLinkedNode();
//    head->next = tail;
//    tail->prev = head;
//    std::string cachemaster_path = cachemaster_path_();
////    char cachemaster_path[MAX_PATH_LEN];
////    get_cachemaster_path(cachemaster_path, MAX_PATH_LEN);
//    PF("[%s]\n",__func__);
//
//
//    if (file_exist(cachemaster_path.c_str())) {
//        struct stat statbuf;
//        lstat(cachemaster_path.c_str(), &statbuf);
//        if(statbuf.st_size !=0){
//            std::ifstream master(cachemaster_path.c_str());
//            std::string key;
//            size_t size;
//            int dirty;
//            while (master >> key >> size >> dirty) {
//                total_size += size;
//                DLinkedNode *node = new DLinkedNode(key, size, dirty);
//                node->next = head->next;
//                node->prev = head;
//                head->next->prev = node;
//                head->next = node;
//            }
//            master.close();
//        }else{
//            PF("[%s]file is empty\n",__func__);
//        }
//
//    }else{
//        PF("[%s]file not exist\n",__func__);
//    }
//
//}
//
//void mycache_store() {
//    DLinkedNode *n;
////    char cachemaster_path[MAX_PATH_LEN];
//    std::string cachemaster_path = cachemaster_path_();
////    get_cachemaster_path(cachemaster_path, MAX_PATH_LEN);
//    PF("[%s]%d\n",__func__, __LINE__);
//    if(!file_exist(cachemaster_path.c_str())){
//        PF("[%s]%d not exist\n",__func__, __LINE__);
//        return;
//    }
//    std::ofstream master(cachemaster_path.c_str());
//    for (n = tail->prev; n != head; n = n->prev) {
//        master << n->key << " " << n->size << " " << n->dirty << "\n";
//    }
//    master.close();
//}


void mycache_rebuild() {

    total_size = 0;

    head = new DLinkedNode();
    tail = new DLinkedNode();
    head->next = tail;
    tail->prev = head;
    std::string cachemaster_path = cachemaster_path_();


    if (file_exist(cachemaster_path.c_str())) {
        std::ifstream master(cachemaster_path.c_str());
        std::string key;
        size_t size;
        int dirty;
        while (master >> key >> size >> dirty) {
            total_size += size;
            DLinkedNode *node = new DLinkedNode(key, size, dirty);
            node->next = head->next;
            node->prev = head;
            head->next->prev = node;
            head->next = node;
        }

        master.close();
    }
}

void mycache_store() {
    DLinkedNode *n;
    std::string cachemaster_path = cachemaster_path_();
    std::ofstream master(cachemaster_path.c_str());
    for (n = tail->prev; n != head; n = n->prev) {
        master << n->key << " " << n->size << " " << n->dirty << "\n";
    }
    master.close();
}
