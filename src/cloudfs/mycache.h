//
// Created by Wilson_Xu on 2021/11/24.
//

#ifndef SRC_MYCACHE_H
#define SRC_MYCACHE_H


struct cache_config {

    struct cloudfs_state *fstate;
    FILE *logfile;
};

struct DLinkedNode {
    std::string key;
    size_t size;
    int dirty;
    DLinkedNode *prev;
    DLinkedNode *next;

    DLinkedNode() : key(""), size(0), dirty(0), prev(nullptr), next(nullptr) {}

    DLinkedNode(std::string _key, size_t _size, int _dirty) : key(_key), size(_size), dirty(_dirty), prev(nullptr),
                                                              next(nullptr) {}
};


int get_buffer_c(const char *buffer, int bufferLength) ;


int put_buffer_c(char *buffer, int bufferLength) ;


void get_cache_path(char *cache_path, const char *md5, int bufsize);

void get_cachemaster_path(char *cachemaster_path, int bufsize) ;

void cache_download(std::string key) ;

void cache_download(const char *key) ;

void cache_download_c(const char *key) ;

void cache_upload(std::string key, long size);




void cache_upload(const char *key, long size) ;

void cache_upload(DLinkedNode *node) ;

void cache_upload_c(const char *key, long size) ;


void mycache_init(FILE *logfile, struct cloudfs_state *fstate) ;

void mycache_destroy() ;


int cloud_put_cache(char *key_c, size_t size) ;


int cloud_get_cache(char *key_c, size_t size);

void cloud_delete_cache(const char *key_c) ;

DLinkedNode *cache_find(std::string key) ;

DLinkedNode *cache_get(std::string key) ;

DLinkedNode *cache_get(char *key_c);


void cache_evict(DLinkedNode *removed, bool upload) ;


void cache_put(std::string key, size_t size, int dirty) ;

void add_to_head(DLinkedNode *node);
void cut_node(DLinkedNode *node) ;

void move_to_head(DLinkedNode *node) ;

DLinkedNode *remove_tail();





std::string cachemaster_path_();




void mycache_rebuild() ;

void mycache_store() ;


#endif //SRC_MYCACHE_H
