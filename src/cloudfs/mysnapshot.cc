//
// Created by Wilson_Xu on 2021/11/24.
//

#include "snapshot.h"

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
#define CACHEDIR (".cache")
#define CACHEMASTER ("cachemaster")

SnapshotMaster::SnapshotMaster(struct cloudfs_state *fstate, FILE *logfile) {
    sn_cfg->fstate = fstate;
    sn_cfg->logfile = logfile;
}

SnapshotMaster::~SnapshotMaster() {

}



