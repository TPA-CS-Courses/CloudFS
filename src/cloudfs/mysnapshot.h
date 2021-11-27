//
// Created by Wilson_Xu on 2021/11/24.
//

#ifndef SRC_MYSNAPSHOT_H
#define SRC_MYSNAPSHOT_H


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

struct snap_config {

    struct cloudfs_state *fstate;
    FILE *logfile;
};

class SnapshotMaster{
public:
    struct snap_config sn_cfg_s;
    struct snap_config *sn_cfg;
    std::vector<unsigned long> snapshot_records;
    SnapshotMaster(const char *s);
    ~SnapshotMaster();

};

#endif //SRC_MYSNAPSHOT_H
