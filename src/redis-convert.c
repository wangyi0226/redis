#include "server.h"
#include "bio.h"
#include "rio.h"

#include <stdarg.h>
#include <sys/time.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>

void createSharedObjects(void);
void initServerDB(void);
struct client *createAOFClient(void);
void freeFakeClientArgv(struct client *c);
void freeFakeClient(struct client *c);
void aofUpdateCurrentSize(void);
int rdbExport(rio *rdb, int *error, int rdbflags, rdbSaveInfo *rsi);
int rdbSaveInfoAuxFields(rio *rdb, int rdbflags, rdbSaveInfo *rsi);
ssize_t rdbSaveAuxField(rio *rdb, void *key, size_t keylen, void *val, size_t vallen);

static ssize_t rdbWriteRaw(rio *rdb, void *p, size_t len) {
    if (rdb && rioWrite(rdb,p,len) == 0)
        return -1;
    return len;
}

void init(void) {
    //init config
    server.rdb_checksum=1;
    server.skip_checksum_validation=0;
    server.key_load_delay=0;
    server.dbnum = 16;
    server.loading_rdb_used_mem=0;
    server.loading_process_events_interval_bytes = 0;
    server.rdb_save_incremental_fsync=0;
    server.aof_load_truncated=0;

    createSharedObjects();

    //init data
    server.db = zmalloc(sizeof(redisDb) * server.dbnum);
    initServerDB();
}

int loadAof(char *filename) {
    struct client *fakeClient;
    FILE *fp = fopen(filename,"r");
    long loops = 0;
    off_t valid_up_to = 0; /* Offset of latest well-formed command loaded. */
    off_t valid_before_multi = 0; /* Offset before MULTI command loaded. */

    if (fp == NULL) {
        printf("Fatal error: can't open the append log file for reading: %s\n",strerror(errno));
        exit(1);
    }

    fakeClient = createAOFClient();
    startLoadingFile(fp, filename, RDBFLAGS_AOF_PREAMBLE);

    /* Check if this AOF file has an RDB preamble. In that case we need to
     * load the RDB file and later continue loading the AOF tail. */
    char sig[5]; /* "REDIS" */
    if (fread(sig,1,5,fp) != 5 || memcmp(sig,"REDIS",5) != 0) {
        /* No RDB preamble, seek back at 0 offset. */
        if (fseek(fp,0,SEEK_SET) == -1) goto readerr;
    } else {
        /* RDB preamble. Pass loading the RDB functions. */
        rio rdb;

        printf("Reading RDB preamble from AOF file...\n");
        if (fseek(fp,0,SEEK_SET) == -1) goto readerr;
        rioInitWithFile(&rdb,fp);
        if (rdbLoadRio(&rdb,RDBFLAGS_AOF_PREAMBLE,NULL) != C_OK) {
            printf("Error reading the RDB preamble of the AOF file, AOF loading aborted\n");
            goto readerr;
        } else {
            printf("Reading the remaining AOF tail...\n");
        }
    }

    /* Read the actual AOF file, in REPL format, command by command. */
    while(1) {
        int argc, j;
        unsigned long len;
        robj **argv;
        char buf[128];
        sds argsds;
        struct redisCommand *cmd;

        if (fgets(buf,sizeof(buf),fp) == NULL) {
            if (feof(fp))
                break;
            else
                goto readerr;
        }
        if (buf[0] != '*') goto fmterr;
        if (buf[1] == '\0') goto readerr;
        argc = atoi(buf+1);
        if (argc < 1) goto fmterr;

        /* Load the next command in the AOF as our fake client
         * argv. */
        argv = zmalloc(sizeof(robj*)*argc);
        fakeClient->argc = argc;
        fakeClient->argv = argv;

        for (j = 0; j < argc; j++) {
            /* Parse the argument len. */
            char *readres = fgets(buf,sizeof(buf),fp);
            if (readres == NULL || buf[0] != '$') {
                fakeClient->argc = j; /* Free up to j-1. */
                freeFakeClientArgv(fakeClient);
                if (readres == NULL)
                    goto readerr;
                else
                    goto fmterr;
            }
            len = strtol(buf+1,NULL,10);

            /* Read it into a string object. */
            argsds = sdsnewlen(SDS_NOINIT,len);
            if (len && fread(argsds,len,1,fp) == 0) {
                sdsfree(argsds);
                fakeClient->argc = j; /* Free up to j-1. */
                freeFakeClientArgv(fakeClient);
                goto readerr;
            }
            argv[j] = createObject(OBJ_STRING,argsds);

            /* Discard CRLF. */
            if (fread(buf,2,1,fp) == 0) {
                fakeClient->argc = j+1; /* Free up to j. */
                freeFakeClientArgv(fakeClient);
                goto readerr;
            }
        }

        /* Command lookup */
        cmd = lookupCommand(argv[0]->ptr);
        if (!cmd) {
            printf("Unknown command '%s' reading the append only file\n",
                      (char*)argv[0]->ptr);
            exit(1);
        }

        if (cmd == server.multiCommand) valid_before_multi = valid_up_to;

        /* Run the command in the context of a fake client */
        fakeClient->cmd = fakeClient->lastcmd = cmd;
        if (fakeClient->flags & CLIENT_MULTI &&
            fakeClient->cmd->proc != execCommand)
        {
            queueMultiCommand(fakeClient);
        } else {
            cmd->proc(fakeClient);
        }

        /* The fake client should not have a reply */
        serverAssert(fakeClient->bufpos == 0 &&
                     listLength(fakeClient->reply) == 0);

        /* The fake client should never get blocked */
        serverAssert((fakeClient->flags & CLIENT_BLOCKED) == 0);

        /* Clean up. Command code may have changed argv/argc so we use the
         * argv/argc of the client instead of the local variables. */
        freeFakeClientArgv(fakeClient);
        fakeClient->cmd = NULL;
        if (server.aof_load_truncated) valid_up_to = ftello(fp);
        if (server.key_load_delay)
            debugDelay(server.key_load_delay);
    }

    /* This point can only be reached when EOF is reached without errors.
     * If the client is in the middle of a MULTI/EXEC, handle it as it was
     * a short read, even if technically the protocol is correct: we want
     * to remove the unprocessed tail and continue. */
    if (fakeClient->flags & CLIENT_MULTI) {
        printf("Revert incomplete MULTI/EXEC transaction in AOF file\n");
        valid_up_to = valid_before_multi;
        goto uxeof;
    }

loaded_ok: /* DB loaded, cleanup and return C_OK to the caller. */
    fclose(fp);
    freeFakeClient(fakeClient);
    stopLoading(1);
    return C_OK;

readerr: /* Read error. If feof(fp) is true, fall through to unexpected EOF. */
    if (!feof(fp)) {
        if (fakeClient) freeFakeClient(fakeClient); /* avoid valgrind warning */
        fclose(fp);
        printf("Unrecoverable error reading the append only file: %s\n", strerror(errno));
        exit(1);
    }

uxeof: /* Unexpected AOF end of file. */
    if (server.aof_load_truncated) {
        printf("!!! Warning: short read while loading the AOF file !!!\n");
        printf("!!! Truncating the AOF at offset %llu !!!\n",
                  (unsigned long long) valid_up_to);
        if (valid_up_to == -1 || truncate(filename,valid_up_to) == -1) {
            if (valid_up_to == -1) {
                printf("Last valid command offset is invalid\n");
            } else {
                printf("Error truncating the AOF file: %s\n",
                          strerror(errno));
            }
        } else {
            /* Make sure the AOF file descriptor points to the end of the
             * file after the truncate call. */
            if (server.aof_fd != -1 && lseek(server.aof_fd,0,SEEK_END) == -1) {
                printf("Can't seek the end of the AOF file: %s\n",
                          strerror(errno));
            } else {
                printf("AOF loaded anyway because aof-load-truncated is enabled\n");
                goto loaded_ok;
            }
        }
    }
    if (fakeClient) freeFakeClient(fakeClient); /* avoid valgrind warning */
    fclose(fp);
    printf("Unexpected end of file reading the append only file. You can: 1) Make a backup of your AOF file, then use ./redis-check-aof --fix <filename>. 2) Alternatively you can set the 'aof-load-truncated' configuration option to yes and restart the server\n");
    exit(1);

fmterr: /* Format error. */
    if (fakeClient) freeFakeClient(fakeClient); /* avoid valgrind warning */
    fclose(fp);
    printf("Bad file format reading the append only file: make a backup of your AOF file, then use ./redis-check-aof --fix <filename>\n");
    exit(1);
}
int mergeRdbSave(rio *rdb, int *error, int rdbflags, rdbSaveInfo *rsi) {
    dictIterator *di = NULL;
    dictEntry *de;
    char magic[10];
    uint64_t cksum;
    size_t processed = 0;
    int j;

    if (server.rdb_checksum)
        rdb->update_cksum = rioGenericUpdateChecksum;
    snprintf(magic,sizeof(magic),"REDIS%04d",RDB_VERSION);
    if (rdbWriteRaw(rdb,magic,9) == -1) goto werr;
    if (rdbSaveInfoAuxFields(rdb,rdbflags,rsi) == -1) goto werr;
    if (rdbSaveModulesAux(rdb, REDISMODULE_AUX_BEFORE_RDB) == -1) goto werr;

    for (j = 0; j < server.dbnum; j++) {
        redisDb *db = server.db+j;
        dict *d = db->dict;
        if (dictSize(d) == 0) continue;
        di = dictGetSafeIterator(d);

        /* Write the SELECT DB opcode */
        if (rdbSaveType(rdb,RDB_OPCODE_SELECTDB) == -1) goto werr;
        if (rdbSaveLen(rdb,j) == -1) goto werr;

        /* Write the RESIZE DB opcode. */
        uint64_t db_size, expires_size;
        db_size = dictSize(db->dict);
        expires_size = dictSize(db->expires);
        if (rdbSaveType(rdb,RDB_OPCODE_RESIZEDB) == -1) goto werr;
        if (rdbSaveLen(rdb,db_size) == -1) goto werr;
        if (rdbSaveLen(rdb,expires_size) == -1) goto werr;

        /* Iterate this DB writing every entry */
        while((de = dictNext(di)) != NULL) {
            sds keystr = dictGetKey(de);
            robj key, *o = dictGetVal(de);
            long long expire;

            initStaticStringObject(key,keystr);
            expire = getExpire(db,&key);
            if (rdbSaveKeyValuePair(rdb,&key,o,expire) == -1) goto werr;

            /* When this RDB is produced as part of an AOF rewrite, move
             * accumulated diff from parent to child while rewriting in
             * order to have a smaller final write. */
            if (rdbflags & RDBFLAGS_AOF_PREAMBLE &&
                rdb->processed_bytes > processed+AOF_READ_DIFF_INTERVAL_BYTES)
            {
                processed = rdb->processed_bytes;
                aofReadDiffFromParent();
            }

        }
        dictReleaseIterator(di);
        di = NULL; /* So that we don't release it again on error. */
    }

    /* If we are storing the replication information on disk, persist
     * the script cache as well: on successful PSYNC after a restart, we need
     * to be able to process any EVALSHA inside the replication backlog the
     * master will send us. */
    if (rsi && dictSize(server.lua_scripts)) {
        di = dictGetIterator(server.lua_scripts);
        while((de = dictNext(di)) != NULL) {
            robj *body = dictGetVal(de);
            if (rdbSaveAuxField(rdb,"lua",3,body->ptr,sdslen(body->ptr)) == -1)
                goto werr;
        }
        dictReleaseIterator(di);
        di = NULL; /* So that we don't release it again on error. */
    }

    if (rdbSaveModulesAux(rdb, REDISMODULE_AUX_AFTER_RDB) == -1) goto werr;

    /* EOF opcode */
    if (rdbSaveType(rdb,RDB_OPCODE_EOF) == -1) goto werr;

    /* CRC64 checksum. It will be zero if checksum computation is disabled, the
     * loading code skips the check in this case. */
    cksum = rdb->cksum;
    memrev64ifbe(&cksum);
    if (rioWrite(rdb,&cksum,8) == 0) goto werr;
    return C_OK;

    werr:
    if (error) *error = errno;
    if (di) dictReleaseIterator(di);
    return C_ERR;
}

int exportRdbFile(char *filename) {
    printf("save rdb file: %s\n",filename);
    FILE *fp = NULL;
    rio rdb;
    int error = 0;
    fp = fopen(filename,"w");
    if (!fp) {
        printf("Failed opening the RDB file %s : %s ",filename,strerror(errno));
        exit(1);
    }
    rioInitWithFile(&rdb,fp);

    if (rdbExport(&rdb,&error,RDBFLAGS_NONE,NULL) == C_ERR) {
        errno = error;
        goto werr;
    }

    if (fflush(fp)) goto werr;
    if (fsync(fileno(fp))) goto werr;
    if (fclose(fp)) { fp = NULL; goto werr; }
    fp = NULL;
    printf("DB saved on disk\n");
    return 0;
werr:
    if (fp) fclose(fp);
    printf("Write error saving DB on disk: %s", strerror(errno));
    exit(1);
}

int redis_convert_main(int argc, char **argv) {
    if (argc != 3) {
        printf("Usage: %s <file.aof> <file.dump> \n", argv[0]);
        //exit(1);
    }
    init();
    printf("convert: %s to %s \n", argv[1],argv[2]);
    loadAof(argv[1]);
    exportRdbFile(argv[2]);
    exit(0);
}
