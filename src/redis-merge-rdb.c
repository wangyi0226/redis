
#include "mt19937-64.h"
#include "server.h"
#include "rdb.h"

#include <stdarg.h>
#include <sys/time.h>
#include <unistd.h>
#include <lua.h>
#include <lauxlib.h>
#include <lualib.h>

#define check_lua_type(L,index,tp) if(lua_type(L,index) != tp){luaError(lua, "wrong type of arguments %d",index);}

char *type_string[] = {
        "string",
        "list-linked",
        "set-hashtable",
        "zset-v1",
        "hash-hashtable",
        "zset-v2",
        "module-value",
        "","",
        "hash-zipmap",
        "list-ziplist",
        "set-intset",
        "zset-ziplist",
        "hash-ziplist",
        "quicklist",
        "stream"
};
void createSharedObjects(void);
void initServerDB(void);
int rdbSaveInfoAuxFields(rio *rdb, int rdbflags, rdbSaveInfo *rsi);
ssize_t rdbSaveAuxField(rio *rdb, void *key, size_t keylen, void *val, size_t vallen);
void rdbLoadProgressCallback(rio *r, const void *buf, size_t len);

void initMergeServer(void) {
    //init config
    server.rdb_checksum=1;
    server.skip_checksum_validation=0;
    server.key_load_delay=0;
    server.dbnum = 16;
    server.loading_rdb_used_mem=0;
    server.loading_process_events_interval_bytes = 0;
    server.rdb_save_incremental_fsync=0;

    createSharedObjects();

    //init data
    server.db = zmalloc(sizeof(redisDb) * server.dbnum);
    initServerDB();
}

void luaError(lua_State *lua,char *fmt,...){
    va_list ap;
    char msg[LOG_MAX_LEN];

    va_start(ap, fmt);
    vsnprintf(msg, sizeof(msg), fmt, ap);
    va_end(ap);

    lua_pushstring(lua, msg);
    lua_error(lua);
}

static ssize_t rdbWriteRaw(rio *rdb, void *p, size_t len) {
    if (rdb && rioWrite(rdb,p,len) == 0)
        return -1;
    return len;
}

int rdbExport(rio *rdb, int *error, int rdbflags, rdbSaveInfo *rsi) {
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

robj *luaGetStringObj(lua_State *lua,int index){
    check_lua_type(lua,index,LUA_TSTRING)
    size_t len;
    char *s;
    s = (char*)lua_tolstring(lua,index,&len);
    if(s==NULL || len ==0 ){
        luaError(lua, "wrong value of arguments %d,strlen:%d",index,len);
    }
    return createStringObject(s,len);
}
sds luaGetSds(lua_State *lua,int index){
    check_lua_type(lua,index,LUA_TSTRING)
    size_t len;
    char *s;
    s = (char*)lua_tolstring(lua,index,&len);
    if(s==NULL || len ==0 ){
        luaError(lua, "wrong value of arguments %d,strlen:%d",index,len);
    }
    return sdsnew(s);
}

char *luaGetString(lua_State *lua,int index){
    check_lua_type(lua,index,LUA_TSTRING)
    size_t len;
    char *s;
    s = (char*)lua_tolstring(lua,index,&len);
    if(s==NULL || len ==0 ){
        luaError(lua, "wrong value of arguments %d,strlen:%d",index,len);
    }
    return s;
}

int luaImportRdbFile(lua_State *lua) {
    char *filename= luaGetString(lua,1) ;
    printf("load rdb file: %s\n",filename);

    FILE *fp;
    rio rdb;
    int retval;
    int rdbflags=RDBFLAGS_NONE;

    if ((fp = fopen(filename,"r")) == NULL){
        luaError(lua,"Failed opening the RDB file %s : %s ",filename,strerror(errno));
        return 0;
    }

    startLoadingFile(fp, filename,rdbflags);
    rioInitWithFile(&rdb,fp);
    retval = rdbLoadRio(&rdb,rdbflags,NULL);
    fclose(fp);
    stopLoading(retval==C_OK);

    return 0;
}

robj* luaMergeCallBack(lua_State *lua,uint64_t dbid, robj *key, robj *val,robj *old,char **new_key) {
    lua_pushvalue(lua,2);
    lua_pushinteger(lua,dbid);
    lua_pushstring(lua,key->ptr);
    lua_pushlightuserdata(lua,val);
    if(old==NULL){
       lua_pushnil(lua);
    }else{
        lua_pushlightuserdata(lua,old);
    }
    lua_call(lua,4,2);
    robj *ret=lua_touserdata(lua,-2);
    if(lua_isstring(lua,-1)){
        (*new_key)=luaL_checkstring(lua,-1);
    }
    lua_pop(lua,1);
    return ret;
}

int luaMergeRdbFile(lua_State *lua) {
    if (lua_gettop(lua) != 2) {
        luaError(lua, "wrong number of arguments");
        return 0;
    }
    if (lua_type(lua,2) != LUA_TFUNCTION) {
        luaError(lua, "wrong type of arguments");
        return 0;
    }
    char *filename= luaGetString(lua,1) ;
    printf("merge rdb file: %s\n",filename);
    FILE *fp;
    rio rdb;
    uint64_t dbid;
    redisDb *db=NULL;
    int type, rdbver;
    char buf[1024];
    long long expiretime, now = mstime();

    if ((fp = fopen(filename,"r")) == NULL){
        luaError(lua,"Failed opening the RDB file %s : %s ",filename,strerror(errno));
        return 0;
    }
    startLoadingFile(fp, filename, RDBFLAGS_NONE);
    rioInitWithFile(&rdb,fp);
    rdb.update_cksum = rdbLoadProgressCallback;
    if (rioRead(&rdb,buf,9) == 0) goto eoferr;
    buf[9] = '\0';
    if (memcmp(buf,"REDIS",5) != 0) {
        fclose(fp);
        luaError(lua,"Wrong signature trying to load DB from file");
        return 0;
    }
    rdbver = atoi(buf+5);
    if (rdbver < 1 || rdbver > RDB_VERSION) {
        fclose(fp);
        luaError(lua,"Can't handle RDB format version %d",rdbver);
        return 0;
    }

    expiretime = -1;
    int keys=0,already_expired=0,expires=0;
    while(1) {
        robj *key, *val,*old;
        int key_type=-1;
        if ((type = rdbLoadType(&rdb)) == -1)goto eoferr;

        if (type == RDB_OPCODE_EXPIRETIME) {
            expiretime = rdbLoadTime(&rdb);
            expiretime *= 1000;
            if (rioGetReadError(&rdb)) goto eoferr;
            continue; /* Read next opcode. */
        } else if (type == RDB_OPCODE_EXPIRETIME_MS) {
            /* EXPIRETIME_MS: milliseconds precision expire times introduced
            * with RDB v3. Like EXPIRETIME but no with more precision. */
            expiretime = rdbLoadMillisecondTime(&rdb, rdbver);
            if (rioGetReadError(&rdb)) goto eoferr;
            continue; /* Read next opcode. */
        } else if (type == RDB_OPCODE_FREQ) {
            /* FREQ: LFU frequency. */
            uint8_t byte;
            if (rioRead(&rdb,&byte,1) == 0) goto eoferr;
            continue; /* Read next opcode. */
        } else if (type == RDB_OPCODE_IDLE) {
            /* IDLE: LRU idle time. */
            if (rdbLoadLen(&rdb,NULL) == RDB_LENERR) goto eoferr;
            continue; /* Read next opcode. */
        } else if (type == RDB_OPCODE_EOF) {
            /* EOF: End of file, exit the main loop. */
            break;
        } else if (type == RDB_OPCODE_SELECTDB) {
            /* SELECTDB: Select the specified database. */
            if ((dbid = rdbLoadLen(&rdb,NULL)) == RDB_LENERR)
                    goto eoferr;
                if(dbid>server.dbnum){
                    fclose(fp);
                    luaError(lua,"DB ID %llu > server.dbnum:%d",(unsigned long long)dbid,server.dbnum);
                    return 0;
                }
                printf("Selecting DB ID %llu\n", (unsigned long long)dbid);

                db=&server.db[dbid];
                continue; /* Read type again. */
        } else if (type == RDB_OPCODE_RESIZEDB) {
            uint64_t db_size, expires_size;
            if ((db_size = rdbLoadLen(&rdb,NULL)) == RDB_LENERR)
                goto eoferr;
            if ((expires_size = rdbLoadLen(&rdb,NULL)) == RDB_LENERR)
                goto eoferr;
            continue; /* Read type again. */
        } else if (type == RDB_OPCODE_AUX) {
            robj *auxkey, *auxval;
            if ((auxkey = rdbLoadStringObject(&rdb)) == NULL) goto eoferr;
            if ((auxval = rdbLoadStringObject(&rdb)) == NULL) goto eoferr;
            decrRefCount(auxkey);
            decrRefCount(auxval);
            continue; /* Read type again. */
        } else if (type == RDB_OPCODE_MODULE_AUX) {
            uint64_t moduleid, when_opcode, when;
            if ((moduleid = rdbLoadLen(&rdb,NULL)) == RDB_LENERR) goto eoferr;
            if ((when_opcode = rdbLoadLen(&rdb,NULL)) == RDB_LENERR) goto eoferr;
            if ((when = rdbLoadLen(&rdb,NULL)) == RDB_LENERR) goto eoferr;
            char name[10];
            moduleTypeNameByID(name,moduleid);
            robj *o = rdbLoadCheckModuleValue(&rdb,name);
            decrRefCount(o);
            continue; /* Read type again. */
        } else {
            if (!rdbIsObjectType(type)) {
                fclose(fp);
                luaError(lua,"Invalid object type: %d", type);
                return 0;
            }
            key_type = type;
        }
        /* Read key */
        if ((key = rdbLoadStringObject(&rdb)) == NULL) goto eoferr;
           keys++;
        if ((val = rdbLoadObject(type,&rdb,key->ptr,NULL)) == NULL) goto eoferr;
            /* Check if the key already expired. */
        if (expiretime != -1 && expiretime < now){
            already_expired++;
        }else{
            robj *ret=NULL;
            char *newkey=NULL;
            old=lookupKeyRead(db,key);
            ret=luaMergeCallBack(lua,dbid,key,val,old,&newkey);
            if(newkey!=NULL){
                decrRefCount(key);
                key=createStringObject(newkey,strlen(newkey));
                old= lookupKeyRead(db,key);
            }
            if(ret != NULL){
                if(old==NULL){
                    dbAdd(db,key,val);
                    if(expiretime != -1){
                        dictEntry  *de;
                        de = dictAddOrFind(db->expires,key->ptr);
                        dictSetSignedIntegerVal(de,expiretime);
                    }
                    key=NULL;
                    val=NULL;
                }else if(ret!=old){
                    dbOverwrite(db,key,val);
                    if(expiretime != -1){
                        dictEntry  *de=dictFind(db->expires,key->ptr);
                        if(de !=NULL && dictGetSignedIntegerVal(de)<expiretime){
                            dictSetSignedIntegerVal(de,expiretime);
                        }
                    }
                    val=NULL;
                }
            }
        }
        if (expiretime != -1) expires++;
        if(key!=NULL){
            decrRefCount(key);
        }
        if(val!=NULL){
            decrRefCount(val);
        }
        expiretime = -1;
    }

    /* Verify the checksum if RDB version is >= 5 */
    if (rdbver >= 5 && server.rdb_checksum) {
        uint64_t cksum, expected = rdb.cksum;
        if (rioRead(&rdb, &cksum, 8) == 0) {
            fclose(fp);
            luaError(lua,"Unexpected EOF reading RDB file");
            return 0;
        }
        memrev64ifbe(&cksum);
        if (cksum == 0) {
            goto eoferr;
        } else if (cksum != expected) {
            fclose(fp);
            luaError(lua,"RDB CRC error");
        } else {
            printf("Checksum OK\n");
        }
    }
    fclose(fp);
    printf("Done loading RDB, keys loaded: %d, keys expired: %d, keys already expired:%d \n",keys, expires,already_expired);
    return 0;

eoferr:
    if(fp)fclose(fp);
    luaError(lua,"RDB file was saved with checksum disabled: no check performed.");
    return 0;
}

int luaExportRdbFile(lua_State *lua) {
    char *filename= luaGetString(lua,1) ;
    printf("save rdb file: %s\n",filename);
    FILE *fp = NULL;
    rio rdb;
    int error = 0;
    fp = fopen(filename,"w");
    if (!fp) {
        luaError(lua,"Failed opening the RDB file %s : %s ",filename,strerror(errno));
        return 0;
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
    luaError(lua,"Write error saving DB on disk: %s", strerror(errno));
    return 0;
}

void luaRdbCheckDBAndKey(lua_State *lua,redisDb **db,robj **key) {
    int dbid = luaL_checkint(lua,1);
    if(dbid>=server.dbnum || dbid<0){
        luaError(lua,"wrong dbid:%d",dbid);
    }
    *key = luaGetStringObj(lua,2);
    (*db)=&server.db[dbid];
}

robj *luaRdbCheckDBAndKeyExist(lua_State *lua,redisDb **db,robj **key) {
    robj *val;
    int dbid = luaL_checkint(lua,1);
    if(dbid>=server.dbnum || dbid<0){
        luaError(lua,"wrong dbid:%d",dbid);
    }
    *key = luaGetStringObj(lua,2);
    *db=&server.db[dbid];
    val= lookupKeyWrite(*db,*key);
    if(val == NULL){
        freeStringObject(*key);
        (*key)=NULL;
        luaError(lua,"db[%d] not found key:%s",dbid,key);
    }
    return val;
}

int luaRdbGet(lua_State *lua) {
    robj *key,*val;
    redisDb *db;
    luaRdbCheckDBAndKey(lua,&db,&key);
    val=lookupKeyWrite(db,key);
    decrRefCount(key);
    if(val==NULL){
        return 0;
    }
    lua_pushlightuserdata(lua,val);
    return 1;
}

int luaRdbSet(lua_State *lua) {
    robj *key,*val;
    redisDb *db;
    luaRdbCheckDBAndKey(lua,&db,&key);
    decrRefCount(key);
    val=luaGetStringObj(lua,3);
    dbAdd(db,key,val);
    return 0;
}

int luaRdbHSet(lua_State *lua) {
    robj *key,*h;
    redisDb *db;
    sds field,hval;
    luaRdbCheckDBAndKey(lua,&db,&key);
    h = lookupKeyWrite(db,key);
    if (h == NULL) {
        h = createHashObject();
        dbAdd(db,key,h);
    }else{
        decrRefCount(key);
    }
    field=luaGetSds(lua,3);
    hval=luaGetSds(lua,4);

    hashTypeSet(h,field,hval,HASH_SET_COPY);
    return 0;
}

int luaRdbHDel(lua_State *lua) {
    robj *key,*h;
    redisDb *db;
    sds field;
    h=luaRdbCheckDBAndKeyExist(lua,&db,&key);
    field=luaGetSds(lua,3);

    hashTypeDelete(h,field);
    sdsfree(field);
    decrRefCount(key);
    return 0;
}

int luaRdbListPush(lua_State *lua,int where) {
    robj *key,*list,*val;
    redisDb *db;
    luaRdbCheckDBAndKey(lua,&db,&key);
    list = lookupKeyWrite(db,key);
    if (list == NULL) {
        list = createQuicklistObject();
        dbAdd(db,key,list);
    }else{
        decrRefCount(key);
    }
    val=luaGetStringObj(lua,3);
    listTypePush(list,val,where);
    return 0;
}

int luaRdbLPush(lua_State *lua) {
    luaRdbListPush(lua,LIST_HEAD);
    return 0;
}

int luaRdbRPush(lua_State *lua) {
    luaRdbListPush(lua,LIST_TAIL);
    return 0;
}

int luaRdbLTrim(lua_State *lua) {
    robj *key,*list;
    redisDb *db;
    long start, end, llen, ltrim, rtrim;
    list=luaRdbCheckDBAndKeyExist(lua,&db,&key);
    start=luaL_checklong(lua,3);
    end=luaL_checklong(lua,4);

    llen = listTypeLength(list);
    /* convert negative indexes */
    if (start < 0) start = llen+start;
    if (end < 0) end = llen+end;
    if (start < 0) start = 0;
    /* Invariant: start >= 0, so this test will be true when end < 0.
    * The range is empty when start > end or start >= length. */
    if (start > end || start >= llen) {
        /* Out of range start or start > end result in empty list */
        ltrim = llen;
        rtrim = 0;
    } else {
        if (end >= llen) end = llen-1;
        ltrim = start;
        rtrim = llen-end-1;
    }
    quicklistDelRange(list->ptr,0,ltrim);
    quicklistDelRange(list->ptr,-rtrim,rtrim);
    if (listTypeLength(list) == 0) {
        dbDelete(db,key);
    }
    decrRefCount(key);
    return 0;
}

int luaRdbSAdd(lua_State *lua) {
    robj *key,*set;
    sds val;
    redisDb *db;
    luaRdbCheckDBAndKey(lua,&db,&key);
    set = lookupKeyWrite(db,key);
    val = luaGetSds(lua,3);
    if(set == NULL){
        set=createSetObject();
        dbAdd(db,key,set);
    }
    setTypeAdd(set,val);
    return 0;
}

int luaRdbSRem(lua_State *lua) {
    robj *key,*set;
    sds val;
    redisDb *db;
    set=luaRdbCheckDBAndKeyExist(lua,&db,&key);
    val = luaGetSds(lua,3);
    setTypeRemove(set,val);
    sdsfree(val);
    return 0;
}

int luaRdbZAdd(lua_State *lua) {
    robj *key,*zset;
    sds zkey;
    double  score,new_score;
    int in_flags=ZADD_IN_NONE,out_flags;
    redisDb *db;
    luaRdbCheckDBAndKey(lua,&db,&key);
    zset = lookupKeyWrite(db,key);
    if(zset == NULL){
        zset=createZsetObject();
        dbAdd(db,key,zset);
    }
    score=(double)luaL_checknumber(lua,3);
    zkey = luaGetSds(lua,4);
    zsetAdd(zset,score, zkey,in_flags,&out_flags,&new_score);
    return 0;
}

int luaRdbZRem(lua_State *lua) {
    robj *key,*zset;
    sds val;
    redisDb *db;
    zset=luaRdbCheckDBAndKeyExist(lua,&db,&key);
    val = luaGetSds(lua,3);
    zsetDel(zset,val);
    sdsfree(val);
    return 0;
}
int luaRdbDeleteKey(lua_State *lua) {
    int dbid = luaL_checkint(lua,1);
    robj *key = luaGetStringObj(lua,2);
    if(dbid>=server.dbnum || dbid<0){
        luaError(lua,"wrong dbid:%d",dbid);
    }
    dbSyncDelete(&server.db[dbid],key);
    decrRefCount(key);
    return 0;
}

int luaRdbExpire(lua_State *lua) {
    int  dbid = luaL_checkint(lua,1);
    char *key = luaGetString(lua,2);
    uint64_t  expiretime = luaL_checkint(lua,3);
    if(dbid>=server.dbnum || dbid<0){
        luaError(lua,"wrong dbid:%d",dbid);
    }
    robj *keyobj = createStringObject(key,strlen(key));
    if(lookupKeyRead(&server.db[dbid],keyobj)!=NULL){
        luaError(lua,"set expire time error, not found key:%s in db:%d",key,dbid);
    }
    dictEntry *de = dictAddOrFind(server.db[dbid].expires,key);
    dictSetSignedIntegerVal(de,expiretime*1000);
    decrRefCount(keyobj);
    return 0;
}

int luaRobjToString(lua_State *lua) {
    robj *r=lua_touserdata(lua,1);
    if(r->type != OBJ_STRING){
        luaError(lua,"one of the redis obj type is not string: %s",type_string[r->type]);
    }
    char buf[128],*str;
    size_t len;
    if (sdsEncodedObject(r)) {
        str = r->ptr;
        len = sdslen(str);
    } else {
        len = ll2string(buf,sizeof(buf),(long) r->ptr);
        str = buf;
    }
    lua_pushlstring(lua,str,len);
    return 1;
}

int luaRobjMerge(lua_State *lua) {
    robj *r1=lua_touserdata(lua,1);
    robj *r2=lua_touserdata(lua,2);
    if (r1==NULL || r2==NULL) {
        luaError(lua, "wrong number or type of arguments");
        return 0;
    }
    if(r1->type!=r2->type){
        luaError(lua, " type not equal:%s %s\n",type_string[r1->type],type_string[r2->type]);
        return 0;
    }
    switch (r1->type) {
        case OBJ_LIST: {
            listTypeIterator *liter;
            listTypeEntry lentry;
            liter = listTypeInitIterator(r2, -1, LIST_HEAD);
            while (listTypeNext(liter, &lentry)) {
                listTypeInsert(&lentry, r1, LIST_TAIL);
            }
            listTypeReleaseIterator(liter);
            break;
        }
        case OBJ_SET: {
            setTypeIterator *ziter;
            int64_t intele;
            sds element;
            ziter = setTypeInitIterator(r2);
            while (setTypeNext(ziter, &element, &intele) != -1) {
                element = sdsfromlonglong(intele);
                setTypeAdd(r1, element);
            }
            setTypeReleaseIterator(ziter);
            break;
        }
        case OBJ_HASH: {
            hashTypeIterator *hiter;
            hiter = hashTypeInitIterator(r2);
            while (hashTypeNext(hiter) != C_ERR) {
                sds key, value;
                key = hashTypeCurrentObjectNewSds(hiter, OBJ_HASH_KEY);
                value = hashTypeCurrentObjectNewSds(hiter, OBJ_HASH_VALUE);
                if(hashTypeGetValueObject(r1,key)!=NULL){
                    hashTypeReleaseIterator(hiter);
                    luaError(lua,"hash table key conflict %s\n",key);
                    return 0;
                }
                hashTypeSet(r1,key,value,HASH_SET_COPY);
            }
            hashTypeReleaseIterator(hiter);
            break;
        }
        case OBJ_ZSET:{
            robj *o=r2;
            int in_flags=ZADD_IN_NX;
            int out_flags;
            double new_score;
            if (o->encoding == OBJ_ENCODING_ZIPLIST) {
                unsigned char *zl = o->ptr;
                unsigned char *eptr, *sptr;
                double score;
                eptr = ziplistIndex(zl,0);//key ptr
                while (eptr != NULL) {
                    sptr = ziplistNext(zl,eptr);//score ptr
                    serverAssert(sptr != NULL);
                    score = zzlGetScore(sptr);
                    zsetAdd(r1,score, ziplistGetObject(eptr),in_flags,&out_flags,&new_score);
                    eptr= ziplistNext(zl,sptr);//next key ptr
                }
            } else if (o->encoding == OBJ_ENCODING_SKIPLIST) {
                zset *zs = o->ptr;
                dictIterator *di = dictGetIterator(zs->dict);
                dictEntry *de;

                while((de = dictNext(di)) != NULL) {
                    sds sdsele = dictGetKey(de);
                    double *score = dictGetVal(de);
                    zsetAdd(r1,*score,sdsele,in_flags,&out_flags,&new_score);
                }
                dictReleaseIterator(di);
            } else {
                luaError(lua,"Unknown sorted set encoding");
            }
            break;
        }
        default:
            luaError(lua, "type not supported:%s\n",type_string[r1->type]);
            return 0;
    }
    return 0;
}

int redis_merge_rdb_main(int argc, char **argv) {
    int ret=0;
    int i=0;
    printf("redis-merge-rdb start\n");
    lua_State *lua = newLuaState();
    lua_newtable(lua);
    for (i = 1; i < argc; i++) {
        lua_pushstring(lua,argv[i]);
        lua_rawseti(lua,-2,i);
    }
    lua_setglobal(lua,"argv");

    //rdb
    lua_newtable(lua);

    //rdb.import
    lua_pushstring(lua, "import");
    lua_pushcfunction(lua, luaImportRdbFile);
    lua_settable(lua, -3);

    //rdb.merge
    lua_pushstring(lua, "merge");
    lua_pushcfunction(lua, luaMergeRdbFile);
    lua_settable(lua, -3);

    //rdb.export
    lua_pushstring(lua, "export");
    lua_pushcfunction(lua, luaExportRdbFile);
    lua_settable(lua, -3);

    //rdb.get
    lua_pushstring(lua, "get");
    lua_pushcfunction(lua, luaRdbGet);
    lua_settable(lua, -3);

    //rdb.set
    lua_pushstring(lua, "set");
    lua_pushcfunction(lua, luaRdbSet);
    lua_settable(lua, -3);

    //rdb.hset
    lua_pushstring(lua, "hset");
    lua_pushcfunction(lua, luaRdbHSet);
    lua_settable(lua, -3);

    //rdb.hdel
    lua_pushstring(lua, "hdel");
    lua_pushcfunction(lua, luaRdbHDel);
    lua_settable(lua, -3);

    //rdb.lpush
    lua_pushstring(lua, "lpush");
    lua_pushcfunction(lua, luaRdbLPush);
    lua_settable(lua, -3);

    //rdb.rpush
    lua_pushstring(lua, "rpush");
    lua_pushcfunction(lua, luaRdbRPush);
    lua_settable(lua, -3);

    //rdb.ltrim
    lua_pushstring(lua, "ltrim");
    lua_pushcfunction(lua, luaRdbLTrim);
    lua_settable(lua, -3);

    //rdb.sadd
    lua_pushstring(lua, "sadd");
    lua_pushcfunction(lua, luaRdbSAdd);
    lua_settable(lua, -3);

    //rdb.srem
    lua_pushstring(lua, "srem");
    lua_pushcfunction(lua, luaRdbSRem);
    lua_settable(lua, -3);

    //rdb.zadd
    lua_pushstring(lua, "zadd");
    lua_pushcfunction(lua, luaRdbZAdd);
    lua_settable(lua, -3);

    //rdb.zrem
    lua_pushstring(lua, "zrem");
    lua_pushcfunction(lua, luaRdbZRem);
    lua_settable(lua, -3);


    //rdb.delete
    lua_pushstring(lua, "delete");
    lua_pushcfunction(lua, luaRdbDeleteKey);
    lua_settable(lua, -3);

    //rdb.expire
    lua_pushstring(lua, "expire");
    lua_pushcfunction(lua, luaRdbExpire);
    lua_settable(lua, -3);

    lua_setglobal(lua,"rdb");

    //robj
    lua_newtable(lua);

    //robj.tostring
    lua_pushstring(lua, "tostring");
    lua_pushcfunction(lua, luaRobjToString);
    lua_settable(lua, -3);

    //robj.merge
    lua_pushstring(lua, "merge");
    lua_pushcfunction(lua, luaRobjMerge);
    lua_settable(lua, -3);
    lua_setglobal(lua,"robj");

    initMergeServer();

    ret=luaL_dofile(lua,"script/main.lua");

    if (0!=ret) {
        printf("run lua script failed:%s\n",lua_tostring(lua,-1));
        exit(-1);
        return -1;
    }
    exit(0);
}
