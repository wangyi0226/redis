
#include "mt19937-64.h"
#include "server.h"
#include "rdb.h"

#include <stdarg.h>
#include <sys/time.h>
#include <unistd.h>
#include <lua.h>
#include <lauxlib.h>
#include <lualib.h>

int redis_merge_rdb_main(int argc, char **argv) {
    int ret=0;
    int i=0;
    if (argc < 2) {
        fprintf(stderr, "Usage: %s <rdb-merge-file1 rdb-merge-file2 ...>\n", argv[0]);
        exit(1);
    }
    printf("redis-merge-rdb start\n");
    lua_State *lua = newLuaState();
    lua_newtable(lua);
    for (i = 1; i < argc; i++) {
        lua_pushstring(lua,argv[i]);
        lua_rawseti(lua,-2,i);
    }
    lua_setglobal(lua,"argv");

    ret=luaL_dofile(lua,"script/main.lua");

    if (0!=ret) {
        printf("run lua script failed:%s\n",lua_tostring(lua,-1));
        exit(-1);
        return -1;
    }
    exit(0);
}
