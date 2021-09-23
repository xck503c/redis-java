package com.xck.redisjava.command;

import com.xck.redisjava.MemoryDB;
import com.xck.redisjava.base.Sds;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * 服务器类的命令
 *
 * @author xuchengkun
 * @date 2021/09/23 15:55
 **/
public class ServerCommand {

    public static ByteBuffer flushdbCommand(List<Sds> args) {
        if (args.size() != 1) {
            return RespMsg.returnWrongNumberMsg("flushdb");
        }

        if (!MemoryDB.dbMap.isEmpty()) {
            MemoryDB.dbMap.clear();
        }
        return RespMsg.returnSimpleStr("ok");
    }
}
