package com.xck.redisjava.command;

import com.xck.redisjava.MemoryDB;
import com.xck.redisjava.base.Sds;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.List;

/**
 * key相关的命令
 *
 * @author xuchengkun
 * @date 2021/09/23 15:53
 **/
public class KeyCommand {

    /**
     * del key1 key2：可批量可单删除
     *
     * @param args
     * @return
     */
    public static ByteBuffer delCommand(List<Sds> args) {

        if (args.size() < 2) {
            return RespMsg.returnWrongNumberMsg("del");
        }

        int delCount = 0;
        for (int i = 1; i < args.size(); i++) {
            String key = new String(args.get(i).getBuf(), Charset.forName("UTF-8"));
            if (MemoryDB.dbMap.remove(key) != null) {
                ++delCount;
            }
        }

        return RespMsg.returnNumber(delCount);
    }

    /**
     * 判断key是否存在：可以批量，可以单个判断
     *
     * @return
     */
    public static ByteBuffer existsCommand(List<Sds> args) {
        if (args.size() < 2) {
            return RespMsg.returnWrongNumberMsg("exists");
        }

        int existsCount = 0;
        for (int i = 1; i < args.size(); i++) {
            if (MemoryDB.dbMap.containsKey(args.get(i))) {
                ++existsCount;
            }
        }

        return RespMsg.returnNumber(existsCount);
    }
}
