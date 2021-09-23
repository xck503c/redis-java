package com.xck.redisjava.command;

import com.xck.redisjava.MemoryDB;
import com.xck.redisjava.base.Sds;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.List;

/**
 * 字符串类的命令
 *
 * @author xuchengkun
 * @date 2021/09/23 15:51
 **/
public class StringCommand {

    /**
     * set key value
     * @param args
     * @return
     */
    public static ByteBuffer setCommand(List<Sds> args) {

        if (args.size() != 3) {
            return RespMsg.returnWrongNumberMsg("set");
        }

        String key = new String(args.get(1).getBuf(), Charset.forName("UTF-8"));
        Object value = MemoryDB.dbMap.get(key);
        if (value != null && !(value instanceof Sds)) {
            return RespMsg.returnErrOperatorWrongType();
        }

        MemoryDB.dbMap.put(key, args.get(2));
        return RespMsg.returnSimpleStr("OK");
    }

    /**
     * get key
     * @param args
     * @return
     */
    public static ByteBuffer getCommand(List<Sds> args) {

        if (args.size() != 2) {
            return RespMsg.returnWrongNumberMsg("get");
        }

        String key = new String(args.get(1).getBuf(), Charset.forName("UTF-8"));
        Sds value = (Sds)MemoryDB.dbMap.get(key);
        if (value == null) {
            return RespMsg.returnNilBulkStr();
        } else if (!(value instanceof Sds)) {
            return RespMsg.returnErrOperatorWrongType();
        }

        return RespMsg.returnBlukStr(value.getBuf());
    }
}
