package com.xck.redisjava.command;

import com.xck.redisjava.MemoryDB;
import com.xck.redisjava.base.Sds;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * 哈希类相关命令
 *
 * @author xuchengkun
 * @date 2021/09/23 15:58
 **/
public class HashCommand {

    /**
     * hset key field value
     *
     * @param args
     * @return
     */
    public static ByteBuffer hsetCommand(List<Sds> args) {
        if (args.size() != 4) {
            return RespMsg.returnWrongNumberMsg("hset");
        }

        String key = new String(args.get(1).getBuf(), Charset.forName("UTF-8"));
        boolean isExists = true;

        Object fieldO = MemoryDB.dbMap.get(key);
        if (fieldO != null && !(fieldO instanceof HashMap)) {
            return RespMsg.returnErrOperatorWrongType();
        }

        HashMap<Sds, Sds> field = (HashMap<Sds, Sds>) fieldO;
        if (field == null) {
            field = new HashMap<>();
            MemoryDB.dbMap.put(key, field);
        }

        Sds oldSds = field.put(args.get(2), args.get(3));
        if (oldSds == null) {
            isExists = false;
        }

        //若原本存在则返回0
        return RespMsg.returnNumber(isExists ? 0 : 1);
    }

    /**
     * hget命令
     *
     * @param args
     * @return
     */
    public static ByteBuffer hgetCommand(List<Sds> args) {
        if (args.size() != 3) {
            return RespMsg.returnWrongNumberMsg("hget");
        }

        String key = new String(args.get(1).getBuf(), Charset.forName("UTF-8"));
        Object fieldO = MemoryDB.dbMap.get(key);
        if (fieldO != null && !(fieldO instanceof HashMap)) {
            return RespMsg.returnErrOperatorWrongType();
        }

        HashMap<Sds, Sds> field = (HashMap<Sds, Sds>) fieldO;
        if (field == null) {
            return RespMsg.returnNilBulkStr();
        }

        Sds value = field.get(args.get(2));
        if (value == null) {
            return RespMsg.returnNilBulkStr();
        }
        return RespMsg.returnBlukStr(value.getBuf());
    }

    public static ByteBuffer hlenCommand(List<Sds> args) {
        if (args.size() != 2) {
            return RespMsg.returnWrongNumberMsg("hlen");
        }

        String key = new String(args.get(1).getBuf(), Charset.forName("UTF-8"));
        Object fieldO = MemoryDB.dbMap.get(key);
        if (fieldO != null && !(fieldO instanceof HashMap)) {
            return RespMsg.returnErrOperatorWrongType();
        }

        HashMap<Sds, Sds> field = (HashMap<Sds, Sds>) fieldO;
        if (field == null) {
            return RespMsg.returnNumber(0);
        }
        return RespMsg.returnNumber(field.size());
    }

    public static ByteBuffer hmsetCommand(List<Sds> args) {
        if (args.size() <= 2 || (args.size() - 2) % 2 != 0) {
            return RespMsg.returnWrongNumberMsg("hmset");
        }

        String key = new String(args.get(1).getBuf(), Charset.forName("UTF-8"));
        Object fieldO = MemoryDB.dbMap.get(key);
        if (fieldO != null && !(fieldO instanceof HashMap)) {
            return RespMsg.returnErrOperatorWrongType();
        }

        HashMap<Sds, Sds> field = (HashMap<Sds, Sds>) fieldO;
        if (field == null) {
            field = new HashMap<>();
            MemoryDB.dbMap.put(key, field);
        }

        for (int i = 2; i < args.size(); i++) {
           field.put(args.get(i), args.get(++i));
        }

        return RespMsg.returnSimpleStr("OK");
    }

    public static ByteBuffer hmgetCommand(List<Sds> args) {
        if (args.size() < 3) {
            return RespMsg.returnWrongNumberMsg("hmget");
        }

        String key = new String(args.get(1).getBuf(), Charset.forName("UTF-8"));
        Object fieldO = MemoryDB.dbMap.get(key);
        if (fieldO != null && !(fieldO instanceof HashMap)) {
            return RespMsg.returnErrOperatorWrongType();
        }

        HashMap<Sds, Sds> field = (HashMap<Sds, Sds>) fieldO;
        if (field == null || field.isEmpty()) {
            return RespMsg.returnEmptyArray(args.size());
        }

        List<Sds> respList = new ArrayList<>();
        for (int i = 2; i < args.size(); i++) {
            Sds value = field.get(args.get(i));
            if (value == null) {
                respList.add(null);
            }else {
                respList.add(value);
            }
        }

        return RespMsg.returnArray(respList);
    }
}
