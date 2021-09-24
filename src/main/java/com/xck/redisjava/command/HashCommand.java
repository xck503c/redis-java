package com.xck.redisjava.command;

import com.xck.redisjava.MemoryDB;
import com.xck.redisjava.base.Sds;
import com.xck.redisjava.util.StrPattern;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
            return RespMsg.returnNilArray(args.size());
        }

        List<Object> respList = new ArrayList<>();
        for (int i = 2; i < args.size(); i++) {
            Sds value = field.get(args.get(i));
            if (value == null) {
                respList.add(null);
            }else {
                respList.add(value);
            }
        }

        return RespMsg.returnArray(respList, true);
    }

    /**
     * HSCAN key cursor [MATCH pattern] [COUNT count]
     *
     * 全部扫描
     * @param args
     * @return
     */
    public static ByteBuffer hscanCommand(List<Sds> args) {
        if (args.size() != 7) {
            return RespMsg.returnWrongNumberMsg("hscan");
        }

        String key = new String(args.get(1).getBuf(), Charset.forName("UTF-8"));
        Object fieldO = MemoryDB.dbMap.get(key);
        if (fieldO != null && !(fieldO instanceof HashMap)) {
            return RespMsg.returnErrOperatorWrongType();
        }

        Sds cursorSds = args.get(2);
        try {
            Integer cursor = Integer.parseInt(new String(cursorSds.getBuf(), Charset.forName("UTF-8")));
        } catch (NumberFormatException e) {
            return RespMsg.returnErrMsg("invalid cursor");
        }

        Sds countSds = args.get(6);
        try {
            Integer count = Integer.parseInt(new String(countSds.getBuf(), Charset.forName("UTF-8")));
        } catch (NumberFormatException e) {
            return RespMsg.returnErrMsg("value is not integer or out of range");
        }

        String pattern = new String(args.get(4).getBuf(), Charset.forName("UTF-8"));
        StrPattern strPattern = StrPattern.build(pattern);

        List<Object> list = new ArrayList<>();
        list.add(new Sds("0".getBytes(Charset.forName("UTF-8"))));
        //第二段参数用
        List<Object> respArgs = new ArrayList<>();
        list.add(respArgs);

        //无数据返回空数组
        HashMap<Sds, Sds> field = (HashMap<Sds, Sds>) fieldO;
        if (field == null || field.isEmpty()) {
            return RespMsg.returnArray(list, false);
        }

        for (Map.Entry<Sds, Sds> entry: field.entrySet()) {
            String tmpKey = new String(entry.getKey().getBuf(), Charset.forName("UTF-8"));
            if (strPattern.isMatch(tmpKey)) {
                respArgs.add(entry.getKey());
                respArgs.add(entry.getValue());
            }
        }

        return RespMsg.returnArray(list, false);
    }

    /**
     * hgetall hashkey
     * @return
     */
    public static ByteBuffer hgetallCommand(List<Sds> args){
        if (args.size() != 2) {
            return RespMsg.returnWrongNumberMsg("hgetall");
        }

        String key = new String(args.get(1).getBuf(), Charset.forName("UTF-8"));
        Object fieldO = MemoryDB.dbMap.get(key);
        if (fieldO != null && !(fieldO instanceof HashMap)) {
            return RespMsg.returnErrOperatorWrongType();
        }

        List<Object> list = new ArrayList<>();

        //无数据返回空数组
        HashMap<Sds, Sds> field = (HashMap<Sds, Sds>) fieldO;
        if (field == null || field.isEmpty()) {
            return RespMsg.returnArray(list, false);
        }

        for (Map.Entry<Sds, Sds> entry: field.entrySet()) {
            list.add(entry.getKey());
            list.add(entry.getValue());
        }

        return RespMsg.returnArray(list, false);
    }
}
