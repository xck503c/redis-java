package com.xck.redisjava.command;

import com.xck.redisjava.MemoryDB;
import com.xck.redisjava.base.Sds;
import com.xck.redisjava.util.StrPattern;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
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

    public static ByteBuffer keysCommand(List<Sds> args) {
        if (args.size() != 2) {
            return RespMsg.returnWrongNumberMsg("keys");
        }

        String pattern = new String(args.get(1).getBuf(), Charset.forName("UTF-8"));
        StrPattern strPattern = StrPattern.build(pattern);

        List<Object> list = new ArrayList<>();
        for (String key : MemoryDB.dbMap.keySet()) {
            if (strPattern.isMatch(key)) {
                list.add(new Sds(key.getBytes(Charset.forName("UTF-8"))));
            }
        }
        if (list.isEmpty()) {
            return RespMsg.returnEmptyArray();
        }
        return RespMsg.returnArray(list, false);
    }

    /**
     * scan cursor match xxx count xxx
     *
     * 全部扫描
     *
     * @param args
     * @return
     */
    public static ByteBuffer scanCommand(List<Sds> args) {

        if (args.size() != 6) {
            return RespMsg.returnWrongNumberMsg("scan");
        }

        Sds cursorSds = args.get(1);
        try {
            Integer cursor = Integer.parseInt(new String(cursorSds.getBuf(), Charset.forName("UTF-8")));
        } catch (NumberFormatException e) {
            return RespMsg.returnErrMsg("invalid cursor");
        }

        Sds countSds = args.get(5);
        try {
            Integer count = Integer.parseInt(new String(countSds.getBuf(), Charset.forName("UTF-8")));
        } catch (NumberFormatException e) {
            return RespMsg.returnErrMsg("value is not integer or out of range");
        }

        String pattern = new String(args.get(3).getBuf(), Charset.forName("UTF-8"));
        StrPattern strPattern = StrPattern.build(pattern);

        List<Object> list = new ArrayList<>();
        list.add(new Sds("0".getBytes(Charset.forName("UTF-8"))));
        List<Object> respArgs = new ArrayList<>();
        list.add(respArgs);
        for (String key : MemoryDB.dbMap.keySet()) {
            if (strPattern.isMatch(key)) {
                respArgs.add(new Sds(key.getBytes(Charset.forName("UTF-8"))));
            }
        }

        return RespMsg.returnArray(list, false);
    }
}
