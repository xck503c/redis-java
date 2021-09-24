package com.xck.redisjava.command;

import com.xck.redisjava.MemoryDB;
import com.xck.redisjava.base.Sds;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.LinkedList;
import java.util.List;

/**
 * 列表类的命令
 *
 * @author xuchengkun
 * @date 2021/09/24 10:32
 **/
public class ListCommand {

    /**
     * llen key
     *
     * @param args
     * @return
     */
    public static ByteBuffer llenCommand(List<Sds> args) {
        if (args.size() != 2) {
            return RespMsg.returnWrongNumberMsg("llen");
        }

        String key = new String(args.get(1).getBuf(), Charset.forName("UTF-8"));
        Object listO = MemoryDB.dbMap.get(key);
        if (listO != null && !(listO instanceof LinkedList)) {
            return RespMsg.returnErrOperatorWrongType();
        }

        LinkedList<Sds> list = (LinkedList) listO;
        if (list == null) {
            return RespMsg.returnNumber(0);
        }

        return RespMsg.returnNumber(list.size());
    }

    /**
     * lpush key v1 v2
     * lpush可以批量可以单个
     * <p>
     * rpush key v1 v2
     *
     * @param args
     * @return 执行命令后，列表的长度。
     */
    public static ByteBuffer pushCommand(List<Sds> args, boolean isLeft) {
        if (args.size() < 3) {
            return RespMsg.returnWrongNumberMsg(isLeft ? "lpush" : "rpush");
        }

        String key = new String(args.get(1).getBuf(), Charset.forName("UTF-8"));
        Object listO = MemoryDB.dbMap.get(key);
        if (listO != null && !(listO instanceof LinkedList)) {
            return RespMsg.returnErrOperatorWrongType();
        }

        LinkedList<Sds> list = (LinkedList) listO;
        if (list == null) {
            list = new LinkedList<>();
            MemoryDB.dbMap.put(key, list);
        }

        for (int i = 2; i < args.size(); i++) {
            if (isLeft) {
                list.addFirst(args.get(i));
            } else {
                list.addLast(args.get(i));
            }
        }

        return RespMsg.returnNumber(list.size());
    }

    /**
     * lpop key
     * rpop key
     * 从左侧或右侧，移除一个元素
     *
     * @param args
     * @return 返回值为移除的元素，当列表不存在时，返回 nil
     */
    public static ByteBuffer popCommand(List<Sds> args, boolean isLeft) {
        if (args.size() != 2) {
            return RespMsg.returnWrongNumberMsg(isLeft ? "lpop" : "rpop");
        }

        String key = new String(args.get(1).getBuf(), Charset.forName("UTF-8"));
        Object listO = MemoryDB.dbMap.get(key);
        if (listO != null && !(listO instanceof LinkedList)) {
            return RespMsg.returnErrOperatorWrongType();
        }

        LinkedList<Sds> list = (LinkedList) listO;
        if (list == null || list.isEmpty()) {
            return RespMsg.returnNilBulkStr();
        }

        Sds sds = isLeft ? list.pollFirst() : list.pollLast();
        if (sds == null) {
            return RespMsg.returnNilBulkStr();
        }
        return RespMsg.returnBlukStr(sds.getBuf());
    }
}
