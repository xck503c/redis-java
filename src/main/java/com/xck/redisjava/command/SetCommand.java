package com.xck.redisjava.command;

import com.xck.redisjava.MemoryDB;
import com.xck.redisjava.base.Sds;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.HashSet;
import java.util.List;

/**
 * set类相关命令处理
 *
 * @author xuchengkun
 * @date 2021/09/24 11:14
 **/
public class SetCommand {

    /**
     * sadd key member1 member2
     *
     * @param args
     * @return 成功添加的数量
     */
    public static ByteBuffer saddCommand(List<Sds> args) {
        if (args.size() < 3) {
            return RespMsg.returnWrongNumberMsg("sadd");
        }

        String key = new String(args.get(1).getBuf(), Charset.forName("UTF-8"));
        Object setO = MemoryDB.dbMap.get(key);
        if (setO != null && !(setO instanceof HashSet)) {
            return RespMsg.returnErrOperatorWrongType();
        }

        HashSet<Sds> set = (HashSet<Sds>) setO;
        if (set == null) {
            MemoryDB.dbMap.put(key, set = new HashSet<>());
        }

        int addCount = 0;
        for (int i = 2; i < args.size(); i++) {
            if (set.add(args.get(i))) {
                ++addCount;
            }
        }

        return RespMsg.returnNumber(addCount);
    }

    /**
     * scard key
     *
     * @param args
     * @return 成员数量
     */
    public static ByteBuffer scardCommand(List<Sds> args) {
        if (args.size() != 2) {
            return RespMsg.returnWrongNumberMsg("scard");
        }

        String key = new String(args.get(1).getBuf(), Charset.forName("UTF-8"));
        Object setO = MemoryDB.dbMap.get(key);
        if (setO != null && !(setO instanceof HashSet)) {
            return RespMsg.returnErrOperatorWrongType();
        }

        HashSet<Sds> set = (HashSet<Sds>) setO;
        if (set == null) {
            return RespMsg.returnNumber(0);
        }

        return RespMsg.returnNumber(set.size());
    }

    /**
     * sismember key member
     *
     * @param args
     * @return 成员元素是集合的成员，返回 1 。 如果成员元素不是集合的成员，或 key 不存在，返回 0
     */
    public static ByteBuffer sismemberCommand(List<Sds> args) {
        if (args.size() != 3) {
            return RespMsg.returnWrongNumberMsg("sismember");
        }

        String key = new String(args.get(1).getBuf(), Charset.forName("UTF-8"));
        Object setO = MemoryDB.dbMap.get(key);
        if (setO != null && !(setO instanceof HashSet)) {
            return RespMsg.returnErrOperatorWrongType();
        }

        HashSet<Sds> set = (HashSet<Sds>) setO;
        if (set == null) {
            return RespMsg.returnNumber(0);
        }

        boolean isContains = set.contains(args.get(2));

        return RespMsg.returnNumber(isContains ? 1 : 0);
    }
}
