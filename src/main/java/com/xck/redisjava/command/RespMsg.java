package com.xck.redisjava.command;

import com.xck.redisjava.base.Sds;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.List;

/**
 * 返回信息
 *
 * @author xuchengkun
 * @date 2021/09/23 15:52
 **/
public class RespMsg {

    /**
     * 简单字符串的字符形式
     *
     * @param info
     * @return
     */
    public static ByteBuffer returnSimpleStr(String info) {
        String resp = "+" + info + "\r\n";
        return ByteBuffer.wrap(resp.getBytes(Charset.forName("UTF-8")));
    }

    /**
     * 错误信息
     *
     * @param info
     * @return
     */
    public static ByteBuffer returnErrMsg(String info) {
        String resp = "-ERR " + info + "\r\n";
        return ByteBuffer.wrap(resp.getBytes(Charset.forName("UTF-8")));
    }

    public static ByteBuffer returnWrongTypeMsg(String info) {
        String resp = "-WRONGTYPE " + info + "\r\n";
        return ByteBuffer.wrap(resp.getBytes(Charset.forName("UTF-8")));
    }

    /**
     * 块字符串：$长度\r\nxxxx\r\n
     *
     * @param info
     * @return
     */
    public static ByteBuffer returnBlukStr(byte[] info) {
        byte[] lenStrBytes = ("$" + info.length + "\r\n").getBytes(Charset.forName("UTF-8"));
        ByteBuffer resp = ByteBuffer.allocate(lenStrBytes.length + info.length + 2);
        resp.put(lenStrBytes);
        resp.put(info);
        resp.put("\r\n".getBytes(Charset.forName("UTF-8")));
        resp.flip();
        return resp;
    }

    /**
     * 块字符串：:数字\r\n
     *
     * @param number
     * @return
     */
    public static ByteBuffer returnNumber(int number) {
        return ByteBuffer.wrap((":" + number + "\r\n").getBytes(Charset.forName("UTF-8")));
    }

    public static ByteBuffer returnWrongNumberMsg(String command) {
        return RespMsg.returnErrMsg(String.format("wrong number of arguments for '%s' command", command));
    }

    /**
     * 操作不支持该操作的数据类型
     *
     * @return
     */
    public static ByteBuffer returnErrOperatorWrongType() {
        return RespMsg.returnWrongTypeMsg("Operation against a key holding the wrong kind of value");
    }

    /**
     * 空字符串，也就是nil
     *
     * @return
     */
    public static ByteBuffer returnNilBulkStr() {
        return ByteBuffer.wrap(("$-1\r\n").getBytes(Charset.forName("UTF-8")));
    }

    /**
     * 封装数组，返回指定数量的nil
     * @param len
     * @return
     */
    public static ByteBuffer returnEmptyArray(int len){
        StringBuilder sb = new StringBuilder("*" + len + "\r\n");
        for (int i = 0; i < len; i++) {
            sb.append("$-1\r\n");
        }
        return ByteBuffer.wrap((sb.toString()).getBytes(Charset.forName("UTF-8")));
    }

    /**
     * 返回数组，若数组为空，则全部返回nil
     * @param list
     * @return
     */
    public static ByteBuffer returnArray(List<Sds> list){
        byte[] prefix = ("*" + list.size() + "\r\n").getBytes(Charset.forName("UTF-8"));
        int dataLen = 0;
        for (int i = 0; i < list.size(); i++) {
            if (list.get(i) == null) {
                dataLen += 5;
                continue;
            }
            dataLen += 5; //$\r\n和\r\n
            int len = list.get(i).sdsLen();
            dataLen += (len + "").getBytes(Charset.forName("UTF-8")).length;
            dataLen += len;

        }
        ByteBuffer resp = ByteBuffer.allocate(prefix.length + dataLen);
        resp.put(prefix);
        for (int i = 0; i < list.size(); i++) {
            Sds sds = list.get(i);
            if (sds == null) { //若为null，则放入-1
                resp.put("$-1\r\n".getBytes(Charset.forName("UTF-8")));
                continue;
            }
            resp.put("$".getBytes(Charset.forName("UTF-8")));
            resp.put((sds.sdsLen() + "").getBytes(Charset.forName("UTF-8")));
            resp.put("\r\n".getBytes(Charset.forName("UTF-8")));
            resp.put(list.get(i).getBuf());
            resp.put("\r\n".getBytes(Charset.forName("UTF-8")));
        }
        resp.flip();
        return resp;
    }
}
