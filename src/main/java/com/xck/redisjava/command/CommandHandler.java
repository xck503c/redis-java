package com.xck.redisjava.command;

import com.xck.redisjava.base.Sds;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

/**
 * 命令处理器
 *
 * @author xuchengkun
 * @date 2021/09/23 09:15
 **/
public class CommandHandler {

    /**
     * 客户端发出命令：
     * 1. 命令是一个数组，所以里面是*开头
     * 2. 命令里面是块字符串，所以每个都是以$开头
     * @param socketChannel
     * @return
     * @throws IOException
     */
    public static List<Sds> readCommand(SocketChannel socketChannel) throws IOException {

        ByteBuffer byteBuffer = ByteBuffer.allocate(1);
        ByteBuffer skip2 = ByteBuffer.allocate(2);

        //读标识
        socketChannel.read(byteBuffer);
        byteBuffer.flip();
        char tag = (char)byteBuffer.get();
        assert tag == '*';

        //读取参数的个数
        int argNum = readNum(socketChannel, byteBuffer, new char[] {'\r', '\n'});
        //这里不能加flip
        socketChannel.read(byteBuffer); //跳过\r\n

        List<Sds> list = new ArrayList<>();

        for (int i = 0; i < argNum; i++) {
            byteBuffer.flip();
            socketChannel.read(byteBuffer);
            byteBuffer.flip();
            char argTag = (char)byteBuffer.get();
            assert argTag == '$';

            //读取块字符串的长度
            int len = readNum(socketChannel, byteBuffer, new char[] {'\r', '\n'});
            socketChannel.read(byteBuffer); //跳过\r\n

            ByteBuffer buf = ByteBuffer.allocate(len);
            socketChannel.read(buf);

            list.add(new Sds(buf.array()));

            socketChannel.read(skip2); //跳过\r\n
            skip2.flip();
        }

        return list;
    }

    public static int readNum(SocketChannel socketChannel, ByteBuffer byteBuffer, char... endTag) throws IOException {

        //读参数数量
        StringBuilder argNumChar = new StringBuilder();
        byteBuffer.flip();
        while (socketChannel.read(byteBuffer) != -1) {
            byteBuffer.flip();
            char c = (char)byteBuffer.get();
            byteBuffer.flip();

            for (int i = 0; i < endTag.length; i++) {
                if (c == endTag[i]) {
                    return Integer.parseInt(argNumChar.toString());
                }
            }
            argNumChar.append(c);
        }

        return -1;
    }

    /**
     * 1. set,get
     * 2. del,exists
     * 3. hset,hget,hlen,hmset,hmget
     * 4. flushdb
     * @param args
     * @return
     */
    public static ByteBuffer handle(List<Sds> args) {
        String command = new String(args.get(0).getBuf(), Charset.forName("UTF-8"));
        command = command.toLowerCase();
        switch (command) {
            //string类
            case "set":
                return StringCommand.setCommand(args);
            case "get":
                return StringCommand.getCommand(args);
            //key类
            case "del":
                return KeyCommand.delCommand(args);
            case "exists":
                return KeyCommand.existsCommand(args);
            //hash类
            case "hset":
                return HashCommand.hsetCommand(args);
            case "hget":
                return HashCommand.hgetCommand(args);
            case "hlen":
                return HashCommand.hlenCommand(args);
            case "hmset":
                return HashCommand.hmsetCommand(args);
            case "hmget":
                return HashCommand.hmgetCommand(args);
            //server类
            case "flushdb":
                return ServerCommand.flushdbCommand(args);

        }
        return RespMsg.returnErrMsg(String.format("unknown Command '%s'", command));
    }
}
