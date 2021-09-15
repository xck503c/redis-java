package com.xck.redisjava;

import com.xck.redisjava.base.Sds;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * 服务端BIO
 *
 * 暂时接收命令，返回响应，处理get和set
 *
 * @author xuchengkun
 * @date 2021/09/14 11:38
 **/
public class Server {

    private int port;

    private ServerSocketChannel serverSocketChannel;

    public Server(int port) {
        this.port = port;
    }

    public void start() throws IOException {

        //打开选择器
        Selector selector = Selector.open();

        //服务端通道打开并注册
        serverSocketChannel = ServerSocketChannel.open();
        serverSocketChannel.configureBlocking(false);
        serverSocketChannel.bind(new InetSocketAddress(port));
        serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);

        while (true) {
            if (selector.select() > 0){
                Set<SelectionKey> selectionKeys = selector.selectedKeys();
                Iterator<SelectionKey> it = selectionKeys.iterator();

                while (it.hasNext()) {
                    SelectionKey selectionKey = it.next();
                    it.remove();

                    if (selectionKey.isAcceptable()) {
                        SocketChannel socketChannel = serverSocketChannel.accept();
                        socketChannel.socket().setSoTimeout(60000);
                        socketChannel.configureBlocking(false);
                        socketChannel.register(selector,  SelectionKey.OP_READ);
                    }else if (selectionKey.isReadable()) {
                        SocketChannel socketChannel = (SocketChannel)selectionKey.channel();
                        List<Sds> args = readCommand(socketChannel);
                        ByteBuffer returnBuffer = handleCommand(args);
                        socketChannel.write(returnBuffer);
                    }
                }
            }
        }
    }

    public List<Sds> readCommand(SocketChannel socketChannel) throws IOException {

        ByteBuffer byteBuffer = ByteBuffer.allocate(1);
        ByteBuffer skip2 = ByteBuffer.allocate(2);

        //读标识
        socketChannel.read(byteBuffer);
        byteBuffer.flip();
        char tag = (char)byteBuffer.get();
        assert tag == '*';

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

    public int readNum(SocketChannel socketChannel, ByteBuffer byteBuffer, char... endTag) throws IOException {

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

    public ByteBuffer handleCommand(List<Sds> args){
        String command = new String(args.get(0).getBuf(), Charset.forName("UTF-8"));
        switch (command) {
            case "set":
                String key = new String(args.get(1).getBuf(), Charset.forName("UTF-8"));;
                MemoryDB.dbMap.put(key, args.get(2).getBuf());
                break;
            case "get":
                String key1 = new String(args.get(1).getBuf(), Charset.forName("UTF-8"));
                byte[] value = (byte[])MemoryDB.dbMap.get(key1);
                ByteBuffer resp = ByteBuffer.allocate(("+").length() + value.length + "\r\n".length());
                resp.put("+".getBytes(Charset.forName("UTF-8")));
                resp.put(value);
                resp.put("\r\n".getBytes(Charset.forName("UTF-8")));
                resp.flip(); //这里要加上
                return resp;
            default:
                break;
        }
        return returnOk();
    }

    public ByteBuffer returnOk(){
        //返回ok
        String respOk = "+OK\r\n";
        return ByteBuffer.wrap(respOk.getBytes(Charset.forName("UTF-8")));
    }
}
