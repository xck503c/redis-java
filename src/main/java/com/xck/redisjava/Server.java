package com.xck.redisjava;

import com.xck.redisjava.base.Sds;
import com.xck.redisjava.command.CommandHandler;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
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
                        List<Sds> args = CommandHandler.readCommand(socketChannel);
                        ByteBuffer returnBuffer = CommandHandler.handle(args);
                        socketChannel.write(returnBuffer);
                    }
                }
            }
        }
    }
}
