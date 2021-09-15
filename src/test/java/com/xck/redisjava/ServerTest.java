package com.xck.redisjava;

import org.junit.jupiter.api.Test;

/**
 * 服务端交互测试
 *
 * @author xuchengkun
 * @date 2021/09/14 13:55
 **/
public class ServerTest {

    @Test
    public void loginTest() throws Exception{
        Server server = new Server(6379);
        server.start();
    }
}
