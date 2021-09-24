package com.xck.redisjava.command;

import com.xck.redisjava.base.Sds;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * 连接相关的命令
 *
 * @author xuchengkun
 * @date 2021/09/24 14:47
 **/
public class ConnectionCommand {

    public static ByteBuffer pingCommand(List<Sds> args){
        if (args.size() != 1) {
            return RespMsg.returnWrongNumberMsg("ping");
        }

        return RespMsg.returnSimpleStr("PONG");
    }

    public static ByteBuffer quitCommand(List<Sds> args){
        if (args.size() != 1) {
            return RespMsg.returnWrongNumberMsg("quit");
        }

        return RespMsg.returnSimpleStr("OK");
    }
}
