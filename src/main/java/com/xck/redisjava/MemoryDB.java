package com.xck.redisjava;

import java.util.HashMap;
import java.util.Map;

/**
 * 内存数据库
 *
 * @author xuchengkun
 * @date 2021/09/15 09:32
 **/
public class MemoryDB {

    /**
     * 直接用jdk hashmap代替
     */
    public static Map<String, Object> dbMap = new HashMap<>();
}
