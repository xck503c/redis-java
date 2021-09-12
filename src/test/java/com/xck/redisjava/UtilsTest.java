package com.xck.redisjava;

import com.xck.redisjava.util.Utils;
import org.junit.jupiter.api.Test;

public class UtilsTest {

    @Test
    public void byteintconvertTest() {
        assert Utils.bytes2Int(Utils.int2Bytes(10)) == 10 ;
        assert Utils.bytes2Int(Utils.int2Bytes(1000)) == 1000 ;
    }
}
