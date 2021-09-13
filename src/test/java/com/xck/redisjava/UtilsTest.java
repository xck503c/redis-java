package com.xck.redisjava;

import com.xck.redisjava.util.Utils;
import org.junit.jupiter.api.Test;

import java.nio.charset.Charset;

public class UtilsTest {

    @Test
    public void byteintconvertTest() {
        assert Utils.bytes2Int(Utils.int2Bytes(10)) == 10 ;
        assert Utils.bytes2Int(Utils.int2Bytes(1000)) == 1000 ;
    }

    @Test
    public void charbyte2NumberTest() {
        String c1 = Long.MAX_VALUE + "";
        assert (Long)Utils.charBytes2Number(c1.getBytes(Charset.forName("UTF-8"))) == Long.MAX_VALUE;

        String c2 = Long.MIN_VALUE + "";
        assert (Long)Utils.charBytes2Number(c2.getBytes(Charset.forName("UTF-8"))) == Long.MIN_VALUE;
    }
}
