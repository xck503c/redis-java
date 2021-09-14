package com.xck.redisjava;

import com.xck.redisjava.base.Sds;
import org.junit.jupiter.api.Test;

import java.nio.charset.Charset;

/**
 * sds测试
 *
 * @author xuchengkun
 * @date 2021/09/13 18:40
 **/
public class SdsTest {

    @Test
    public void sdsInitTest(){
        String str = "a1";
        Sds sds = new Sds(str.getBytes(Charset.forName("UTF-8")));
        assert new String(sds.getBuf(), Charset.forName("UTF-8")).equals(str);
        assert sds.sdsLen() == 2;
        assert sds.sdsFree() == 0;
    }

    @Test
    public void sdsCopyTest(){
        String str = "a1";
        Sds sds = new Sds(str.getBytes(Charset.forName("UTF-8")));

        String str1 = "a10000";
        sds.sdsCopy(str1.getBytes(Charset.forName("UTF-8")));
        assert sds.sdsLen() == str1.length();
        assert sds.sdsFree() == sds.sdsLen();
    }

    @Test
    public void sdsCatTest(){
        String str = "a1";
        Sds sds = new Sds(str.getBytes(Charset.forName("UTF-8")));

        String str1 = "a10000";
        sds.sdsCat(str1.getBytes(Charset.forName("UTF-8")));
        assert sds.sdsLen() == str1.length() + str.length();
        assert sds.sdsFree() == sds.sdsLen();
        assert new String(sds.getBuf(), Charset.forName("UTF-8")).equals(str + str1);
    }
}
