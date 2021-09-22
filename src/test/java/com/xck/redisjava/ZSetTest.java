package com.xck.redisjava;

import com.xck.redisjava.base.Sds;
import com.xck.redisjava.base.ZSet;
import org.junit.jupiter.api.Test;

import java.nio.charset.Charset;

/**
 * zset测试
 *
 * @author xuchengkun
 * @date 2021/09/19 11:49
 **/
public class ZSetTest {

    @Test
    public void testInsertAndDelete() {
        Sds value = new Sds("hello1".getBytes(Charset.forName("UTF-8")));
        Sds value1 = new Sds("hello122".getBytes(Charset.forName("UTF-8")));
        Sds value2 = new Sds("hello122".getBytes(Charset.forName("UTF-8")));

        ZSet zSet = new ZSet();
        assert zSet.insert(2, value) == 1;
        assert zSet.len() == 1;
        assert zSet.findScoreByMember(value) == 2;
        assert zSet.insert(2, value) == 0;
        assert zSet.delete(value) == 1;
        assert zSet.len() == 0;
        assert zSet.insert(3, value1) == 1;
        assert zSet.insert(3, value2) == 0;
        assert zSet.len() == 1;
        assert zSet.findScoreByMember(value2) == 3;
    }

    @Test
    public void testRank() {
        Sds value = new Sds("hello1fff".getBytes(Charset.forName("UTF-8")));
        Sds value1 = new Sds("hello1ffff".getBytes(Charset.forName("UTF-8")));

        ZSet zSet = new ZSet();
        assert zSet.insert(2, value) == 1;
        assert zSet.insert(3, value1) == 1;
        assert zSet.findElemByRank(1).getMember().compareTo(value) == 0;
        assert zSet.findElemByRank(2).getMember().compareTo(value1) == 0;
    }

    @Test
    public void testRankMatch(){
        ZSet zSet = new ZSet();
        for (int i = 0; i < 10000; i++) {
            Sds order = new Sds(("hello" + (i+1)).getBytes(Charset.forName("UTF-8")));
            zSet.insert(i + 1, order);
        }

        Sds order5001 = new Sds(("hello5001").getBytes(Charset.forName("UTF-8")));
        assert zSet.findElemByRank(5001).getMember().compareTo(order5001) == 0;
        assert zSet.getElemRank(5001, order5001) == 5001;

        Sds order3451 = new Sds(("hello3451").getBytes(Charset.forName("UTF-8")));
        assert zSet.findElemByRank(3451).getMember().compareTo(order3451) == 0;
        assert zSet.getElemRank(3451, order3451) == 3451;
    }

    @Test
    public void testScoreRange(){
        ZSet zSet = new ZSet();
        for (int i = 0; i < 10000; i++) {
            Sds order = new Sds(("hello" + (i+1)).getBytes(Charset.forName("UTF-8")));
            zSet.insert(i + 1, order);
        }

        Sds order600 = new Sds(("hello600").getBytes(Charset.forName("UTF-8")));
        Sds order1000 = new Sds(("hello1000").getBytes(Charset.forName("UTF-8")));
        assert zSet.findFirstInScoreRange(600, 1000).getMember().compareTo(order600) == 0;
        assert zSet.findLastInScoreRange(600, 1000).getMember().compareTo(order1000) == 0;


        Sds order6910 = new Sds(("hello6910").getBytes(Charset.forName("UTF-8")));
        Sds order9723= new Sds(("hello9723").getBytes(Charset.forName("UTF-8")));
        assert zSet.findFirstInScoreRange(6910, 9723).getMember().compareTo(order6910) == 0;
        assert zSet.findLastInScoreRange(6910, 9723).getMember().compareTo(order9723) == 0;
    }

    @Test
    public void testScoreRangeDel(){
        ZSet zSet = new ZSet();
        for (int i = 0; i < 10000; i++) {
            Sds order = new Sds(("hello" + (i+1)).getBytes(Charset.forName("UTF-8")));
            zSet.insert(i + 1, order);
        }

        assert zSet.deleteByScoreRange(600, 1000) == 401;
    }

    @Test
    public void testRankRangeDel(){
        ZSet zSet = new ZSet();
        for (int i = 0; i < 10000; i++) {
            Sds order = new Sds(("hello" + (i+1)).getBytes(Charset.forName("UTF-8")));
            zSet.insert(i + 1, order);
        }

        assert zSet.deleteByRankRange(600, 1000) == 401;
    }
}
