package com.xck.redisjava;

import com.xck.redisjava.base.ZipList;
import org.junit.jupiter.api.Test;

import java.nio.charset.Charset;

public class ZipListTest {

    @Test
    public void insertTest() throws Exception {
        ZipList zipList = new ZipList();
        zipList.ziplistNew();

        zipList.ziplistInsert("a1".getBytes(Charset.forName("UTF-8")));

        ZipList.ZlEntry zlEntry = zipList.getEntry(0);

        assert new String((byte[]) zlEntry.getContent(), "utf-8").equals("a1");
    }

    @Test
    public void delTest() throws Exception {
        ZipList zipList = new ZipList();
        zipList.ziplistNew();

        zipList.ziplistInsert("a1".getBytes(Charset.forName("UTF-8")));
        zipList.ziplistInsert("a2".getBytes(Charset.forName("UTF-8")));
        zipList.ziplistInsert("a3".getBytes(Charset.forName("UTF-8")));

        zipList.delEntry(1);
        ZipList.ZlEntry zlEntry = zipList.getEntry(0);
        assert new String((byte[]) zlEntry.getContent(), "utf-8").equals("a1");

        zlEntry = zipList.getEntry(1);
        assert new String((byte[]) zlEntry.getContent(), "utf-8").equals("a3");
    }

    @Test
    public void lenTest() throws Exception {
        ZipList zipList = new ZipList();
        zipList.ziplistNew();

        zipList.ziplistInsert("a1".getBytes(Charset.forName("UTF-8")));
        assert zipList.len() == 1;
        zipList.ziplistInsert("a2".getBytes(Charset.forName("UTF-8")));
        assert zipList.len() == 2;
        zipList.ziplistInsert("a3".getBytes(Charset.forName("UTF-8")));
        assert zipList.len() == 3;
        zipList.delEntry(1);
        assert zipList.len() == 2;

    }
}
