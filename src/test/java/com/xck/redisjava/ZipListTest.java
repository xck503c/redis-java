package com.xck.redisjava;

import com.xck.redisjava.base.ZipList;
import org.junit.jupiter.api.Test;

public class ZipListTest {

    @Test
    public void insertTest() throws Exception{
        ZipList zipList = new ZipList();
        zipList.ziplistNew();

        zipList.ziplistInsert("a1");

        ZipList.ZlEntry zlEntry = zipList.getEntry(0);

        assert new String((byte[])zlEntry.getContent(), "utf-8").equals("a1");
    }

    @Test
    public void delTest() throws Exception{
        ZipList zipList = new ZipList();
        zipList.ziplistNew();

        zipList.ziplistInsert("a1");
        zipList.ziplistInsert("a2");
        zipList.ziplistInsert("a3");

        zipList.delEntry(1);
        ZipList.ZlEntry zlEntry = zipList.getEntry(0);
        assert new String((byte[])zlEntry.getContent(), "utf-8").equals("a1");

        zlEntry = zipList.getEntry(1);
        assert new String((byte[])zlEntry.getContent(), "utf-8").equals("a3");
    }
}
