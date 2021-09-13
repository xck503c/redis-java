package com.xck.redisjava.base;

import com.xck.redisjava.util.Utils;

/**
 * 对应redis中的压缩列表，为了方便java实现，对格式定义做了改动
 *
 * 提供操作：
 * 1. 单元素的插入，删除和获取
 * 2. 长度的获取
 */
public class ZipList {

    private int zlTail; //尾偏移量
    private short zlEntrySize; //节点数量
    private byte[] zl;

    public static final class ZlEntry {
        /**
         * 1. 字节编码：类型+字节长度
         * 0x00-1字节
         * 0x40-2字节
         * 0x80-4字节
         * 2. 数字编码：单字节类型
         * 0xc0 - byte
         * 0xd0 - short
         * 0xe0 - int
         * 0xf0 - long
         */
        private final static byte onebyteBytes = (byte) 0x00;
        private final static byte twobyteBytes = (byte) 0x40;
        private final static byte fourbyteBytes = (byte) 0x80;

        private final static byte byteTag = (byte) 0xc0;
        private final static byte shortTag = (byte) 0xd0;
        private final static byte intTag = (byte) 0xe0;
        private final static byte longTag = (byte) 0xf0;


        int len;

        int prevLenSize; //信息本身占用的字节数
        int prevLen; //前面节点长度信息
        byte encoding;
        Object content; //entry数据

        public int getLen() {
            return len;
        }

        public int getPrevLenSize() {
            return prevLenSize;
        }

        public int getPrevLen() {
            return prevLen;
        }

        public byte getEncoding() {
            return encoding;
        }

        public Object getContent() {
            return content;
        }
    }

    /**
     * 创建新的压缩列表，需要分配：
     * 1. zlbytes：总字节数，4字节，java中不需要
     * 2. zltail：尾节点相对列表开始的偏移量，4字节
     * 3. zllen: 列表中的节点个数，2字节
     * 4. zlend: 列表标识符，1字节，java中不需要
     *
     * @return
     */
    public void ziplistNew() {
        zlTail = zlEntrySize = 0;
        zl = new byte[0];
    }

    /**
     * 插入元素，固定为尾插法
     * 1. 首先获取最后一个节点
     * 2. 尝试编码为数字，不行就编码为字节数组
     * 3. 重新分配压缩列表
     * 4. 写入entry
     *
     * @param item 待插入元素
     * @return
     */
    public void ziplistInsert(byte[] item) {

        //tail节点
        ZlEntry tailEntry = entryParse(zl, zlTail);

        ZlEntry newEntry = null;
        Number number = Utils.charBytes2Number(item);
        if (number != null) { //可以转换为数字
            newEntry = value2Entry(number, tailEntry);
        } else {
            newEntry = value2Entry(item, tailEntry);
        }

        int reqTotalLen = zl.length + newEntry.len;

        int writeIndex = 0;
        //原数据copy
        byte[] newZl = new byte[reqTotalLen];
        System.arraycopy(zl, 0, newZl, 0, zl.length);
        writeIndex += zl.length;

        writeEntry(newEntry, writeIndex, newZl);

        zl = newZl;

        zlTail = zlTail + tailEntry.len; //tail往后递增

        zlEntrySize++;
    }

    /**
     * 删除给定位置的entry，需要考虑连锁更新的问题，因为删除一个节点，可能导致
     * 后面的节点的prev信息发生变化。
     * <p>
     * 1. 找到目标位置的节点
     * 2. 找到目标节点前面的节点
     * 2. 依次检查目标位置节点的后面节点是否需要重新分配长度
     *
     * @param i
     */
    public void delEntry(int i) {
        if (i < 0 || i >= zlEntrySize) {
            return;
        }

        /**
         *         待删除            待删除的下一个
         *           |                    |
         *           v                    v
         * |        del        |        delNext     |  ..  |
         * ^                              ^         ^
         * |                              |         |
         * index-delNet.len-del.len     index   zl.length
         */
        int index = 0;
        ZlEntry del = null, delNext = null;
        for (int j = 0; j < zlEntrySize; j++) {
            delNext = entryParse(zl, index);
            index += delNext.len;
            if (j == i + 1) { //到了下一个就停止
                break;
            }
            del = delNext;
        }

        byte[] newZl;
        if (del == delNext) {
            if (zlEntrySize > 1) { //删除最后一个
                newZl = new byte[zl.length - del.len];
                System.arraycopy(zl, 0, newZl, 0, newZl.length);
            } else { //删除第一个而且只有1个
                newZl = new byte[0];
            }
        } else {
            newZl = new byte[zl.length - del.len];
            int leftLen = index - delNext.len - del.len;
            int rightLen = newZl.length - leftLen;
            System.arraycopy(zl, 0, newZl, 0, leftLen);
            System.arraycopy(zl, index - delNext.len, newZl, leftLen, rightLen);

            checkEntryLen(newZl, del, index - delNext.len, leftLen);
        }

        zl = newZl;
        zlEntrySize--;
        zlTail = 0;
    }

    public ZlEntry getEntry(int i) {
        if (i < 0 || i >= zlEntrySize) {
            return null;
        }

        int index = 0;
        ZlEntry target = null;
        for (int j = 0; j < i + 1 && j < zlEntrySize; j++) {
            target = entryParse(zl, index);
            index += target.len;
        }

        return target;
    }

    public ZlEntry value2Entry(Object v, ZlEntry tailEntry) {
        ZlEntry zlEntry = new ZlEntry();

        zlEntry.len++;
        if (v instanceof Number) {
            byte[] numberBytes;
            long value;
            if (v instanceof Byte) {
                value = (Byte) v;
                zlEntry.encoding = ZlEntry.byteTag;
                numberBytes = new byte[1];
                zlEntry.len++;
            } else if (v instanceof Short) {
                value = (Short) v;
                zlEntry.encoding = zlEntry.shortTag;
                numberBytes = new byte[2];
                zlEntry.len += 2;
            } else if (v instanceof Integer) {
                value = (Integer) v;
                zlEntry.encoding = zlEntry.intTag;
                numberBytes = new byte[4];
                zlEntry.len += 4;
            } else {
                value = (Long) v;
                zlEntry.encoding = zlEntry.longTag;
                numberBytes = new byte[8];
                zlEntry.len += 8;
            }
            Utils.number2Bytes(value, numberBytes);
            zlEntry.content = numberBytes;
        } else {
            byte[] b = (byte[]) v;
            byte[] value;
            if (b.length <= Byte.MAX_VALUE) {
                zlEntry.encoding = ZlEntry.onebyteBytes;
                value = new byte[1 + b.length];
                value[0] = (byte) b.length;
                System.arraycopy(b, 0, value, 1, b.length);

            } else if (b.length <= Short.MAX_VALUE) {
                zlEntry.encoding = zlEntry.twobyteBytes;
                value = new byte[2 + b.length];
                System.arraycopy(Utils.short2Bytes((short) b.length), 0, value, 0, 2);
                System.arraycopy(b, 0, value, 2, b.length);

            } else {
                zlEntry.encoding = zlEntry.fourbyteBytes;
                value = new byte[4 + b.length];
                System.arraycopy(Utils.int2Bytes(b.length), 0, value, 0, 4);
                System.arraycopy(b, 0, value, 4, b.length);

            }
            zlEntry.content = value;
            zlEntry.len += value.length;
        }

        zlEntry.prevLen = tailEntry.len;
        if (tailEntry.prevLen < Byte.MAX_VALUE) {
            zlEntry.prevLenSize = 1;
        } else {
            zlEntry.prevLenSize = 5;
        }
        zlEntry.len += zlEntry.prevLenSize;

        return zlEntry;
    }

    /**
     * 解析压缩列表entry
     *
     * @param zipEntryStart
     * @return
     */
    public ZlEntry entryParse(byte[] zl, int zipEntryStart) {
        ZlEntry zlEntry = new ZlEntry();

        //原本就没有，直接返回
        if (zlEntrySize == 0) {
            zlEntry.len = 0;
            return zlEntry;
        }

        //解析前面一个节点长度
        prevParse(zipEntryStart, zlEntry);
        zipEntryStart += zlEntry.prevLenSize;

        zlEntry.len += zlEntry.prevLenSize;

        zlEntry.encoding = zl[zipEntryStart];
        zlEntry.len++;

        zipEntryStart++;

        //先判断是不是字符串编码
        if (zlEntry.encoding == ZlEntry.onebyteBytes) {
            zlEntry.content = new byte[zl[zipEntryStart]];
            zipEntryStart++;
            zlEntry.len++;
        } else if (zlEntry.encoding == ZlEntry.twobyteBytes) {
            byte[] tmp = new byte[2];
            System.arraycopy(zl, zipEntryStart, tmp, 0, 2);
            zlEntry.content = new byte[Utils.bytes2Short(tmp)];
            zipEntryStart += 2;
            zlEntry.len += 2;
        } else if (zlEntry.encoding == ZlEntry.fourbyteBytes) {
            byte[] tmp = new byte[4];
            System.arraycopy(zl, zipEntryStart, tmp, 0, 4);
            zlEntry.content = new byte[Utils.bytes2Int(tmp)];
            zipEntryStart += 4;
            zlEntry.len += 4;
        }

        if (zlEntry.len > 0) {
            byte[] b = (byte[]) zlEntry.content;
            System.arraycopy(zl, zipEntryStart, zlEntry.content, 0, b.length);
            zlEntry.len += b.length;
            return zlEntry;
        }

        //再判断是不是数字编码
        byte[] b;
        if (zlEntry.encoding == ZlEntry.byteTag) {
            b = new byte[1];
        } else if (zlEntry.encoding == ZlEntry.shortTag) {
            b = new byte[2];
        } else if (zlEntry.encoding == ZlEntry.intTag) {
            b = new byte[4];
        } else {
            b = new byte[8];
        }
        System.arraycopy(zl, zipEntryStart, b, 0, b.length);
        zlEntry.content = Utils.bytes2Number(b);
        zlEntry.len += b.length;

        return zlEntry;
    }

    public void prevParse(int zipEntryStart, ZlEntry zlEntry) {
        if (zl[zipEntryStart] == Byte.MIN_VALUE) {
            zlEntry.prevLenSize = 5;
            byte[] tmp = new byte[4];
            System.arraycopy(zl, zipEntryStart + 1, tmp, 0, 4);
            zlEntry.prevLen = Utils.bytes2Int(tmp);
        } else {
            zlEntry.prevLenSize = 1;
            zlEntry.prevLen = zl[zipEntryStart];
        }
    }

    /**
     * 写入entry，对象->字节数组
     *
     * @param newEntry   待写入的entry
     * @param writeIndex entry写入的起始索引
     * @param newZl      目标压缩列表
     */
    public void writeEntry(ZlEntry newEntry, int writeIndex, byte[] newZl) {

        writePrev(newEntry, writeIndex, newZl);
        writeIndex += newEntry.prevLenSize;

        //encoding写入
        newZl[writeIndex] = newEntry.encoding;
        writeIndex++;

        //数据写入
        byte[] content = (byte[]) newEntry.content;
        System.arraycopy(content, 0, newZl, writeIndex, content.length);
    }

    /**
     * 写入节点信息：prev信息
     *
     * @param newEntry
     * @param writeIndex
     * @param newZl
     */
    private void writePrev(ZlEntry newEntry, int writeIndex, byte[] newZl) {
        //前一个节点长度copy
        byte[] prevBytes = new byte[newEntry.prevLenSize];
        Utils.number2Bytes(newEntry.prevLenSize, prevBytes);
        System.arraycopy(prevBytes, 0, newZl, writeIndex, prevBytes.length);
    }

    /**
     * 从某个位置开始检查entry的长度，若有需要，进行更新
     *
     * @param newZl        新的压缩列表
     * @param del          删除节点信息
     * @param oldNextIndex 旧列表中删除节点的下一个节点起始索引
     * @param newNextIndex 新列表中删除节点的下一个节点起始索引
     */
    public void checkEntryLen(byte[] newZl, ZlEntry del, int oldNextIndex, int newNextIndex) {

        while (true) {
            //解析新列表里面的next节点
            ZlEntry nextNext = entryParse(newZl, newNextIndex);
            if (nextNext.len == 0) {
                return;
            }

            //记录next节点当前长度，计算分配值
            int oldNextPrevLenSize = nextNext.prevLenSize;
            int change = del.prevLenSize - nextNext.prevLenSize;

            //一样的编码长度，但数值不一样
            if (change == 0) {
                if (nextNext.prevLen != del.prevLen) {
                    System.arraycopy(zl, oldNextIndex, newZl, newNextIndex, nextNext.prevLenSize);
                }
                return;
            }

            //新的压缩列表
            byte[] tmpZl = new byte[newZl.length + change];
            System.arraycopy(newZl, 0, tmpZl, 0, newNextIndex);

            nextNext.prevLenSize = del.prevLenSize;
            nextNext.prevLen = del.prevLen;

            //刷新字节数组中的信息:写入长度
            writePrev(nextNext, newNextIndex, tmpZl);
            //将newZl长度后面的信息copy到tmp列表里面
            System.arraycopy(newZl, newNextIndex + oldNextPrevLenSize
                    , tmpZl, newNextIndex + nextNext.prevLenSize
                    , newZl.length - newNextIndex - oldNextPrevLenSize);

            newZl = tmpZl;

            newNextIndex += nextNext.len;
        }
    }

    /**
     * O(1) 复杂度获取列表长度
     */
    public int len() {
        return zlEntrySize;
    }
}