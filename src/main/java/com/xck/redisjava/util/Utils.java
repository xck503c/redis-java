package com.xck.redisjava.util;

public class Utils {

    public static byte[] long2Bytes(long i) {
        byte[] bytes = new byte[8];
        bytes[0] = (byte) (i >>> 56);
        bytes[1] = (byte) (i >>> 48);
        bytes[2] = (byte) (i >>> 40);
        bytes[3] = (byte) (i >>> 32);
        bytes[4] = (byte) (i >>> 24);
        bytes[5] = (byte) (i >>> 16);
        bytes[6] = (byte) (i >>> 8);
        bytes[7] = (byte) i;
        return bytes;
    }

    public static byte[] int2Bytes(int i) {
        byte[] bytes = new byte[4];
        bytes[0] = (byte) (i >>> 24);
        bytes[1] = (byte) (i >>> 16);
        bytes[2] = (byte) (i >>> 8);
        bytes[3] = (byte) i;
        return bytes;
    }

    /**
     * 字节数组转换int，这里要注意，为什么要&0xff，因为补码高位补1，这里可以去掉高位的1
     *
     * @param bytes
     * @return
     */
    public static int bytes2Int(byte[] bytes) {
        int value = 0;
        value |= ((((int) bytes[0]) & 0xff) << 24);
        value |= ((((int) bytes[1]) & 0xff) << 16);
        value |= ((((int) bytes[2]) & 0xff) << 8);
        value |= (((int) bytes[3]) & 0xff);
        return value;
    }

    public static byte[] short2Bytes(short i) {
        byte[] bytes = new byte[2];
        bytes[0] = (byte) (i >>> 8);
        bytes[1] = (byte) i;
        return bytes;
    }

    public static short bytes2Short(byte[] bytes) {
        short value = 0;
        value |= ((((short) bytes[0]) & 0xff) << 8);
        value |= (((short) bytes[1]) & 0xff);
        return value;
    }

    public static long bytes2Long(byte[] bytes) {
        long value = 0;
        value |= ((((long) bytes[0]) & 0xff) << 56);
        value |= ((((long) bytes[1]) & 0xff) << 48);
        value |= ((((long) bytes[2]) & 0xff) << 40);
        value |= ((((long) bytes[3]) & 0xff) << 32);
        value |= ((((long) bytes[4]) & 0xff) << 24);
        value |= ((((long) bytes[5]) & 0xff) << 16);
        value |= ((((long) bytes[6]) & 0xff) << 8);
        value |= (((long) bytes[7]) & 0xff);
        return value;
    }

    public static Number str2Number(String str) {
        try {
            String maxLongStr = Long.MAX_VALUE+"";
            if (str.length() > maxLongStr.length()) return null;

            long v = Long.parseLong(str);
            if (v <= Byte.MAX_VALUE) {
                return (byte)v;
            } else if (v <= Short.MAX_VALUE) {
                return(short)v;
            } else if (v <= Integer.MAX_VALUE) {
                return (int)v;
            } else if (v <= Long.MAX_VALUE) {
                return v;
            }
        } catch (NumberFormatException e) {
        }

        return null;
    }

    public static Number bytes2Number(byte[] b) {
        if (b.length == 1) {
            return b[0];
        } else if (b.length == 2) {
            return bytes2Short(b);
        } else if (b.length == 4) {
            return bytes2Int(b);
        } else {
            return bytes2Long(b);
        }
    }

    public static void number2Bytes(long i, byte[] b) {
        if (b.length == 1) {
            b[0] = (byte)i;
        } else if (b.length == 2) {
            System.arraycopy(Utils.short2Bytes((short)i), 0, b, 0, 2);
        } else if (b.length == 4) {
            System.arraycopy(Utils.int2Bytes((int)i), 0, b, 0, 4);
        } else {
            System.arraycopy(Utils.long2Bytes(i), 0, b, 0, 8);
        }
    }
}
