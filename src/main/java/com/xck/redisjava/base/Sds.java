package com.xck.redisjava.base;

/**
 * 简单动态字符串sds
 *
 * 暂时先统一实现，int类型，后面再考虑将不同长度用不同数字类型分开
 *
 * @author xuchengkun
 * @date 2021/09/13 11:03
 **/
public class Sds {

    /**
     * SDS根据表达的长度不同，分为不同的类型
     * <p>
     * int len; //已经使用字节数
     * int free; //未使用的字节数
     * byte[] buf; //保存数据的缓冲区
     */
    byte[] buf;
    int free;
    int len;

    /**
     * sds初始化
     *
     * @param target
     */
    public Sds(byte[] target) {
        this.buf = target;
        this.free = 0;
        this.len = buf.length;
    }

    /**
     * 字符串copy，将目标字符串复制到sds中，覆盖原有数据，类似替换
     *
     * 1. 如果现有的长度不够，要扩容
     * 2. 复制过来，更新长度信息
     *
     * @param target 待copy的字符串
     * @return
     */
    public void sdsCopy(byte[] target){
        //原总长度是否足够存储新的数据
        int total = buf.length;
        if (total < target.length) {
            expansion(target.length - total);
        }
        System.arraycopy(target, 0, buf, 0, target.length);
        len = target.length;
        free = buf.length - len;
    }

    /**
     * 拼接字符串
     *
     * @param target
     * @return
     */
    public void sdsCat(byte[] target){
        expansion(target.length);
        System.arraycopy(target, 0, buf, len, target.length);
        len = len + target.length;
        free = buf.length - len;
    }

    /**
     * 扩容
     *
     * 1. 如果剩余空间还充足，就直接返回
     * 2. 若最终长度<1MB，则扩大为2倍
     *      若最终长度>=1MB，则直接扩容1MB
     * 3. 然后重新分配空间，数据copy过去，返回就行了
     *
     * @param addLen 新增的字节长度
     */
    public void expansion(int addLen){

        if (free >= addLen) return;

        int newLen = len + free + addLen;
        if (newLen < 1024*1024) {
            newLen *= 2;
        } else {
            newLen += 1024*1024;
        }

        byte[] newBuf = new byte[newLen];
        System.arraycopy(buf, 0, newBuf, 0, len);
        this.buf = newBuf;
    }

    /**
     * sds的使用长度
     * @return
     */
    public int sdsLen(){
        return len;
    }

    /**
     * sds的空闲长度
     * @return
     */
    public int sdsFree(){
        return free;
    }

    public byte[] getBuf() {
        byte[] b = new byte[len];
        System.arraycopy(buf, 0, b, 0, len);
        return b;
    }
}
