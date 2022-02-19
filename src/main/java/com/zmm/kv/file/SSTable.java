package com.zmm.kv.file;

import com.zmm.kv.lsm.MemTable;

/**
 * @author zmm
 * @date 2022/2/18 14:08
 */
public class SSTable {

    private static int fileNum = 0;

    private byte[] header;
    private byte[] index;
    private byte[] data;

    public void incFileNum() {
        fileNum++;
    }

    public static SSTable build(MemTable memTable) {

        SSTable ssTable = new SSTable();

        return ssTable;
    }

    private void buildHeader() {

        //
        // type: 类型             1字节
        // indexLen: 索引长度
        // size: 整个sst的字节     4字节 可以表示1G多点的sst文件
    }

    private void buildIndex() {

    }

    private void buildData(MemTable memTable) {

    }

    public static int getFileNum() {
        return fileNum;
    }
}
