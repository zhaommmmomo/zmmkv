package com.zmm.kv.api;

/**
 * sst:         (L0) 1024 * 1024 * 8
 *              (L1) 1024 * 1024 * 10
 *              (L2) 1024 * 1024 * 100
 *              (L3) 1024 * 1024 * 300
 *              (L4) 1024 * 1024 * 600
 *              (L5) 1024 * 1024 * 1024
 * block:       1024 * 4
 * memTable:    1024 * 4
 * wal:         1024 * 1024 * 16
 * @author zmm
 * @date 2022/2/18 13:06
 */
public class Option {

    private int memSize = 1024 * 4;
    private String dir;
    private final int walSize = 1024 * 1024 * 16;
    private int blockSize = 1024 * 4;
    private int sstSize = 1024 * 1024 * 1024;

    public Option() {
        dir = System.getProperty("user.dir") + "\\db";
    }

    public Option dir(String dir) {
        this.dir = dir;
        return this;
    }

    public String getDir() {
        return dir;
    }
    public int getMemSize() {
        return memSize;
    }
    public int getBlockSize() {
        return blockSize;
    }
    public int getSstSize() {
        return sstSize;
    }
    public int getWalSize() {
        return walSize;
    }
}
