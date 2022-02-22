package com.zmm.kv.api;

/**
 * sst:         (L0) 1024 * 1024 * 16
 *              (L1) 1024 * 1024 * 40
 *              (L2) 1024 * 1024 * 100
 *              (L3) 1024 * 1024 * 300
 *              (L4) 1024 * 1024 * 600
 *              (L5) 1024 * 1024 * 1024
 * block:       1024 * 4
 * memTable:    1024 * 1024 * 16
 * wal:         1024 * 1024 * 16
 * valueSize:   1024 * 4
 * @author zmm
 * @date 2022/2/18 13:06
 */
public class Option {

    private final int memSize = 1024 * 10;
    private String dir;
    private final int blockSize = 1024 * 4;
    private final int[] sstSize = new int[]{1024 * 16,
                                            1024 * 50,
                                            1024 * 100,
                                            1024 * 300,
                                            1024 * 600,
                                            1024 * 1024};
    private final int valueSize = blockSize;

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
    public int getSstSize(int level) {
        return sstSize[level];
    }
    public int getValueSize() {
        return valueSize;
    }
}
