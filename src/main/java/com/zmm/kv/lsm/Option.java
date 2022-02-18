package com.zmm.kv.lsm;

/**
 * @author zmm
 * @date 2022/2/18 13:06
 */
public class Option {

    private int memSize = 1024;
    private String dir;
    private final int walSize = 1024 * 1024 * 16;

    public Option() {
        dir = System.getProperty("user.dir") + "\\db";
    }

    public Option dir(String dir) {
        this.dir = dir;
        return this;
    }

    public int getMemSize() {
        return memSize;
    }
    public int getWalSize() {
        return walSize;
    }
}
