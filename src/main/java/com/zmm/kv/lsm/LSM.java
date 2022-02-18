package com.zmm.kv.lsm;

import com.zmm.kv.api.DBIterator;

/**
 * @author zmm
 * @date 2022/2/17 19:20
 */
public class LSM {

    /** 内存表 */
    private MemTable menTable;

    public boolean put(byte[] key, byte[] val) {
        return false;
    }

    public byte[] get(byte[] key) {
        return null;
    }

    public boolean del(byte[] key)  {
        return false;
    }

    public DBIterator iterator() {
        return null;
    }
}
