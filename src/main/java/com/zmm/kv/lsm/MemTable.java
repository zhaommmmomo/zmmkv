package com.zmm.kv.lsm;

import com.zmm.kv.api.DBIterator;
import com.zmm.kv.pb.Entry;

/**
 * @author zmm
 * @date 2022/2/17 19:21
 */
public abstract class MemTable {

    public boolean put(Entry entry) {
        return false;
    }

    public byte[] get(byte[] key) {
        return null;
    }

    public boolean del(byte[] key)  {
        return false;
    }

    public int size() {
        return 0;
    }

    public int len() {
        return 0;
    }

    public DBIterator iterator() {
        return null;
    }
}
