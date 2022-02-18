package com.zmm.kv.api;

import com.zmm.kv.entry.Entry;
import com.zmm.kv.lsm.LSM;
import com.zmm.kv.lsm.Option;

/**
 * @author zmm
 * @date 2022/2/17 19:15
 */
public class DBImpl implements DB{

    private final LSM lsm;

    public DBImpl() {
        lsm = new LSM(new Option());
    }

    public boolean put(byte[] key, byte[] value) {
        return lsm.put(new Entry(key, value));
    }

    public byte[] get(byte[] key) {
        return lsm.get(key);
    }

    public boolean del(byte[] key) {
        return lsm.del(key);
    }

    public DBIterator iterator() {
        return lsm.iterator();
    }

    public void close() {

    }
}
