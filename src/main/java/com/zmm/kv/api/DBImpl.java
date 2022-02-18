package com.zmm.kv.api;

/**
 * @author zmm
 * @date 2022/2/17 19:15
 */
public class DBImpl implements DB{


    public boolean put(byte[] key, byte[] value) {
        return false;
    }

    public byte[] get(byte[] key) {
        return new byte[0];
    }

    public boolean del(byte[] del) {
        return false;
    }

    public DBIterator iterator() {
        return null;
    }

    public void close() {

    }
}
