package com.zmm.kv.api;

/**
 * @author zmm
 * @date 2022/2/17 19:15
 */
public interface DB {
    boolean put(byte[] key, byte[] value);
    byte[] get(byte[] key);
    boolean del(byte[] del);
    DBIterator iterator();
    void close();
}
