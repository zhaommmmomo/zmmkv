package com.zmm.kv.api;

import com.google.protobuf.ByteString;
import com.zmm.kv.lsm.LSM;
import com.zmm.kv.pb.Entry;

/**
 * @author zmm
 * @date 2022/2/17 19:15
 */
public class DBImpl implements DB{

    private final LSM lsm;
    private final Option option;

    public DBImpl() {
        option = new Option();
        lsm = new LSM(option);
    }

    public DBImpl(Option option) {
        this.option = option;
        lsm = new LSM(option);
    }

    @Override
    public boolean put(byte[] key, byte[] value) {
        return lsm.put(Entry.newBuilder()
                            .setKey(ByteString.copyFrom(key))
                            .setValue(ByteString.copyFrom(value))
                            .build());
    }

    @Override
    public byte[] get(byte[] key) {
        return lsm.get(key);
    }

    @Override
    public boolean del(byte[] key) {
        return lsm.del(key);
    }

    @Override
    public Range range(byte[] startKey, byte[] endKey) {

        return null;
    }

    @Override
    public DBIterator iterator() {
        return lsm.iterator();
    }

    @Override
    public void close() {

    }
}
