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

    public DBImpl() {
        lsm = new LSM(new Option());
    }

    public boolean put(byte[] key, byte[] value) {

        // TODO: 2022/2/18 大value分离。 4kb以上的进行分离
        return lsm.put(Entry.newBuilder()
                            .setKey(ByteString.copyFrom(key))
                            .setValue(ByteString.copyFrom(value))
                            .build());
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
