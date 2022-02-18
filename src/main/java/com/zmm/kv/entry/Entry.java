package com.zmm.kv.entry;

/**
 * @author zmm
 * @date 2022/2/17 20:00
 */
public class Entry {

    private final byte[] key;
    private byte[] value;

    public Entry(byte[] key, byte[] value) {
        this.key = key;
        this.value = value;
    }

    public byte[] getKey() {
        return key;
    }

    public void setValue(byte[] value) {
        this.value = value;
    }

    public byte[] getValue() {
        return value;
    }

    public int size() {
        return key.length + value.length;
    }
}
