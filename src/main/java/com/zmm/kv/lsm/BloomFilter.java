package com.zmm.kv.lsm;

/**
 * @author zmm
 * @date 2022/2/17 19:21
 */
public class BloomFilter {

    private final byte[] filter;
    private int size;

    public BloomFilter(byte[] filter) {
        this.filter = filter;
        this.size = filter.length * 8;
    }

    public BloomFilter(int count, float p) {
        // 根据bloom的公式
        // size = - count * ln(p) / ln(2)^2
        size = (int) - (count * Math.log(p) / Math.pow(Math.log(2), 2));
        // 除8是因为我们要节约内存。1byte有8位
        int len = size / 8;
        if (len == 0) {
            size = 8;
            len = 1;
        }
        filter = new byte[len];
    }

    public void appendKey(byte[] key) {
        int hash = hash(key);
        int bytePos = hash / 8;
        int bitPos = hash % 8;
        filter[bytePos] |= 1 << bitPos;
    }

    public boolean containKey(byte[] key) {
        int hash = hash(key);
        int bytePos = hash / 8;
        int bitPos = hash % 8;

        return (filter[bytePos] >> bitPos & 1) == 1;
    }

    /**
     * Murmurhash3
     * @param key       key
     * @return          hash value
     */
    private int hash(byte[] key) {
        long seed = 0xbc9f1d34;
        long m = 0xc6a4a793;
        int h = (int) ((int) (seed) ^ key.length * m);
        int index = 3;
        while (index < key.length) {
            h += key[index - 3] | key[index - 2] << 8 |
                    key[index - 1] << 16 | key[index] << 24;
            h *= m;
            h ^= h >> 16;
            index += 4;
        }
        switch (key.length + 3 - index) {
            case 3:
                h += key[index - 1] << 16;
            case 2:
                h += key[index - 2] << 8;
            case 1:
                h += key[index -3];
                h *= m;
                h ^= h >> 24;
        }
        return h % size;
    }

    public static void main(String[] args) {
        BloomFilter filter = new BloomFilter(10, 0.8f);
        filter.appendKey("a".getBytes());
        filter.appendKey("b".getBytes());
        System.out.println(filter.containKey("a".getBytes()));
        System.out.println(filter.containKey("c".getBytes()));
    }
}
