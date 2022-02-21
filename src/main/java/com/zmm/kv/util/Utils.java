package com.zmm.kv.util;

/**
 * @author zmm
 * @date 2022/2/21 14:17
 */
public class Utils {

    /**
     * 将key的前8个字节散列，方便比较
     * @param key           key
     * @return              key前8个字节散列后的hash
     */
    public static float calcScore(byte[] key) {
        int l = Math.min(key.length, 8);
        long hash = 0;
        for (int i = 0 ; i < l; i++) {
            int j = 64 - 8 - i * 8;
            hash |= key[i] << j;
        }
        return hash;
    }

    /**
     * 比较两个字节数组的大小
     * @param bytes1            arr1
     * @param bytes2            arr2
     * @return                  -1: 小于； 0：等于； 1：大于
     */
    public static int compare(byte[] bytes1, byte[] bytes2) {
        if (bytes1 == null || bytes2 == null) {
            throw new RuntimeException("byte arr is null!");
        }
        int res = 0;
        int len = Math.min(bytes1.length, bytes2.length);
        for (int i = 0; i < len; i++) {
            if (bytes1[i] == bytes2[i]) continue;
            if (bytes1[i] > bytes2[i]) return 1;
            else return -1;
        }
        if (bytes1.length > bytes2.length) {
            res = 1;
        } else if (bytes1.length < bytes2.length) {
            res = -1;
        }
        return res;
    }
}
