package com.zmm.kv.file;

import com.zmm.kv.api.Option;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.TimeUnit;

/**
 * @author zmm
 * @date 2022/2/18 13:20
 */
public class Wal {

    private static int preNum = 0;
    private static int newNum = 0;
    private static FileChannel fc;
    private final Option option;

    public Wal(Option option) {
        this.option = option;
    }

    public Wal(Option option, int _preNum, int _lastNum) {
        this.option = option;
        preNum = _preNum;
        newNum = _lastNum;
    }

    public void append(byte[] key) {
        append(key, null);
    }

    public void append(byte[] key, byte[] value) {

        if (fc == null) newWalFile(option.getDir());

        int len = 4 + key.length + (value == null ? 0 :value.length);
        byte[] bytes = new byte[len];
        // 将len的前8位和后8位分别用一个字节记录
        // 表示范围0 ~ 65535个字节
        bytes[0] = (byte) ((key.length >> 8 & 255) - 128);
        bytes[1] = (byte) ((key.length & 255) - 128);
        if (value == null) {
            bytes[2] = bytes[3] = -128;
        } else {
            bytes[2] = (byte) ((value.length >> 8 & 255) - 128);
            bytes[3] = (byte) ((value.length & 255) - 128);
            System.arraycopy(value, 0, bytes, key.length + 4, value.length);
        }
        System.arraycopy(key, 0, bytes, 4, key.length);

        try {
            // 写入wal中
            fc.write(ByteBuffer.wrap(bytes));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void newWalFile(String dir) {
        try {
            if (fc != null) {
                fc.close();
            }
            fc = new RandomAccessFile(new File(dir + "\\" + (++newNum) + ".wal"), "rw").getChannel();
        } catch (IOException e) {
            e.printStackTrace();
        }

        //if (newNum - preNum > 100) {
        //    // 如果wal文件超过100个，清理最前面的一个
        //    delPreWal(dir);
        //}
    }

    public static void delPreWal(String dir, int count) {
        while (count > 0) {
            new File(dir + "\\" + (++preNum) + ".wal").delete();
            count--;
        }
    }
}
