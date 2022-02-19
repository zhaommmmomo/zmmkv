package com.zmm.kv.file;

import com.zmm.kv.api.Option;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * @author zmm
 * @date 2022/2/18 13:20
 */
public class Wal {

    private int fileNum = 0;
    private int size = 0;
    private FileChannel fc;
    private Option option;

    public Wal(Option option) {
        this.option = option;
        newWalFile();
    }

    public Wal(Option option, int num) {
        this.option = option;
        this.fileNum = num;
        newWalFile();
    }

    public void append(byte[] key) {
        append(key, null);
    }

    public void append(byte[] key, byte[] value) {

        if (size > option.getWalSize()) {
            // 如果wal的size到达了最大值
            // 创建新的wal文件
            newWalFile();
        }

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
            size += len;
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void newWalFile() {
        size = 0;
        try {
            fc = new RandomAccessFile(new File(option.getDir() + "\\" + incFileNum() + ".wal"), "rw").getChannel();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    private int incFileNum() {
        ++fileNum;
        fileNum = fileNum == Integer.MAX_VALUE ? 1 : fileNum;
        return fileNum;
    }

    public static void main(String[] args) throws Exception {
        Wal wal = new Wal(new Option());
        wal.append("aaaaaa".getBytes(), "aaaaaa".getBytes());
        //Option option = new Option();
        //FileChannel ff = new RandomAccessFile(new File(option.getDir() + "\\" + 1 + ".wal"), "rw").getChannel();
        //ByteBuffer buf = ByteBuffer.allocate(20);
        //ff.read(buf);
        //byte[] bytes = buf.array();
        //for (byte b : bytes) {
        //    System.out.print(b + " ");
        //}

    }
}
