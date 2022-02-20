package com.zmm.kv.file;

import com.google.protobuf.ByteString;
import com.zmm.kv.api.DBIterator;
import com.zmm.kv.api.Option;
import com.zmm.kv.lsm.BloomFilter;
import com.zmm.kv.lsm.MemTable;
import com.zmm.kv.pb.Block;
import com.zmm.kv.pb.Entry;
import com.zmm.kv.pb.Index;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author zmm
 * @date 2022/2/18 14:08
 */
public class SSTable {

    private static volatile AtomicInteger fileNum = new AtomicInteger(0);

    private int fid;
    private int level = 0;

    /**
     * 通过memTable创建的sst
     * @param memTable          内存表
     * @param option            配置
     * @return                  新的sst
     */
    public static SSTable build(MemTable memTable, Option option) {

        int mLen = memTable.len();
        if (mLen == 0) return null;

        try {
            SSTable ssTable = new SSTable();
            ssTable.setFid();

            MappedByteBuffer mb = new RandomAccessFile(new File(option.getDir() + "\\" + ssTable.getFileName()), "rw").getChannel()
                    .map(FileChannel.MapMode.READ_WRITE, 0, option.getSstSize(0));

            // build
            DBIterator iterator = memTable.iterator();
            Entry entry;
            BloomFilter bloomFilter = new BloomFilter(mLen, 0.01f);
            byte[] minKey = null;
            byte[] maxKey = null;
            int size = 0;
            int count = 0;
            Block.Builder builder = Block.newBuilder();
            while (iterator.hasNext()) {
                entry = iterator.next();
                // 判断是否需要进行kv分离
                if (entry.getValue().size() > option.getValueSize()) {
                    // 进行kv分离
                }

                // 添加到bloom
                bloomFilter.appendKey(entry.getKey().toByteArray());

                // 如果当前block剩下的容量不足以放这个key
                // 预留2个字节来表示填充位的len
                if (builder.build().getSerializedSize() + entry.getSerializedSize() > 4094) {
                    fill(builder, mb);
                    count++;
                    builder = Block.newBuilder();
                    size = 0;
                }

                ByteString key = entry.getKey();
                // 如果这个block是新开的
                if (size == 0) {
                    builder.setBaseKey(key);
                    if (minKey == null) {
                        // 判断这个block是不是第一个block
                        minKey = key.toByteArray();
                    }
                    size += builder.build().getSerializedSize();
                }

                if (maxKey == null) {
                    maxKey = key.toByteArray();
                }

                // 将entry添加到block中
                builder.addEntry(entry);

                size += entry.getSerializedSize();
            }

            // 判断最后一个block是否写入
            if (builder.getBaseKey().size() != 0) {
                fill(builder, mb);
                count++;
            }

            // write index
            byte[] indexs = Index.newBuilder()
                                .setBloomFilter(ByteString.copyFrom(bloomFilter.toByteArray()))
                                .setMaxKey(ByteString.copyFrom(maxKey))
                                .setMinKey(ByteString.copyFrom(minKey))
                                .build().toByteArray();
            mb.put(indexs);

            // write header
            // size
            count = 4 * 1024 * count + 8;
            byte[] bytes = new byte[4];
            bytes[3] = (byte) ((count & 255) - 128);
            bytes[2] = (byte) (((count >> 8) & 255) - 128);
            bytes[1] = (byte) (((count >> 16) & 255) - 128);
            bytes[0] = (byte) (((count >> 24) & 255) - 128);
            mb.put(bytes);

            // indexLen
            bytes = new byte[3];
            bytes[2] = (byte) ((indexs.length & 255) - 128);
            bytes[1] = (byte) (((indexs.length >> 8) & 255) - 128);
            bytes[0] = (byte) (((indexs.length >> 16) & 255) - 128);
            mb.put(bytes);

            // type
            mb.put((byte) 1);

            return ssTable;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 压缩创建的新的sst
     * @param ssTables          sst列表
     * @param option            配置
     * @return                  新的sst
     */
    public static SSTable build(List<SSTable> ssTables, Option option) {

        return null;
    }

    private static void fill(Block.Builder builder, MappedByteBuffer mb) {
        byte[] bytes = builder.build().toByteArray();
        mb.put(bytes);
        bytes = new byte[4096 - bytes.length];
        int l = bytes.length - 2;
        Arrays.fill(bytes, (byte) 0);
        bytes[l] = (byte) ((l & 255) - 128);
        bytes[l + 1] = (byte) ((l & 255) - 128);
        mb.put(bytes);
    }

    private void buildHeader() {

        //
        // type: 类型             1字节
        // indexLen: 索引长度      3byte
        // size: 整个sst的字节     4字节 可以表示1G多点的sst文件
    }

    public static void setFileNum(int _fileNum) {
        fileNum = new AtomicInteger(_fileNum);
    }

    private void setFid() {
        this.fid = fileNum.incrementAndGet();
    }

    public int getFid() {
        return fid;
    }

    public String getFileName() {
        return fid + ".sst";
    }

    public int level() {
        return level;
    }

    /**
     * 将该sst文件删除
     * @param dir               sst文件目录
     */
    public void remove(String dir) {
        new File(dir + "\\" + getFileName()).delete();
    }
}
