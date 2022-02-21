package com.zmm.kv.file;

import com.google.protobuf.ByteString;
import com.zmm.kv.api.DBIterator;
import com.zmm.kv.api.Option;
import com.zmm.kv.lsm.BloomFilter;
import com.zmm.kv.lsm.MemTable;
import com.zmm.kv.pb.Block;
import com.zmm.kv.pb.Entry;
import com.zmm.kv.pb.Index;
import com.zmm.kv.util.Utils;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
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
            byte[] key = null;
            int size = 0;
            int count = 0;
            Block.Builder builder = Block.newBuilder();
            while (iterator.hasNext()) {
                entry = iterator.next();
                // 判断是否需要进行kv分离
                if (entry.getValue().size() > option.getValueSize()) {
                    // 进行kv分离
                }

                key = entry.getKey().toByteArray();

                // 添加到bloom
                bloomFilter.appendKey(key);

                // 如果当前block剩下的容量不足以放这个key
                // 预留2个字节来表示填充位的len
                if (builder.build().getSerializedSize() + entry.getSerializedSize() > 4094) {
                    fill(builder, mb);
                    count++;
                    builder = Block.newBuilder();
                    size = 0;
                }

                // 如果这个block是新开的
                if (size == 0) {
                    builder.setBaseKey(entry.getKey());
                    size += builder.build().getSerializedSize();
                }

                // 将entry添加到block中
                builder.addEntry(entry);

                size += entry.getSerializedSize();

                if (minKey == null) minKey = key;
            }

            // 判断最后一个block是否写入
            if (builder.getBaseKey().size() != 0) {
                fill(builder, mb);
                count++;
            }

            // write index
            byte[] indexs = Index.newBuilder()
                                .setBloomFilter(ByteString.copyFrom(bloomFilter.toByteArray()))
                                .setMaxKey(Utils.calcScore(key))
                                .setMinKey(Utils.calcScore(minKey))
                                .build().toByteArray();
            mb.put(indexs);

            // 填充
            int fillLen = mb.capacity() - mb.position() - 8;
            if (fillLen > 0) {
                mb.put(new byte[fillLen]);
            }

            count = 4 * 1024 * count;
            byte[] bytes = new byte[8];
            // write header
            // dataSize
            bytes[0] = (byte) (((count >> 24) & 255) - 128);
            bytes[1] = (byte) (((count >> 16) & 255) - 128);
            bytes[2] = (byte) (((count >> 8) & 255) - 128);
            bytes[3] = (byte) ((count & 255) - 128);

            // indexLen
            bytes[4] = (byte) (((indexs.length >> 16) & 255) - 128);
            bytes[5] = (byte) (((indexs.length >> 8) & 255) - 128);
            bytes[6] = (byte) ((indexs.length & 255) - 128);

            // type
            //bytes[7] = 1;

            mb.put(bytes);

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

    /**
     * 会在4kb中预留2b来表示fillLen
     */
    private static void fill(Block.Builder builder, MappedByteBuffer mb) {
        byte[] bytes = builder.build().toByteArray();
        mb.put(bytes);
        bytes = new byte[4096 - bytes.length];
        int l = bytes.length - 2;
        Arrays.fill(bytes, (byte) 0);
        bytes[l] = (byte) (((l >> 8) & 255) - 128);
        bytes[l + 1] = (byte) ((l & 255) - 128);
        mb.put(bytes);
    }

    public static void setFileNum(int _fileNum) {
        fileNum = new AtomicInteger(_fileNum);
    }

    public static Block readBlock(FileChannel fc, int position) {
        try {
            byte[] buf = new byte[4096];
            fc.read(ByteBuffer.wrap(buf), position);
            // 获取填充字段的长度
            int fillLen = ((buf[buf.length - 2] + 128) << 8) +
                            buf[buf.length - 1] + 128;
            return Block.parseFrom(
                    ByteString.copyFrom(buf, 0, buf.length - fillLen - 2));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
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
