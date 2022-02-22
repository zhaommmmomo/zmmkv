package com.zmm.kv.file;

import com.google.protobuf.ByteString;
import com.zmm.kv.api.DBIterator;
import com.zmm.kv.api.Option;
import com.zmm.kv.lsm.BloomFilter;
import com.zmm.kv.lsm.MemTable;
import com.zmm.kv.lsm.Table;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author zmm
 * @date 2022/2/18 14:08
 */
public class SSTable extends Table {

    private static volatile AtomicInteger fileNum = new AtomicInteger(0);

    private int fid;
    private int level = 0;
    private int dataSize;
    private int size;
    private BloomFilter bloomFilter;

    public SSTable() {
        this.setFid();
    }

    public SSTable(int level) {
        this.setFid();
        this.level = level;
    }

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

            MappedByteBuffer mb = new RandomAccessFile(new File(option.getDir() + "\\" + ssTable.getFileName()), "rw").getChannel()
                    .map(FileChannel.MapMode.READ_WRITE, 0, option.getSstSize(0));

            // build
            DBIterator iterator = memTable.iterator();
            Entry entry;
            ssTable.bloomFilter = new BloomFilter(mLen, 0.01f);
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
                ssTable.bloomFilter.appendKey(key);

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

            // 可以直接获取memTable的（还没加上去）
            ssTable.maxKeyScore = Utils.calcScore(key);
            ssTable.minKeyScore = Utils.calcScore(minKey);

            // write index
            byte[] indexs = Index.newBuilder()
                                .setBloomFilter(ByteString.copyFrom(ssTable.bloomFilter.toByteArray()))
                                .setMaxKey(ssTable.maxKeyScore)
                                .setMinKey(ssTable.minKeyScore)
                                .build().toByteArray();
            mb.put(indexs);

            // 填充
            int fillLen = mb.capacity() - mb.position() - 8;
            if (fillLen > 0) {
                mb.put(new byte[fillLen]);
            }

            ssTable.dataSize = 4 * 1024 * count;

            mb.put(buildHeader(ssTable.dataSize, indexs.length));
            ssTable.size = count + indexs.length + 8;

            return ssTable;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 压缩创建的新的sst
     * @param mergeFile         需要merge的sst
     * @param mergeFiles        下一层有重叠的sst
     * @param dataSize          需要merge的sst的dataSize
     * @param dataSizes         重叠的sst的dataSize
     * @param option            配置类
     * @return                  新的sst
     */
    public static List<SSTable> build(File mergeFile,
                                      List<File> mergeFiles,
                                      int dataSize,
                                      List<Integer> dataSizes,
                                      int level,
                                      Option option) {
        List<SSTable> res = new ArrayList<>();
        SSTable ssTable = new SSTable(level);
        // 写新sst的fc
        FileChannel fc;
        ssTable.bloomFilter = new BloomFilter(10000000, 0.01f);
        int newSSTSize = 0;
        byte[] startKey = null;
        byte[] endKey = null;

        // 读旧sst的fc
        FileChannel fc1;
        FileChannel fc2;
        int size1 = 0;
        int size2 = 0;
        try {
            fc = new RandomAccessFile(new File(option.getDir() + "\\" + ssTable.getFileName()), "rw").getChannel();
            fc1 = new RandomAccessFile(mergeFile, "rw").getChannel();
            // n层的sst的block
            Block block1;
            // n + 1层的sst的block
            Block block2;
            // 新sst的block
            Block.Builder builder = Block.newBuilder();

            List<Entry> entries1;
            List<Entry> entries2;
            for (int k = 0; k < mergeFiles.size(); k++) {

                File nextMergeFile = mergeFiles.get(k);

                fc2 = new RandomAccessFile(nextMergeFile, "rw").getChannel();

                int i = 0;
                int j = 0;
                do {
                    // 获取最前面的block
                    block1 = readBlock(fc1, size1);
                    block2 = readBlock(fc2, size2);

                    entries1 = block1.getEntryList();
                    entries2 = block2.getEntryList();

                    while (i < entries1.size() && j < entries2.size()) {
                        int compare = Utils.compare(entries1.get(i).getKey().toByteArray(),
                                entries2.get(j).getKey().toByteArray());
                        Entry entry;
                        if (compare == 1) {
                            // 如果下一层的key小与当前key
                            entry = entries2.get(i);
                            j++;
                        } else {
                            entry = entries1.get(i);
                            if (compare == 0) {
                                j++;
                            }
                            i++;
                        }

                        endKey = entry.getKey().toByteArray();
                        if (startKey == null) startKey = endKey;

                        if (builder.build().getSerializedSize() + entry.getSerializedSize() > 4094) {
                            fill(builder, fc);
                            builder = Block.newBuilder();
                            newSSTSize += 4096;

                            // 如果新sst文件大小超过阈值
                            if (newSSTSize > option.getSstSize(level)) {

                                doBuildSSTable(ssTable, fc, newSSTSize, startKey, endKey, builder);

                                res.add(ssTable);
                                ssTable = new SSTable(level);
                                newSSTSize = 0;
                                ssTable.bloomFilter = new BloomFilter(10000000, 0.01f);
                            }
                        }

                        ssTable.bloomFilter.appendKey(endKey);

                        if (builder.getBaseKey().size() == 0) {
                            builder.setBaseKey(entry.getKey());
                        }

                        builder.addEntry(entry);
                    }

                    if (i == entries1.size()) {
                        // 如果是第n层的这个block读完了
                        size1 += 4096;
                    }
                    if (j == entries2.size()) {
                        size2 += 4096;
                    }
                    if (size1 == dataSize && size2 == dataSizes.get(k)) {
                        // 如果第n层的sst与n + 1层的sst同时读完，说明都读完了。
                        if (builder.getBaseKey().size() != 0) {
                            doBuildSSTable(ssTable, fc, newSSTSize, startKey, endKey, builder);
                        }
                        return res;
                    } else if (size1 == dataSize) {
                        // 如果是第n层的读完了，这代表n + 1层是最后一个了且还没读完
                        fc1 = fc2;
                        size1 = size2;
                        dataSize = dataSizes.get(k);
                        break;
                    } else if (size2 == dataSizes.get(k)) {
                        // 如果是第n + 1层读完了，下一个file
                        break;
                    }
                } while (true);
            }

            // 继续读未读完的sst
            while (size1 < dataSize) {
                block1 = readBlock(fc1, size1);
                entries1 = block1.getEntryList();
                for (Entry entry : entries1) {

                    endKey = entry.getKey().toByteArray();
                    if (startKey == null) startKey = endKey;

                    if (builder.build().getSerializedSize() + entry.getSerializedSize() > 4094) {
                        fill(builder, fc);
                        builder = Block.newBuilder();
                        newSSTSize += 4096;

                        // 如果新sst文件大小超过阈值
                        if (newSSTSize > option.getSstSize(level)) {

                            doBuildSSTable(ssTable, fc, newSSTSize, startKey, endKey, builder);

                            res.add(ssTable);
                            ssTable = new SSTable(level);
                            newSSTSize = 0;
                            ssTable.bloomFilter = new BloomFilter(10000000, 0.01f);
                        }
                    }

                    ssTable.bloomFilter.appendKey(endKey);

                    if (builder.getBaseKey().size() == 0) {
                        builder.setBaseKey(entry.getKey());
                    }

                    builder.addEntry(entry);
                }

                size1 += 4096;
            }

            if (builder.getBaseKey().size() != 0) {
                doBuildSSTable(ssTable, fc, newSSTSize, startKey, endKey, builder);
                res.add(ssTable);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return res;
    }

    private static void doBuildSSTable(SSTable ssTable, FileChannel fc, int newSSTSize, byte[] startKey, byte[] endKey, Block.Builder builder) throws IOException {
        fill(builder, fc);
        newSSTSize += 4096;
        ssTable.minKeyScore = Utils.calcScore(startKey);
        ssTable.maxKeyScore = Utils.calcScore(endKey);

        // 构建index
        byte[] indexs = Index.newBuilder()
                .setBloomFilter(ByteString.copyFrom(ssTable.bloomFilter.toByteArray()))
                .setMaxKey(ssTable.maxKeyScore)
                .setMinKey(ssTable.minKeyScore)
                .build().toByteArray();
        fc.write(ByteBuffer.wrap(indexs));

        // 构建header
        fc.write(ByteBuffer.wrap(buildHeader(newSSTSize, indexs.length)));
    }

    /**
     * 会在4kb中预留2b来表示fillLen
     */
    private static void fill(Block.Builder builder, MappedByteBuffer mb) {
        byte[] bytes = builder.build().toByteArray();
        mb.put(bytes);
        bytes = getFillBytes(bytes);
        mb.put(bytes);
    }

    /**
     * 会在4kb中预留2b来表示fillLen
     */
    private static void fill(Block.Builder builder, FileChannel fc) throws IOException {
        byte[] bytes = builder.build().toByteArray();
        fc.write(ByteBuffer.wrap(bytes));
        bytes = getFillBytes(bytes);
        fc.write(ByteBuffer.wrap(bytes));
    }

    private static byte[] getFillBytes(byte[] bytes) {
        bytes = new byte[4096 - bytes.length];
        int l = bytes.length - 2;
        Arrays.fill(bytes, (byte) 0);
        bytes[l] = (byte) (((l >> 8) & 255) - 128);
        bytes[l + 1] = (byte) ((l & 255) - 128);
        return bytes;
    }

    private static byte[] buildHeader(int dataSize, int indexSize) {
        byte[] bytes = new byte[8];
        // write header
        // dataSize
        bytes[0] = (byte) (((dataSize >> 24) & 255) - 128);
        bytes[1] = (byte) (((dataSize >> 16) & 255) - 128);
        bytes[2] = (byte) (((dataSize >> 8) & 255) - 128);
        bytes[3] = (byte) ((dataSize & 255) - 128);

        // indexLen
        bytes[4] = (byte) (((indexSize >> 16) & 255) - 128);
        bytes[5] = (byte) (((indexSize >> 8) & 255) - 128);
        bytes[6] = (byte) ((indexSize & 255) - 128);

        // type
        // bytes[7]

        return bytes;
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

    public BloomFilter bloomFilter() {
        return bloomFilter;
    }

    public int size() {
        return size;
    }

    public int dataSize() {
        return dataSize;
    }
}
