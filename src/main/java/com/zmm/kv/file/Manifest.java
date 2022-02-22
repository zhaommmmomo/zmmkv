package com.zmm.kv.file;

import com.google.protobuf.ProtocolStringList;
import com.zmm.kv.api.Option;
import com.zmm.kv.lsm.BloomFilter;
import com.zmm.kv.pb.*;
import com.zmm.kv.util.Utils;
import com.zmm.kv.worker.Merger;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * 存储磁盘中的level层级、索引信息
 * @author zmm
 * @date 2022/2/19 17:43
 */
public class Manifest {

    private File manifest;
    private final List<File>[] levels;
    private final Map<File, Integer> fileLevelMap;
    private final Map<File, List<Object>> fileIndexMap;
    private int count = 0;
    private String dir;
    private Merger merger;

    public Manifest(Option option) {
        levels = new CopyOnWriteArrayList[6];
        for (int i = 0; i < 6; i++) {
            levels[i] = new CopyOnWriteArrayList<>();
        }
        fileLevelMap = new ConcurrentHashMap<>();
        fileIndexMap = new ConcurrentHashMap<>();
        merger = new Merger(this, option);
        this.dir = option.getDir();
    }

    public void loadManifest() {
        this.manifest = new File(dir + "\\MANIFEST");
        this.dir = dir;
        // 加载manifest中的内容
        read();
        // 验证manifest
        checkManifest();
    }

    public byte[] get(byte[] key) {
        // 从l0 -> l5遍历sst，如果找到了就直接返回

        // 计算key的score
        float score = Utils.calcScore(key);

        List<Object> index;
        FileChannel fc;
        File file;
        for (int i = 0; i < 6; i++) {
            // 从右向左遍历，因为最新的sst总是在右边添加
            for (int j = levels[i].size() - 1; j >= 0; j--) {
                file = levels[i].get(j);
                index = fileIndexMap.get(levels[i].get(j));
                BloomFilter bf = (BloomFilter) index.get(0);
                float minKey = (float) index.get(1);
                float maxKey = (float) index.get(2);

                if (minKey <= score && maxKey >= score && bf.containKey(key)) {
                    // 如果key在该sst的index匹配成功
                    // 获取dataSize
                    int size = (int) index.get(3);
                    // 获取block的个数
                    int blockCount = size / 4096;
                    try {
                        fc = new RandomAccessFile(file, "rw").getChannel();

                        Block mBlock;
                        int l = 0;
                        int r = blockCount - 1;
                        int mid = 0;
                        float midScore;
                        byte[] midKey;
                        out: while (l < r) {
                            mid = l + (r - l + 1) / 2;
                            mBlock = SSTable.readBlock(fc, mid * 4096);
                            midKey = mBlock.getBaseKey().toByteArray();
                            midScore = Utils.calcScore(midKey);
                            if (score == midScore) {
                                if (Utils.compare(key, midKey) != -1) {
                                    do {
                                        // 说明key可能在这个block中
                                        List<Entry> entries = mBlock.getEntryList();
                                        for (Entry entry : entries) {
                                            int compare = Utils.compare(key, entry.getKey().toByteArray());
                                            if (compare == 0) {
                                                // 找到了，直接返回
                                                return entry.getValue().toByteArray();
                                            } else if (compare == -1) {
                                                // 需要查找的key比这个key小，说明这个sst中不存在
                                                break out;
                                            }
                                        }
                                        // 如果没在的话，就说明可能存在多个连续的baseKey的前缀相同的block
                                        mid++;
                                        if (mid == blockCount) {
                                            // 如果遍历到这个sst的最后一个block了都还没找到就退出
                                            break out;
                                        }
                                        mBlock = SSTable.readBlock(fc, mid * 4096);
                                    } while (true);
                                }
                                r = mid - 1;
                            } else if (score > midScore) {
                                l = mid;
                            } else {
                                r = mid - 1;
                            }
                        }

                        // 判断这个block是否有效
                        if (r >= 0 && r < blockCount) {
                            mBlock = SSTable.readBlock(fc, r * 4096);
                            midKey = mBlock.getBaseKey().toByteArray();
                            if (Utils.compare(key, midKey) != -1) {
                                // 可能在这个block
                                for (Entry entry : mBlock.getEntryList()) {
                                    int compare = Utils.compare(key, entry.getKey().toByteArray());
                                    if (compare == 0) {
                                        // 找到了，直接返回
                                        return entry.getValue().toByteArray();
                                    } else if (compare == -1) {
                                        // 需要查找的key比这个key小，说明这个sst中不存在
                                        break;
                                    }
                                }
                            }
                        }

                        // 如果在这个sst中还是没找到
                        // L0层：继续遍历
                        // Ln层：下一层开始遍历
                        if (i > 0) {
                            break;
                        }
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }
        return null;
    }

    public boolean changeLevels(SSTable ssTable) {
        boolean flag = true;
        File sst = new File(dir + "\\" + ssTable.getFileName());
        levels[0].add(sst);
        putSSTIndex(ssTable, sst);

        count++;
        // 如果flush了3个sst，触发写write();
        if (count % 3 == 0 && (flag = write())) {
            // 删除最前面的3个wal文件
            Wal.delPreWal(dir, 3);
        }

        // 判断是否需要触发merge
        if (levels[0].size() > 20) {
            merger.merge(0);
        }
        return flag;
    }

    public boolean changeLevels(List<SSTable> newSSTables,
                                File oldSSt,
                                List<File> mergeFiles,
                                int level) {
        // merge操作直接触发write()。
        int l = level + 1;
        File sst;
        for (SSTable ssTable : newSSTables) {
            sst = new File(dir + "\\" + ssTable.getFileName());
            levels[l].add(sst);
            putSSTIndex(ssTable, sst);
        }

        levels[level].remove(oldSSt);
        fileLevelMap.remove(oldSSt);
        fileIndexMap.remove(oldSSt);

        for (File file : mergeFiles) {
            levels[l].remove(file);
            fileLevelMap.remove(file);
            fileIndexMap.remove(file);
        }

        if (l < 5 && levels[l].size() > 20) {
            merger.merge(l);
        }
        return write();
    }

    private void putSSTIndex(SSTable ssTable, File sst) {
        List<Object> list = new ArrayList<>(5);
        list.add(ssTable.bloomFilter());
        list.add(ssTable.minKeyScore);
        list.add(ssTable.maxKeyScore);
        list.add(ssTable.dataSize());
        list.add(ssTable.size());
        fileIndexMap.put(sst, list);
    }

    /**
     * 读取manifest内容
     */
    private void read() {
        try {
            List<Level> levels = Levels.parseFrom(
                    Files.readAllBytes(manifest.toPath())).getLevelsList();
            String fileName = "0.sst";
            for (int i = 0; i < 6; i++) {
                ProtocolStringList level = levels.get(i).getLevelList();
                for (String filePath : level) {
                    File file = new File(filePath);
                    fileLevelMap.put(file, i);
                    this.levels[i].add(file);
                    fileName = file.getName();
                }
            }
            SSTable.setFileNum(Integer.parseInt(fileName.substring(0, fileName.indexOf('.'))));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 更新Manifest文件
     * @return              true / false
     */
    private boolean write() {
        try (FileOutputStream out = new FileOutputStream(manifest)) {
            Levels.Builder builder = Levels.newBuilder();
            for (int i = 0; i < 6; i++) {
                Level.Builder levelBuilder = Level.newBuilder();
                for (File sst : levels[i]) {
                    levelBuilder.addLevel(sst.getAbsolutePath());
                }
                builder.addLevels(levelBuilder.build());
            }
            out.write(builder.build().toByteArray());
            return true;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    private void checkManifest() {
        File[] files = new File(dir).listFiles();
        for (File file : files) {
            if (file.getName().endsWith(".sst")) {
                // 判断manifest中是否包含了该sst
                if (fileLevelMap.containsKey(file)) {
                    // 读取sst，获取index段
                    loadSSTIndex(file);
                } else {
                    file.delete();
                }
            }
        }
    }

    private void loadSSTIndex(File file) {
        try {
            FileChannel fc = new RandomAccessFile(file, "rw").getChannel();
            // 读取header
            ByteBuffer buf = ByteBuffer.allocate(8);

            int size = (int) fc.size();
            fc.read(buf, size - 8);
            byte[] bytes = buf.array();
            int dataSize = ((bytes[0] + 128) << 24) +
                            ((bytes[1] + 128) << 16) +
                            ((bytes[2] + 128) << 8) +
                            (bytes[3] + 128);
            int indexLen = ((bytes[4] + 128) << 16) +
                            ((bytes[5] + 128) << 8) +
                            (bytes[6] + 128);

            // 获取Index
            buf = ByteBuffer.allocate(indexLen);
            fc.read(buf, dataSize);
            Index index = Index.parseFrom(buf.array());
            List<Object> list = new ArrayList<>(5);
            list.add(new BloomFilter(index.getBloomFilter().toByteArray()));
            list.add(index.getMinKey());
            list.add(index.getMaxKey());
            list.add(dataSize);
            list.add(dataSize + indexLen + 8);
            fileIndexMap.put(file, list);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public List<File>[] getLevels() {
        return levels;
    }

    public Map<File, List<Object>> getFileIndexMap() {
        return fileIndexMap;
    }
}
