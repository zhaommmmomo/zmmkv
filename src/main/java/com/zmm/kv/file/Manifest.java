package com.zmm.kv.file;

import com.google.protobuf.ProtocolStringList;
import com.zmm.kv.lsm.BloomFilter;
import com.zmm.kv.pb.Index;
import com.zmm.kv.pb.Level;
import com.zmm.kv.pb.Levels;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
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

    public Manifest() {
        levels = new CopyOnWriteArrayList[6];
        for (int i = 0; i < 6; i++) {
            levels[i] = new CopyOnWriteArrayList<>();
        }
        fileLevelMap = new ConcurrentHashMap<>();
        fileIndexMap = new ConcurrentHashMap<>();
    }

    public void loadManifest(String dir) {
        this.manifest = new File(dir + "\\MANIFEST");
        this.dir = dir;
        if (manifest.exists()) {
            // 加载manifest中的内容
            read();
            // 验证manifest
            check();
        }
    }

    public boolean changeLevels(SSTable ssTable) {
        boolean flag = true;
        levels[ssTable.level()].add(
                new File(dir + "\\" + ssTable.getFileName()));
        count++;
        // 如果flush了3个sst，触发写write();
        if (count % 3 == 0 && (flag = write())) {
            // 删除最前面的3个wal文件
            Wal.delPreWal(dir, 3);
        }
        return flag;
    }

    public boolean changeLevels(SSTable ssTable, List<File> files) {
        // merge操作直接触发write()。
        levels[ssTable.level()].add(
                new File(dir + "\\" + ssTable.getFileName()));
        for (File file : files) {
            levels[fileLevelMap.get(file)].remove(file);
            fileLevelMap.remove(file);
        }
        return write();
    }

    /**
     * 读取manifest内容
     */
    private void read() {
        try {
            List<Level> levels = Levels.parseFrom(
                    Files.readAllBytes(manifest.toPath())).getLevelsList();
            for (int i = 0; i < 6; i++) {
                ProtocolStringList level = levels.get(i).getLevelList();
                for (String filePath : level) {
                    File file = new File(filePath);
                    fileLevelMap.put(file, i);
                    this.levels[i].add(file);
                }
            }
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

    private void check() {
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
            // 读取header中的indexLen
            ByteBuffer buf = ByteBuffer.allocate(3);
            int size = (int) fc.size();
            fc.read(buf, size - 5);
            byte[] bytes = buf.array();
            int indexLen = ((bytes[0] + 128) << 16) +
                           ((bytes[1] + 128) << 8) +
                           (bytes[2] + 128);
            // 获取Index
            buf = ByteBuffer.allocate(indexLen);
            fc.read(buf, size - indexLen - 9);
            Index index = Index.parseFrom(buf.array());
            List<Object> list = new ArrayList<>();
            list.add(new BloomFilter(index.getBloomFilter().toByteArray()));
            list.add(index.getMinKey());
            list.add(index.getMaxKey());
            fileIndexMap.put(file, list);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
