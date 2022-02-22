package com.zmm.kv.lsm;

import com.google.protobuf.ByteString;
import com.zmm.kv.api.DBIterator;
import com.zmm.kv.api.Option;
import com.zmm.kv.api.Range;
import com.zmm.kv.file.Wal;
import com.zmm.kv.pb.Entry;
import com.zmm.kv.worker.Flusher;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * @author zmm
 * @date 2022/2/17 19:20
 */
public class LSM {

    /** 可变内存表 */
    private MemTable menTable;
    /** 不可变内存表 */
    private final List<MemTable> immutables;
    private final Option option;
    private LevelManager levelManager;
    private Wal wal;

    private GlobalIndex globalIndex;

    private final Flusher flusher;

    public LSM(Option option) {
        this.option = option;
        menTable = new SkipList();
        immutables = new CopyOnWriteArrayList<>();
        levelManager = new LevelManager(option);
        flusher = new Flusher(option, levelManager);

        init();
    }

    private void init() {

        File file = new File(option.getDir());
        List<File> wals = new ArrayList<>();
        if (file.exists()) {
            // 加载文件
            String name;
            for (File f : Objects.requireNonNull(file.listFiles())) {
                name = f.getName();
                if (name.endsWith("wal")) {
                    wals.add(f);
                } else if ("MANIFEST".equals(name)) {
                    levelManager.loadManifest();
                }
            }
        }

        // 初始化wal
        if (wals.size() != 0) {
            recovery(wals);
            String preFid = wals.get(0).getName();
            String lastFid = wals.get(wals.size() - 1).getName();
            wal = new Wal(option,
                    Integer.parseInt(preFid.substring(0, preFid.length() - 4)),
                    Integer.parseInt(lastFid.substring(0, lastFid.length() - 4)));
        } else {
            wal = new Wal(option);
        }
    }

    private void recovery(List<File> wals) {

        // 先对wal文件进行排序
        wals.sort(Comparator.comparing(File::getName));
        try {
            FileChannel fc;
            for (File walFile : wals) {

                fc = new RandomAccessFile(walFile, "rw").getChannel();
                // 如果是空wal文件
                if (fc.size() == 0) break;
                ByteBuffer buf;
                int end;
                int keyLen = 0;
                int valueLen = 0;
                byte[] bytes;
                while (true) {
                    if (keyLen != 0) {
                        // 如果是data区
                        buf = ByteBuffer.allocate(keyLen);
                        fc.read(buf);

                        if (valueLen == 0) {
                            menTable.del(buf.array());
                        } else {
                            Entry.Builder builder = Entry.newBuilder();
                            builder.setKey(ByteString.copyFrom(buf.array()));
                            buf = ByteBuffer.allocate(valueLen);
                            fc.read(buf);
                            builder.setValue(ByteString.copyFrom(buf.array()));
                            if (menTable.size() > option.getMemSize()) {
                                immutables.add(menTable);
                                menTable = new SkipList();
                                // 触发flush操作
                                flusher.flush(immutables);
                            }
                            menTable.put(builder.build());
                        }

                        keyLen = 0;
                        valueLen = 0;
                    } else {
                        buf = ByteBuffer.allocate(4);
                        end = fc.read(buf);
                        if (end == -1) break;
                        bytes = buf.array();
                        // header
                        keyLen = ((bytes[0] + 128) << 8) + bytes[1] + 128;
                        valueLen = ((bytes[2] + 128) << 8) + bytes[3] + 128;
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public boolean put(Entry entry) {

        // 判断是否需要将可变内存表变为不可变内存表
        if (menTable.size() > option.getMemSize()) {
            immutables.add(menTable);
            menTable = new SkipList();

            // 创建新的wal文件
            Wal.newWalFile(option.getDir());

            // 触发flush操作
            flusher.flush(immutables);
        }

        // 写wal文件
        wal.append(entry.getKey().toByteArray(), entry.getValue().toByteArray());

        return menTable.put(entry);
    }

    public byte[] get(byte[] key) {
        // 先在可变内存表中查询
        byte[] res = menTable.get(key);
        if (res == null) {
            // 如果在可变内存表中没查询到，去不可变内存表中查
            for (int i = immutables.size() - 1; i >= 0; i--) {
                res = immutables.get(i).get(key);
                // 如果查询到了，直接返回
                if (res != null) return res;
            }

            // 不可变内存表中如果没查询到，去磁盘查
            res = levelManager.get(key);
        }
        return res;
    }

    public boolean del(byte[] key)  {
        // 写wal
        wal.append(key);

        return menTable.del(key);
    }

    public Range range(byte[] startKey, byte[] endKey) {
        

        return null;
    }

    public DBIterator iterator() {

        // TODO: 2022/2/18 应该将所有数据遍历

        return menTable.iterator();
    }
}
