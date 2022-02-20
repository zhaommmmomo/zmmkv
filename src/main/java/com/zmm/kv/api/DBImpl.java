package com.zmm.kv.api;

import com.google.protobuf.ByteString;
import com.zmm.kv.file.Wal;
import com.zmm.kv.lsm.LSM;
import com.zmm.kv.pb.Entry;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/**
 * @author zmm
 * @date 2022/2/17 19:15
 */
public class DBImpl implements DB{

    private final LSM lsm;
    private Option option;
    private Wal wal;

    public DBImpl() {
        option = new Option();
        lsm = new LSM(option);

        init();
    }

    public DBImpl(Option option) {
        this.option = option;
        lsm = new LSM(option);

        init();
    }

    private void init() {

        File file = new File(option.getDir());
        List<File> wals = new ArrayList<>();
        if (file.exists()) {
            // 加载wal文件
            String name;
            for (File f : Objects.requireNonNull(file.listFiles())) {
                name = f.getName();
                if (name.endsWith("wal")) {
                    wals.add(f);
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
                            del(buf.array());
                        } else {
                            Entry.Builder builder = Entry.newBuilder();
                            builder.setKey(ByteString.copyFrom(buf.array()));
                            buf = ByteBuffer.allocate(valueLen);
                            fc.read(buf);
                            builder.setValue(ByteString.copyFrom(buf.array()));
                            lsm.put(builder.build());
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

    public boolean put(byte[] key, byte[] value) {

        // 写wal文件
        wal.append(key, value);

        return lsm.put(Entry.newBuilder()
                            .setKey(ByteString.copyFrom(key))
                            .setValue(ByteString.copyFrom(value))
                            .build());
    }

    public byte[] get(byte[] key) {
        return lsm.get(key);
    }

    public boolean del(byte[] key) {

        // 写wal
        wal.append(key);

        return lsm.del(key);
    }

    public DBIterator iterator() {
        return lsm.iterator();
    }

    public void close() {

    }
}
