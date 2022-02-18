package com.zmm.kv.lsm;

import com.zmm.kv.api.DBIterator;
import com.zmm.kv.api.Option;
import com.zmm.kv.pb.Entry;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author zmm
 * @date 2022/2/17 19:20
 */
public class LSM {

    /** 可变内存表 */
    private MemTable menTable;
    /** 不可变内存表 */
    private List<MemTable> immutables;
    private final Option option;

    public LSM(Option option) {
        this.option = option;
        menTable = new SkipList();
        immutables = new CopyOnWriteArrayList<>();

        ExecutorService executor = Executors.newSingleThreadExecutor();
    }

    public boolean put(Entry entry) {

        // 判断是否需要将可变内存表变为不可变内存表
        if (menTable.size() > option.getMemSize()) {
            immutables.add(menTable);
            menTable = new SkipList();
        }

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

            // TODO: 2022/2/18 不可变内存表中如果没查询到，去磁盘查
        }
        return res;
    }

    public boolean del(byte[] key)  {
        return menTable.del(key);
    }

    public DBIterator iterator() {

        // TODO: 2022/2/18 应该将所有数据遍历

        return menTable.iterator();
    }
}
