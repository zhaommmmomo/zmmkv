package com.zmm.kv.lsm;

import com.zmm.kv.api.DBIterator;
import com.zmm.kv.entry.Entry;

import java.util.ArrayList;
import java.util.List;

/**
 * @author zmm
 * @date 2022/2/17 19:20
 */
public class SkipList extends MemTable{

    @Override
    public boolean put(byte[] key, byte[] val) {
        return super.put(key, val);
    }

    @Override
    public byte[] get(byte[] key) {
        return super.get(key);
    }

    @Override
    public boolean del(byte[] key) {
        return super.del(key);
    }

    @Override
    public DBIterator iterator() {
        return super.iterator();
    }

    class Node {
        private List<Node> levels = new ArrayList<>();
        private Entry entry;
        private float score;
    }
}
