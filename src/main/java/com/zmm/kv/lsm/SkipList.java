package com.zmm.kv.lsm;

import com.google.protobuf.ByteString;
import com.zmm.kv.api.DBIterator;
import com.zmm.kv.pb.Entry;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author zmm
 * @date 2022/2/17 19:20
 */
public class SkipList extends MemTable{

    private int level = 0;
    private int length = 0;
    private int size = 0;
    private final Node header = new Node();

    private final Lock lock = new ReentrantLock();

    @Override
    public boolean put(Entry entry) {
        return put(entry, 0);
    }

    /**
     * @param entry             entry
     * @param type              0：put； 1：del
     * @return                  true / false
     */
    private boolean put(Entry entry, int type) {
        byte[] key = entry.getKey().toByteArray();
        // 计算score
        float score = calcScore(key);
        // 前继节点
        Node[] preNode = new Node[level];
        // 找插入的位置
        Node p = header;
        // 竖着找
        // 遍历节点p的levels
        List<Node> levels = p.levels;
        Node next;
        for (int i = level - 1; i >= 0; i--) {

            // 获取该层的next节点
            next = levels.get(i);
            // 横向找，直到下一个分数大于等于当前分数或者下一个为null
            while (next != null) {

                // 与当前key进行比较
                int flag = compare(score, key, next);
                if (flag == -1) {
                    // 下一层
                    break;
                } else if (flag == 0) {
                    // 相同，直接修改并返回
                    size += entry.toByteArray().length - next.entry.toByteArray().length;
                    next.entry = entry;
                    return true;
                }

                // 下一个节点
                p = next;
                levels = p.levels;
                next = levels.get(i);
            }

            // 添加前继节点
            preNode[i] = p;
        }

        // 如果是删除
        if (type == 1) return false;

        // 构建新节点
        Node newNode = new Node(entry, score);
        // 获取新节点层数
        int cLevel = randLevel();
        int minLevel = Math.min(cLevel, level);
        // 添加节点
        for (int i = 0; i < minLevel; i++) {
            Node swap = preNode[i].levels.get(i);
            preNode[i].levels.set(i, newNode);
            newNode.addLevel(swap);
        }
        // 判断是否需要增加层数
        while (cLevel > level) {
            // 将头节点层数增加
            header.levels.add(newNode);
            newNode.addLevel(null);
            level++;
        }
        length++;
        return true;
    }

    @Override
    public byte[] get(byte[] key) {
        float score = calcScore(key);
        List<Node> levels = header.levels;
        for (int i = level - 1; i >= 0; i--) {
            Node next = levels.get(i);
            while (next != null) {
                int flag = compare(score, key, next);
                if (flag == 0) {
                    return next.getEntry().getValue().toByteArray();
                } else if (flag == -1){
                    break;
                }
                levels = next.levels;
                next = levels.get(i);
            }
        }
        return null;
    }

    @Override
    public boolean del(byte[] key) {
        return put(Entry.newBuilder()
                        .setKey(ByteString.copyFrom(key))
                        .setValue(ByteString.copyFrom("".getBytes()))
                        .build(), 1);
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public DBIterator iterator() {
        return new Iterator();
    }

    /**
     * 参考Redis的randLevel()方法
     * @return          新节点的level
     */
    private int randLevel() {
        int level = 1;
        Random random = new Random();
        // 从1 ~ MAX_LEVEL中取一个随机数
        // 如果该随机数取在1/4内（1 ~ 1 / 4 * MAX_LEVEL）中并且level小于MAX_LEVEL
        // level++
        int MAX_LEVEL = 64;
        while (random.nextInt(MAX_LEVEL) + 1 < MAX_LEVEL * 0.25 && level < MAX_LEVEL) {
            level++;
        }
        return level;
    }

    /**
     * 将key的前8个字节散列，方便比较
     * @param key           key
     * @return              key前8个字节散列后的hash
     */
    private float calcScore(byte[] key) {
        int l = Math.min(key.length, 8);
        long hash = 0;
        for (int i = 0 ; i < l; i++) {
            int j = 64 - 8 - i * 8;
            hash |= key[i] << j;
        }
        return hash;
    }

    /**
     * 比较A、B节点的大小
     * @param score         节点A的hash分数
     * @param key           节点A的key
     * @param node          节点B
     * @return              1：大于；0：等于；-1：小于
     */
    private int compare(float score, byte[] key, Node node) {
        if (score == node.score) {
            byte[] nodeKey = node.entry.getKey().toByteArray();
            for (int i = 0; i < key.length && i < nodeKey.length; i++) {
                if (key[i] > nodeKey[i]) {
                    return 1;
                } else if (key[i] < nodeKey[i]) {
                    return -1;
                }
            }
            if (key.length == nodeKey.length) {
                return 0;
            } else if (key.length > nodeKey.length) {
                return 1;
            }
            return -1;
        }
        return score > node.score ? 1 : -1;
    }

    class Node {
        private List<Node> levels = new ArrayList<>();
        private Entry entry;
        private float score;

        public Node() {}

        public Node(Entry entry, float score) {
            this.entry = entry;
            this.score = score;
        }

        public Entry getEntry() {
            return entry;
        }

        public void addLevel(Node node) {
            levels.add(node);
        }
    }

    class Iterator implements DBIterator {

        private Node node;

        public Iterator() {
            node = header;
        }

        @Override
        public boolean hasNext() {
            if (node == null) return false;
            Node index = node;
            do {
                index = index.levels.get(0);
            } while (index != null && index.entry.getValue().size() == 0);
            return index != null && index.entry.getValue().size() != 0;
        }

        @Override
        public Entry next() {

            if (node == null) return null;
            while ((node = node.levels.get(0)).entry.getValue().size() == 0) {}
            return node.entry;
        }

        @Override
        public void rewind() {
            node = header;
        }
    }
}
