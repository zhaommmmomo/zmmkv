package com.zmm.kv.worker;

import com.zmm.kv.api.Option;
import com.zmm.kv.file.Manifest;
import com.zmm.kv.file.SSTable;

import java.io.File;
import java.util.*;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author zmm
 * @date 2022/2/18 13:24
 */
public class Merger implements Runnable{

    private final Lock lock;
    private final Condition condition;
    private static volatile boolean flag = true;
    private final Manifest manifest;
    private final Option option;
    private final Queue<Integer> task;

    public Merger(Manifest manifest, Option option) {
        lock = new ReentrantLock();
        condition = lock.newCondition();
        this.manifest = manifest;
        task = new LinkedList<>();
        this.option = option;
        Thread thread = new Thread(this, "merger");
        thread.start();
    }

    @Override
    public void run() {
        lock.lock();
        try {
            while (flag) {
                condition.await();
                doMerge();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            lock.unlock();
        }
    }

    public void merge(int level) {
        lock.lock();
        try {
            task.offer(level);
            condition.signalAll();
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            lock.unlock();
        }
    }

    public void doMerge() {
        int level;
        while (!task.isEmpty()) {
            level = task.poll();
            if (level == 0) {
                merge0();
            } else {
                mergeN(level);
            }
        }
    }

    private void merge0() {
        // 遍历l0层的所有sst，将相同区间的压缩合并到下一层
        List<File>[] levels = manifest.getLevels();
        File sst = levels[0].get(0);
        // 从下一层中找区间重叠的
        List<File> mergeFile = new ArrayList<>();
        List<Integer> dataSizes = new ArrayList<>();
        overlap(sst, levels[1], mergeFile, dataSizes);
        mergeFile.add(sst);
        dataSizes.add((int) manifest.getFileIndexMap().get(sst).get(3));
        List<File> level = new ArrayList<>(levels[0]);
        level.remove(0);
        overlap(sst, level, mergeFile, dataSizes);

        // 构建新的SST
        List<SSTable> newSSTable = SSTable.build(mergeFile, dataSizes, option);

        // 修改manifest
        if (!manifest.changeLevels(newSSTable, mergeFile))
            throw new RuntimeException("[merge] changeLevels fail!");
    }

    private void mergeN(int level) {
        // 将最旧的sst与下一层相同区间的sst进行压缩合并
        List<File>[] levels = manifest.getLevels();
        Map<File, List<Object>> fileIndexMap = manifest.getFileIndexMap();
        File sst = levels[level].get(0);
        List<Object> indexs = fileIndexMap.get(sst);
        // 获取最大最小keyScore以及dataSize
        int dataSize = (int) indexs.get(3);

        // 从下一层中找区间重叠的
        List<File> mergeFile = new ArrayList<>();
        List<Integer> dataSizes = new ArrayList<>();
        overlap(sst, levels[level + 1], mergeFile, dataSizes);

        // 构建新的sst
        List<SSTable> newSSTables =
                SSTable.build(sst, mergeFile, dataSize, dataSizes, level + 1, option);

        // 修改manifest
        if (!manifest.changeLevels(newSSTables, sst, mergeFile, level))
            throw new RuntimeException("[merge] changeLevels fail!");
    }

    private void overlap(File sst, List<File> level, List<File> mergeFile, List<Integer> dataSizes) {
        Map<File, List<Object>> fileIndexMap = manifest.getFileIndexMap();
        boolean flag = mergeFile.size() == 1;
        List<Object> indexs = fileIndexMap.get(sst);
        // 获取最大最小keyScore以及dataSize
        float minKeyScore = (float) indexs.get(1);
        float maxKeyScore = (float) indexs.get(2);

        List<Object> nextIndex;
        float nextMinKeyScore;
        float nextMaxKeyScore;
        for (File nextFile : level) {
            nextIndex = fileIndexMap.get(nextFile);
            nextMinKeyScore = (float) nextIndex.get(1);
            nextMaxKeyScore = (float) nextIndex.get(2);
            if (nextMinKeyScore > maxKeyScore ||
                    nextMaxKeyScore < minKeyScore) continue;

            // 记录有重叠区间的sst
            if (flag) {
                // 如果是L0层
                mergeFile.add(nextFile);
                dataSizes.add((int) nextIndex.get(3));
            } else {
                // 如果是LN层
                int i = 0;
                for (; i < mergeFile.size(); i++) {
                    if (nextMinKeyScore <
                            (float) fileIndexMap.get(mergeFile.get(i)).get(1)) {
                        break;
                    }
                }
                // 将minKeyScore小的放前面
                mergeFile.add(i, nextFile);
                dataSizes.add(i, (int) nextIndex.get(3));
            }
        }
    }

    public void close() {
        flag = false;
    }
}
