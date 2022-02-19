package com.zmm.kv.worker;

import com.zmm.kv.api.DBIterator;
import com.zmm.kv.api.Option;
import com.zmm.kv.file.SSTable;
import com.zmm.kv.lsm.LevelManager;
import com.zmm.kv.lsm.MemTable;
import com.zmm.kv.pb.Entry;

import java.util.List;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author zmm
 * @date 2022/2/18 13:21
 */
public class Flusher implements Runnable{

    private final Lock lock;
    private final Condition condition;
    private volatile boolean flag = true;

    private final LevelManager levelManager;
    private final Option option;
    private List<MemTable> task;

    public Flusher(Option option, LevelManager levelManager){
        lock = new ReentrantLock();
        condition = lock.newCondition();
        this.option = option;
        this.levelManager = levelManager;
        new Thread(this).start();
    }

    @Override
    public void run() {
        lock.lock();
        try {
            while (flag) {
                condition.await();
                doFlush();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            lock.unlock();
        }
    }

    /**
     * 唤醒flusher开始执行flush
     * @param task          任务列表
     */
    public void flush(List<MemTable> task) {
        lock.lock();
        try {
            this.task = task;
            condition.signalAll();
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            lock.unlock();
        }
    }

    private void doFlush() {
        // TODO: 2022/2/18 大value分离

        DBIterator iterator;
        Entry entry;
        for (MemTable memTable : task) {
            iterator = memTable.iterator();
            while (iterator.hasNext()) {
                entry = iterator.next();
                // 判断是否需要进行kv分离
                if (entry.getValue().size() > option.getValueSize()) {
                    // 进行kv分离
                }
            }

            // 构建sst
            //SSTable ssTable = levelManager.buildSSTable(memTable);
            SSTable ssTable = SSTable.build(memTable);



            //
        }
    }

    public void close() {
        flag = false;
    }
}
