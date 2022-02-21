package com.zmm.kv.worker;

import com.zmm.kv.api.DBIterator;
import com.zmm.kv.api.Option;
import com.zmm.kv.file.SSTable;
import com.zmm.kv.file.Wal;
import com.zmm.kv.lsm.LevelManager;
import com.zmm.kv.lsm.MemTable;
import com.zmm.kv.pb.Entry;

import java.nio.MappedByteBuffer;
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
        Thread thread = new Thread(this, "flusher");
        //thread.setDaemon(true);
        thread.start();
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

        while (task.size() > 0) {
            // 构建sst
            SSTable ssTable = SSTable.build(task.get(0), option);

            if (ssTable == null) {
                // 说明该memTable中没有有效元素
                task.remove(0);
                continue;
            }

            // 将新构建的sst放入level层级中
            if (levelManager.changeLevels(ssTable)) {
                // 将对应的内存表删除
                task.remove(0);
            } else {
                ssTable.remove(option.getDir());
            }
        }
    }

    public void close() {
        flag = false;
    }
}
