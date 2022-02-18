package com.zmm.kv.worker;

import com.zmm.kv.lsm.MemTable;

import java.util.List;

/**
 * @author zmm
 * @date 2022/2/18 13:21
 */
public class Flusher implements Runnable{

    private List<MemTable> task;

    public Flusher(List<MemTable> task) {
        this.task = task;
    }

    @Override
    public void run() {

    }
}
