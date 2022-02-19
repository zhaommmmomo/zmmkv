package com.zmm.kv.worker;

import com.zmm.kv.lsm.MemTable;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author zmm
 * @date 2022/2/18 13:21
 */
public class Flusher implements Runnable{

    private static Queue<MemTable> task = new LinkedBlockingQueue<>();

    @Override
    public void run() {

    }

}
