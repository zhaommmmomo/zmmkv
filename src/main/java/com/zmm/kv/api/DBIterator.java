package com.zmm.kv.api;

import com.zmm.kv.pb.Entry;

/**
 * @author zmm
 * @date 2022/2/17 19:19
 */
public interface DBIterator {

    boolean hasNext();
    Entry next();
    void rewind();
}
