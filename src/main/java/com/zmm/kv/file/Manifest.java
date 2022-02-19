package com.zmm.kv.file;

import java.io.File;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * @author zmm
 * @date 2022/2/19 17:43
 */
public class Manifest {

    private List<File>[] levels;

    public Manifest() {
        levels = new CopyOnWriteArrayList[8];

    }

    public void loadManifest(File file) {

    }
}
