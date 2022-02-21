package com.zmm.kv.lsm;

import com.zmm.kv.api.Option;
import com.zmm.kv.file.Manifest;
import com.zmm.kv.file.SSTable;

import java.io.File;
import java.util.List;

/**
 * @author zmm
 * @date 2022/2/18 13:36
 */
public class LevelManager {

    private Manifest manifest;
    private Cache cache;
    private Option option;

    public LevelManager(Option option) {
        this.option = option;
        cache = new Cache();
        manifest = new Manifest();
        init();
    }

    private void init() {

        manifest.loadManifest(option.getDir());
    }

    public byte[] get(byte[] key) {
        // 在Cache中找
        byte[] res = cache.get(key);

        res = res != null ? res : manifest.get(key);

        return res;
    }

    public boolean changeLevels(SSTable ssTable) {
        if (manifest.changeLevels(ssTable)) return true;
        throw new RuntimeException("[flush] changeLevels fail!");
    }

    public boolean changeLevels(SSTable ssTable, List<File> files) {
        if (manifest.changeLevels(ssTable, files)) return true;
        throw new RuntimeException("[merge] changeLevels fail!");
    }
}
