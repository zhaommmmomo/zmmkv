package com.zmm.kv.lsm;

import com.zmm.kv.api.Option;
import com.zmm.kv.file.Manifest;
import com.zmm.kv.file.SSTable;

import java.io.File;
/**
 * @author zmm
 * @date 2022/2/18 13:36
 */
public class LevelManager {

    private Cache cache;
    private Manifest manifest;
    private Option option;

    public LevelManager(Option option) {
        this.option = option;
        cache = new Cache();
        manifest = new Manifest();
        init();
    }

    private void init() {
        File manifestFile = new File(option.getDir() + "\\MANIFEST");
        if (manifestFile.exists()) {
            manifest.loadManifest(manifestFile);
        }
    }

    public byte[] get(byte[] key) {
        // 在Cache中找
        byte[] res = cache.get(key);
        if (res == null) {
            // 如果在Cache中没找到，去磁盘中找

        }
        return res;
    }
}
