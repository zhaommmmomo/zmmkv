package com.zmm.kv.lsm;

import com.zmm.kv.api.Option;
import com.zmm.kv.file.Manifest;
import com.zmm.kv.file.SSTable;

import java.io.File;
import java.util.List;
import java.util.logging.Level;

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

        File manifestFile = new File(option.getDir() + "\\MANIFEST");
        if (manifestFile.exists()) {

        }
    }

    public byte[] get(byte[] key) {
        // 在Cache中找
        byte[] res = cache.get(key);
        if (res == null) {
            // TODO: 2022/2/20 如果在Cache中没找到，去磁盘中找

        }
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
