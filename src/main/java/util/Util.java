package util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;

import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Thomas on 16/6/2017.
 */
public class Util {

    public static final int minimumAct = 3;
    public static final int topK = 15;

    public static void addCacheFile (String loc, Configuration conf) throws Exception{
        DistributedCache.addCacheFile(new URI(loc), conf);
    }

    public static List<String> getCacheFiles(Configuration conf) throws IOException{
        List<String> cacheFiles = new ArrayList<String>();

        Path[] locations = DistributedCache.getLocalCacheFiles(conf);
        for (Path file : locations) {
            cacheFiles.add(file.toString());
        }

        return cacheFiles;

    }
}
