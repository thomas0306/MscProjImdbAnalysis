package util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

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
        String[] locArr = loc.split("/");
        if (locArr.length > 1) {
            FileSystem fs = FileSystem.get(conf);
            for (FileStatus file: fs.listStatus(new Path(locArr[0]))) {
                String filePath = file.getPath().toString();
                String[] filePathArr = filePath.split("/");
                String fileName = filePathArr[filePathArr.length-1];
                if (fileName.startsWith(locArr[1])) {
                    DistributedCache.addCacheFile(new URI(filePath), conf);
                }
            }
        }
        else {
            DistributedCache.addCacheFile(new URI(loc), conf);
        }
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
