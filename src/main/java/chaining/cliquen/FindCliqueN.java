package chaining.cliquen;

import actorset.actorsetgenerator.ActorsetGeneratorDriver;
import actorset.initialfiltering.InitialFilteringDriver;
import averaging.AverageDriver;

import com.google.common.base.Stopwatch;
import namelookup.NameLookupDriver;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import topk.TopKDriver;

import java.net.URI;
import java.util.concurrent.TimeUnit;


/**
 * Created by Thomas on 17/6/2017.
 */
public class FindCliqueN {
    public static void main (String[] args) throws Exception {
        Logger logger = Logger.getLogger(FindCliqueN.class);
        final Stopwatch stopwatch = new Stopwatch().start();

        int n = Integer.parseInt(args[0]);

        if (n > 10) {
            throw new Exception("N is too large! (" + n + ")");
        }

        FileSystem hdfs = FileSystem.get(/*new URI("hdfs://quickstart.cloudera:8020"), */new Configuration());

        // Initial filtering
        if (!hdfs.exists(new Path("output_actorset1/"))) {
            String[] InitialFilteringArgs = {"output_datacleaning/", "output_actorset1/"};
            InitialFilteringDriver.main(InitialFilteringArgs);
        }

        // Actorset Generation
        int currentN = n;

        // Check if any < n actorset already computed

        while (currentN > 1 && !hdfs.exists(new Path("output_actorset"+currentN+"/"))) {
            currentN--;
        }

        // Start from the largest available actorset
        for (int i = currentN; i < n; i++) {
            String[] ActorsetGeneratorArgs = {"output_actorset1/part-r-00000", "output_actorset" + i + "/", "output_actorset" + (i + 1) + "/"};
            ActorsetGeneratorDriver.main(ActorsetGeneratorArgs);
        }

        // Averaging
        String[] AverageDriverArgs = {"output_datacleaning/ratings-m-00000", "output_actorset" + n + "/", "output_avg_actorset" + n + "/"};
        AverageDriver.main(AverageDriverArgs);

        // Top K
        String[] TopKDriverArgs = {"output_avg_actorset" + n + "/", "output_topk_actorset" + n + "/"};
        TopKDriver.main(TopKDriverArgs);

        // Name Lookup
        String[] NameLookupDriverArgs = {"output_datacleaning/actors-m-00000", "output_topk_actorset" + n + "/", "output_topk_name" + n + "/"};
        NameLookupDriver.main(NameLookupDriverArgs);

        logger.info("Time of execution in milliseconds: " + stopwatch.stop().elapsedTime(TimeUnit.MILLISECONDS));
    }
}
