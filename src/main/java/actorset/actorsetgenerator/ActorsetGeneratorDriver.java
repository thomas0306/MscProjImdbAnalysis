package actorset.actorsetgenerator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import util.Util;

/**
 * Created by Thomas on 16/6/2017.
 */
public class ActorsetGeneratorDriver {
    public static void main (String[] args) throws Exception {
        Configuration conf = new Configuration();

        // put small table into distributed cache
        Util.addCacheFile(args[0], conf);

        Job job = new Job(conf, "ActorsetGeneratorDriver");
        job.setJarByClass(ActorsetGeneratorDriver.class);

        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        job.setMapperClass(ActorsetGeneratorMapper.class);
        job.setReducerClass(ActorsetGeneratorReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.waitForCompletion(true);
    }
}
