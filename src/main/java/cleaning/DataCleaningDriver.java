package cleaning;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * Created by Thomas on 16/6/2017.
 */
public class DataCleaningDriver {
    public static void main (String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapred.max.split.size", "2000000");

        Job job = new Job(conf, "DataCleaningDriver");
        job.setJarByClass(DataCleaningDriver.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(DataCleaningMapper.class);
        job.setReducerClass(DataCleaningReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleOutputs.addNamedOutput(job, "actors", TextOutputFormat.class, Text.class, Text.class);
        MultipleOutputs.addNamedOutput(job, "ratings", TextOutputFormat.class, Text.class, Text.class);
        MultipleOutputs.addNamedOutput(job, "actin", TextOutputFormat.class, Text.class, Text.class);

        job.waitForCompletion(true);
    }
}
