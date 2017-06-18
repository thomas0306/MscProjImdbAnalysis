package topk;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import util.Util;

import java.io.IOException;
import java.util.TreeMap;

/**
 * Created by Thomas on 17/6/2017.
 */
public class TopKMapper extends Mapper<Text, Text, NullWritable, Text> {
    private TreeMap<Float, Text> ratingsTree = new TreeMap<Float, Text>();

    @Override
    public void map (Text key, Text value, Context context) throws IOException, InterruptedException {
        Float rating = Float.parseFloat(value.toString());

        ratingsTree.put(rating, new Text(value.toString() + "|" + key.toString()));

        if (ratingsTree.size() > Util.topK) {
            ratingsTree.remove(ratingsTree.firstKey());
        }
    }

    @Override
    public void cleanup (Context context) throws IOException, InterruptedException {
        for (Text clique : ratingsTree.values()) {
            context.write(NullWritable.get(), clique);
        }
    }
}
