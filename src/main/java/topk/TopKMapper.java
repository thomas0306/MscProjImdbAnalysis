package topk;

import com.google.common.collect.TreeMultimap;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import util.Util;

import java.io.IOException;

/**
 * Created by Thomas on 17/6/2017.
 */
public class TopKMapper extends Mapper<Text, Text, NullWritable, Text> {
    private TreeMultimap<Float, Text> ratingsTree = TreeMultimap.create();

    @Override
    public void map (Text key, Text value, Context context) throws IOException, InterruptedException {
        Float rating = Float.parseFloat(value.toString());

        ratingsTree.put(rating, new Text(value.toString() + "|" + key.toString()));

        if (ratingsTree.size() > Util.topK) {
            float firstKey = ratingsTree.keySet().first();
            ratingsTree.remove(firstKey, ratingsTree.get(firstKey).first());
        }
    }

    @Override
    public void cleanup (Context context) throws IOException, InterruptedException {
        for (Text clique : ratingsTree.values()) {
            context.write(NullWritable.get(), clique);
        }
    }
}
