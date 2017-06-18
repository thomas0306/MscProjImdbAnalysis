package topk;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import util.Util;

import java.io.IOException;
import java.util.TreeMap;

/**
 * Created by Thomas on 17/6/2017.
 */
public class TopKReducer extends Reducer<NullWritable, Text, Text, Text> {
    private TreeMap<Float, String> ratingsTree = new TreeMap<Float, String>();

    @Override
    public void reduce (NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            float rating = Float.parseFloat(value.toString().split("\\|")[0]);
            String clique = value.toString();

            ratingsTree.put(rating, clique);

            if (ratingsTree.size() > Util.topK) {
                ratingsTree.remove(ratingsTree.firstKey());
            }
        }

        for (String record : ratingsTree.descendingMap().values()) {
            String[] fields = record.split("\\|");
            String clique = fields[1];
            String rating = fields[0];
            context.write(new Text(clique), new Text(rating));
        }
    }
}
