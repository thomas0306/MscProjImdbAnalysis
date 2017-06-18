package cleaning;

import com.google.common.base.Joiner;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by Thomas on 16/6/2017.
 */
public class DataCleaningReducer extends Reducer<Text, Text, Text, Text> {
    private MultipleOutputs<Text, Text> outputs;
    private StringBuilder builder;

    @Override
    public void setup (Context context) {
        outputs = new MultipleOutputs<Text, Text>(context);
        builder = new StringBuilder();
    }

    @Override
    public void reduce (Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        builder.setLength(0);

        List<Integer> movieIds = new ArrayList<Integer>();
        for (Text id_text : values) {
            movieIds.add(Integer.parseInt(id_text.toString()));
        }

        Collections.sort(movieIds);

        Text movieIdsStr = new Text(Joiner.on(",").join(movieIds));

        outputs.write("actin", key, movieIdsStr);
    }

    @Override
    public void cleanup (Context context) throws IOException, InterruptedException {
        outputs.close();
    }

}
