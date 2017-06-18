package actorset.actorsetgenerator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by Thomas on 17/6/2017.
 */
public class ActorsetGeneratorReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce (Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        context.write(key, values.iterator().next());
    }
}
