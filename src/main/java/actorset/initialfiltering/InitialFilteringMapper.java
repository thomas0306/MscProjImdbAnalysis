package actorset.initialfiltering;

import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import util.Util;

import java.io.IOException;

/**
 * Created by Thomas on 16/6/2017.
 */
public class InitialFilteringMapper extends Mapper<Text, Text, Text, Text> {
    @Override
    public void map (Text key, Text value, Context context) throws IOException, InterruptedException{
        int acted = Iterables.size(Splitter.on(",").split(value.toString()));
        if (acted >= Util.minimumAct) {
            context.write(key, value);
        }
    }
}
