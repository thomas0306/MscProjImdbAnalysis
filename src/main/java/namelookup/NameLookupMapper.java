package namelookup;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import util.Util;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Thomas on 17/6/2017.
 */
public class NameLookupMapper extends Mapper<Text, Text, Text, Text> {
    private static Map<Integer, String> actorNames = new HashMap<Integer, String>();
    private BufferedReader reader;

    @Override
    public void setup (Context context) throws IOException, InterruptedException {
        List<String> cachedFiles = Util.getCacheFiles(context.getConfiguration());

        for (String path : cachedFiles) {
            loadActorNames(path, context);
        }
    }

    private void loadActorNames (String path, Context context) throws IOException {
        String line = "";

        try {
            reader = new BufferedReader(new FileReader(path));
            while ((line = reader.readLine()) != null) {
                String[] fields = line.split("\\t");

                int actorId = Integer.parseInt(fields[0]);
                String actorName = fields[1];

                actorNames.put(actorId, actorName);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                reader.close();
            }
        }
    }

    @Override
    public void map (Text key, Text value, Context context) throws IOException, InterruptedException {
        StringBuilder builder = new StringBuilder();

        for (String actorId : key.toString().split(",")) {
            String actorName = actorNames.get(Integer.parseInt(actorId));
            builder.append(actorName);
            builder.append(",");
        }

        if (builder.length() > 0){
            builder.setLength(builder.length() - 1);
        }

        context.write(value, new Text(builder.toString()));
    }
}
