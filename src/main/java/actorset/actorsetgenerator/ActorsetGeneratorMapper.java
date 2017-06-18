package actorset.actorsetgenerator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import util.CliqueCast;
import util.Util;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Thomas on 16/6/2017.
 */

public class ActorsetGeneratorMapper extends Mapper<Text, Text, Text, Text> {
    private static List<CliqueCast> oneActorSet = new ArrayList<CliqueCast>();
    private BufferedReader reader;

    @Override
    public void setup (Context context) throws IOException, InterruptedException {
        List<String> cachedFiles = Util.getCacheFiles(context.getConfiguration());
        for (String path : cachedFiles) {
            loadNActorSet(path, context);
        }
    }

    private void loadNActorSet (String path, Context context) throws IOException {
        String line = "";

        try {
            reader = new BufferedReader(new FileReader(path));
            while ((line = reader.readLine()) != null) {
                String[] fields = line.split("\\t");

                CliqueCast clique = new CliqueCast(fields[0], fields[1]);
                oneActorSet.add(clique);
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
        for (CliqueCast clique : oneActorSet) {
            if (!clique.isIncluded(key.toString()) && clique.isCoActedGreaterThanOrEqualToN(value.toString(), Util.minimumAct)) {
                String[] nPlus1Clique = clique.combineActorIntoClique(key.toString(), value.toString());
                context.write(new Text(nPlus1Clique[0]), new Text(nPlus1Clique[1]));
            }
        }
    }
}