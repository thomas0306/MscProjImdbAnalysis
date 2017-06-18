package cleaning;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.util.regex.Pattern;

/**
 * Created by Thomas on 16/6/2017.
 */
public class DataCleaningMapper extends Mapper<Object, Text, Text, Text> {
    private Pattern linkPattern;
    private Pattern namePattern;
    private Pattern ratingPattern;

    private MultipleOutputs<Text, Text> outputs;

    @Override
    public void setup (Context context) {
        linkPattern = Pattern.compile("^(actor|atress)\\d{1,6}\\s:\\smovie\\d{1,6}\\.persons$");
        namePattern = Pattern.compile("^(actor|actress)\\d{1,6}\\.name\\s=\\s(\".+\"|\\d\\.\\d)$");
        ratingPattern = Pattern.compile("^movie\\d{1,6}\\.rating\\s=\\s\\d\\.\\d$");

        outputs = new MultipleOutputs<Text, Text>(context);
    }

    @Override
    public void map (Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

        if (linkPattern.matcher(line).matches()) {
            // actor5241 : movie46959.persons
            String actorId = line.split(" : ")[0];
            actorId = actorId.replaceAll("(actor|actress|movie)", "");
            String movieId = line.split(" : ")[1].split("\\.")[0];
            movieId = movieId.replaceAll("(actor|actress|movie)", "");

            context.write(new Text(actorId), new Text(movieId));
        } else if (namePattern.matcher(line).matches()) {
            // actor2659.name = "Bako, Peter"
            String actorId = line.split(" = ")[0].split("\\.")[0].replaceAll("(actor|actress)", "");
            String actorName = line.split(" = ")[1];

            outputs.write("actors", new Text(actorId), new Text(actorName));
        } else if (ratingPattern.matcher(line).matches()) {
            // movie46959.rating = 0.0
            String movieId = line.split(" = ")[0].split("\\.")[0].replaceAll("movie", "");
            String rating = line.split(" = ")[1];

            outputs.write("ratings", new Text(movieId), new Text(rating));
        }
    }

    @Override
    public void cleanup (Context context) throws IOException, InterruptedException {
        outputs.close();
    }
}
