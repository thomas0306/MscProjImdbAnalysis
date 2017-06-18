package averaging;

import org.apache.hadoop.io.FloatWritable;
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
public class AverageMapper extends Mapper<Text, Text, Text, Text> {
    private static Map<Integer, Float> movieRatings = new HashMap<Integer, Float>();
    private BufferedReader reader;

    @Override
    public void setup (Context context) throws IOException, InterruptedException {
        List<String> cachedFiles = Util.getCacheFiles(context.getConfiguration());

        for (String path : cachedFiles) {
            loadMovieRatings(path, context);
        }
    }

    private void loadMovieRatings (String path, Context context) throws IOException {
        String line = "";

        try {
            reader = new BufferedReader(new FileReader(path));
            while ((line = reader.readLine()) != null) {
                String[] fields = line.split("\\t");

                int id = Integer.parseInt(fields[0]);
                float rating = Float.parseFloat(fields[1]);
                movieRatings.put(id, rating);
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
        int count = 0;
        float sum = 0;

        for (String movie : value.toString().split(",")) {
            int movieId = Integer.parseInt(movie);

            if (movieRatings.containsKey(movieId)) {
                float rating = movieRatings.get(movieId);
                sum += rating;
                count++;
            }
        }

        float average = sum / count;

        context.write(key, new Text(String.format("%.4f", average)));
    }
}
