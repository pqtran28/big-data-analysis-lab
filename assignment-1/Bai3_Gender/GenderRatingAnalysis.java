import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Locale;

public class GenderRatingAnalysis {

    public static class GenderMapper extends Mapper<LongWritable, Text, Text, Text> {
        private HashMap<String, String> movieNames = new HashMap<>();
        private HashMap<String, String> userGenders = new HashMap<>();
        private Text outKey = new Text();
        private Text outValue = new Text();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null && cacheFiles.length >= 2) {
                loadMovies(cacheFiles[0], context.getConfiguration());
                loadUsers(cacheFiles[1], context.getConfiguration());
            }
        }

        private void loadMovies(URI fileUri, Configuration conf) throws IOException {
            FileSystem fs = FileSystem.get(conf);
            try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(fileUri))))) {
                String line;
                while ((line = br.readLine()) != null) {
                    int firstComma = line.indexOf(',');
                    int lastComma = line.lastIndexOf(',');
                    if (firstComma != -1 && lastComma != -1) {
                        String id = line.substring(0, firstComma).trim();
                        String title = line.substring(firstComma + 1, lastComma).trim();
                        movieNames.put(id, title);
                    }
                }
            }
        }

        private void loadUsers(URI fileUri, Configuration conf) throws IOException {
            FileSystem fs = FileSystem.get(conf);
            try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(fileUri))))) {
                String line;
                while ((line = br.readLine()) != null) {
                    String[] parts = line.split(",");
                    if (parts.length >= 2) {
                        userGenders.put(parts[0].trim(), parts[1].trim()); // UserID -> Gender
                    }
                }
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split(",");
            if (parts.length >= 3) {
                String userId = parts[0].trim();
                String movieId = parts[1].trim();
                String rating = parts[2].trim();

                String title = movieNames.get(movieId);
                String gender = userGenders.get(userId);

                if (title != null && gender != null) {
                    outKey.set(title);
                    outValue.set(gender + ":" + rating);
                    context.write(outKey, outValue);
                }
            }
        }
    }

    public static class GenderReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double maleSum = 0, femaleSum = 0;
            int maleCount = 0, femaleCount = 0;

            for (Text val : values) {
                String[] pair = val.toString().split(":");
                String gender = pair[0];
                double rating = Double.parseDouble(pair[1]);

                if (gender.equalsIgnoreCase("M")) {
                    maleSum += rating;
                    maleCount++;
                } else if (gender.equalsIgnoreCase("F")) {
                    femaleSum += rating;
                    femaleCount++;
                }
            }

            // Tính trung bình: Avg = Sum / Count
            String maleAvg = (maleCount > 0) ? String.format(Locale.US, "%.2f", maleSum / maleCount) : "0.00";
            String femaleAvg = (femaleCount > 0) ? String.format(Locale.US, "%.2f", femaleSum / femaleCount) : "0.00";

            context.write(key, new Text("Male: " + maleAvg + ", Female: " + femaleAvg));
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            System.err.println("Usage: GenderRatingAnalysis <movies> <users> <ratings_input> <output>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Gender Rating Analysis");
        job.setJarByClass(GenderRatingAnalysis.class);

        job.setMapperClass(GenderMapper.class);
        job.setReducerClass(GenderReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Add 2 files to Distributed Cache
        job.addCacheFile(new URI(args[0])); // movies.txt
        job.addCacheFile(new URI(args[1])); // users.txt

        FileInputFormat.addInputPath(job, new Path(args[2])); // ratings input
        FileOutputFormat.setOutputPath(job, new Path(args[3]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}