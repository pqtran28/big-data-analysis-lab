import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class MovieRatingAnalysis {

    // MAPPER: Đọc file ratings_1.txt và ratings_2.txt
    // Input: UserID, MovieID, Rating, Timestamp
    public static class RatingMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        private Text movieID = new Text();
        private DoubleWritable rating = new DoubleWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] tokens = line.split(","); 
            if (tokens.length >= 3) {
                movieID.set(tokens[1].trim()); // MovieID
                try {
                    rating.set(Double.parseDouble(tokens[2])); // Rating
                    context.write(movieID, rating);
                } catch (NumberFormatException e) {
                }
            }
        }
    }

    // REDUCER: Tính toán và tìm phim cao điểm nhất
    public static class RatingReducer extends Reducer<Text, DoubleWritable, NullWritable, Text> {
        private Map<String, String> movieNames = new HashMap<>();
        private String maxMovieTitle = "";
        private double maxAvgRating = -1.0;

        // Load movies.txt từ Distributed Cache
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null && cacheFiles.length > 0) {
                for (URI cacheFile : cacheFiles) {
                    if (cacheFile.getPath().contains("movies.txt")) {
                        loadMovieNames(cacheFile);
                    }
                }
            }
        }

        private void loadMovieNames(URI fileUri) throws IOException {
            try (BufferedReader reader = new BufferedReader(new FileReader("movies.txt"))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] tokens = line.split("::|;|,");
                    if (tokens.length >= 2) {
                        movieNames.put(tokens[0], tokens[1]); // ID -> Title
                    }
                }
            }
        }

        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double sum = 0;
            int count = 0;

            for (DoubleWritable val : values) {
                sum += val.get();
                count++;
            }

            double average = sum / count;
            String title = movieNames.getOrDefault(key.toString(), "Unknown (ID: " + key.toString() + ")");

            context.write(NullWritable.get(), new Text(title + " Average rating: " + average + " (Total ratings: " + count + ")"));

            if (count >= 5) {
                if (average > maxAvgRating) {
                    maxAvgRating = average;
                    maxMovieTitle = title;
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            if (!maxMovieTitle.isEmpty()) {
                context.write(NullWritable.get(), new Text("\n--- HIGHEST RATED MOVIE ---"));
                context.write(NullWritable.get(), new Text(maxMovieTitle + " is the highest rated movie with an average rating of " 
                              + maxAvgRating + " among movies with at least 5 ratings."));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: MovieRatingAnalysis <input_ratings_path> <input_movies_file> <output_path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Movie Rating Analysis");

        job.setJarByClass(MovieRatingAnalysis.class);
        job.setMapperClass(RatingMapper.class);
        job.setReducerClass(RatingReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        // Thêm file movies.txt vào cache
        job.addCacheFile(new Path(args[1]).toUri());

        FileInputFormat.addInputPath(job, new Path(args[0])); // Thư mục chứa ratings_1 và ratings_2
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}