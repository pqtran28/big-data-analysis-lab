import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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

public class GenreRatingAnalysis {

    // ---------------------------------------------------------
    // MAPPER
    // ---------------------------------------------------------
    public static class GenreMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        
        // HashMap lưu trữ ánh xạ MovieID -> Mảng các thể loại (Genres)
        private HashMap<String, String[]> movieGenresMap = new HashMap<>();
        private Text genreOut = new Text();
        private DoubleWritable ratingOut = new DoubleWritable();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // Đọc file movies.txt từ Distributed Cache
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null && cacheFiles.length > 0) {
                Path moviesPath = new Path(cacheFiles[0]);
                FileSystem fs = FileSystem.get(context.getConfiguration());
                BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(moviesPath)));
                
                String line;
                while ((line = br.readLine()) != null) {
                    int firstComma = line.indexOf(',');
                    int lastComma = line.lastIndexOf(',');
                    
                    if (firstComma != -1 && lastComma != -1 && firstComma != lastComma) {
                        String movieId = line.substring(0, firstComma).trim();
                        String genresStr = line.substring(lastComma + 1).trim();
                        
                        String[] genres = genresStr.split("\\|");
                        movieGenresMap.put(movieId, genres);
                    }
                }
                br.close();
            }
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] parts = line.split(",");
            
            if (parts.length >= 3) {
                String movieId = parts[1].trim();
                try {
                    double rating = Double.parseDouble(parts[2].trim());
                    
                    // Tìm list thể loại của MovieID này
                    String[] genres = movieGenresMap.get(movieId);
                    if (genres != null) {
                        // Với mỗi thể loại, emit cặp <Genre, Rating>
                        for (String genre : genres) {
                            genreOut.set(genre.trim());
                            ratingOut.set(rating);
                            context.write(genreOut, ratingOut);
                        }
                    }
                } catch (NumberFormatException e) { // lỡ có lỗi
                }
            }
        }
    }

    // ---------------------------------------------------------
    // REDUCER
    // ---------------------------------------------------------
    public static class GenreReducer extends Reducer<Text, DoubleWritable, Text, Text> {
        private Text result = new Text();

        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double sum = 0;
            int count = 0;

            // Tính tổng điểm và tổng số lượt đánh giá
            for (DoubleWritable val : values) {
                sum += val.get();
                count++;
            }

            if (count > 0) {
                double avg = sum / count;
                String outValue = String.format(Locale.US, "Avg: %.2f, Count: %d", avg, count);
                result.set(outValue);
                context.write(key, result);
            }
        }
    }

    // ---------------------------------------------------------
    // DRIVER
    // ---------------------------------------------------------
    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Cú pháp: GenreRatingAnalysis <đường_dẫn_movies.txt> <thư_mục_ratings> <thư_mục_output>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Bài 2: Đánh Giá Theo Thể Loại");
        job.setJarByClass(GenreRatingAnalysis.class);

        // THIẾT LẬP MAPPER / REDUCER
        job.setMapperClass(GenreMapper.class);
        job.setReducerClass(GenreReducer.class);

        // CẤU HÌNH KIỂU DỮ LIỆU ĐẦU RA CHO MAPPER VÀ REDUCER
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.addCacheFile(new URI(args[0]));

        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        boolean success = job.waitForCompletion(true);
        System.exit(success ? 0 : 1);
    }
}