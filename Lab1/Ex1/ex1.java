import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//kỹ thuật reduce side join
public class ex1 {

    // MAPPER 1: Xử lý file movies.txt 
    public static class MovieMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split(",");
            if (parts.length >= 2) {
                context.write(new Text(parts[0].trim()), new Text("M:" + parts[1].trim()));
            }
        }
    }

    // MAPPER 2: Xử lý file ratings
    public static class RatingMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split(",");
            if (parts.length >= 3) {
                context.write(new Text(parts[1].trim()), new Text("R:" + parts[2].trim()));
            }
        }
    }

    // REDUCER: Thực hiện Join và tính toán
    public static class JoinReducer extends Reducer<Text, Text, Text, Text> {
        private String maxMovie = "";
        private double maxRating = -1.0;

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String movieTitle = "Unknown";
            double sum = 0;
            int count = 0;
           
            for (Text val : values) {
                String strVal = val.toString();
                if (strVal.startsWith("M:")) {
                    movieTitle = strVal.substring(2); 
                } else if (strVal.startsWith("R:")) {
                    sum += Double.parseDouble(strVal.substring(2)); 
                    count++;
                }
            }
          
            if (!movieTitle.equals("Unknown") && count > 0) {
                context.getCounter("MOVIE_STATS", "TOTAL_DIFFERENT_MOVIES").increment(1);
                double average = sum / count;
                context.write(new Text(movieTitle), new Text("AverageRating: " + average + " (TotalRatings: " + count + ")"));
                if (count >= 5 && average > maxRating) {
                    maxRating = average;
                    maxMovie = movieTitle;
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            long totalDifferentMovies = context.getCounter("MOVIE_STATS", "TOTAL_DIFFERENT_MOVIES").getValue();
            context.write(new Text("\n--- THỐNG KÊ CHUNG ---"), new Text(""));
            context.write(new Text("Tổng số phim khác nhau đã xử lý: "), new Text(String.valueOf(totalDifferentMovies)));
            if (!maxMovie.isEmpty()) {
                context.write(new Text("\n--- KẾT QUẢ ---"), new Text(""));
                context.write(new Text(maxMovie), new Text("is the highest rated movie with an average rating of " 
                    + maxRating + " among movies with at least 5 ratings."));
            }
            else {
                context.write(new Text("\n--- KẾT QUẢ ---"), new Text("No movie found with at least 5 ratings."));
            }
            
        }

    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Movie Reduce-side Join");
        job.setJarByClass(ex1.class);
        // args[0] là path tới movies.txt, args[1] là path tới thư mục chứa các file ratings1.txt, args[2] là path tới thư mục chứa các file ratings2.txt
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, MovieMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, RatingMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[2]), TextInputFormat.class, RatingMapper.class);
        job.setReducerClass(JoinReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job, new Path(args[3]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}