import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.*;
import java.util.*;
import java.net.URI;

public class ex3 {

    public static class GenderMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Map<String, String> userGenderMap = new HashMap<>();
        private Map<String, double[]> combinedData = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException {
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null) {
                for (URI cacheUri : cacheFiles) {
                    String path = cacheUri.getPath();
                    if (path.contains("users.txt")) {
                        loadUserCache(new File("users.txt"));
                    }
                }
            }
        }
        // private void loadMovieCache(File file) throws IOException {
        //     BufferedReader reader = new BufferedReader(new FileReader(file));
        //     String line;
        //     while ((line = reader.readLine()) != null) {
        //         String[] parts = line.split(",");
        //         if (parts.length >= 2) movieTitleMap.put(parts[0].trim(), parts[1].trim());
        //     }
        //     reader.close();
        // }

        private void loadUserCache(File file) throws IOException {
            BufferedReader reader = new BufferedReader(new FileReader(file));
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(",");
                if (parts.length >= 2) userGenderMap.put(parts[0].trim(), parts[1].trim());
            }
            reader.close();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) {
            String[] parts = value.toString().split(",");
            if (parts.length >= 3) {
                String userId = parts[0].trim();
                String movieId = parts[1].trim();
                double rating = Double.parseDouble(parts[2].trim());

                String gender = userGenderMap.get(userId);
                if (gender != null) {
                    double[] stats = combinedData.getOrDefault(movieId, new double[4]);
                    if (gender.equalsIgnoreCase("M")) {
                        stats[0] += rating; // Male Sum
                        stats[1] += 1;      // Male Count
                    } else {
                        stats[2] += rating; // Female Sum
                        stats[3] += 1;      // Female Count
                    }
                    combinedData.put(movieId, stats);
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry<String, double[]> entry : combinedData.entrySet()) {
                String movieId = entry.getKey();
                double[] s = entry.getValue();
                // Gửi: Title -> MaleSum,MaleCount,FemaleSum,FemaleCount
                context.write(new Text(movieId), new Text(s[0] + "," + s[1] + "," + s[2] + "," + s[3]));
            }
        }   
    }

    public static class GenderReducer extends Reducer<Text, Text, Text, Text> {
        private Map<String, String> movieTitleMap = new HashMap<>();
        @Override
        protected void setup(Context context) throws IOException {
        // Đọc movies.txt từ Distributed Cache vào map tương tự như ở Mapper
            BufferedReader reader = new BufferedReader(new FileReader("movies.txt"));
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(",");
                if (parts.length >= 2) {
                    movieTitleMap.put(parts[0].trim(), parts[1].trim());
                }
            }
            reader.close();
        }
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double mSum = 0, fSum = 0;
            long mCount = 0, fCount = 0;

            for (Text val : values) {
                String[] p = val.toString().split(",");
                mSum += Double.parseDouble(p[0]);       
                mCount += Long.parseLong(p[1].split("\\.")[0]);
                fSum += Double.parseDouble(p[2]);
                fCount += Long.parseLong(p[3].split("\\.")[0]);
            }

            String mAvg = (mCount > 0) ? String.format("%.2f", mSum / mCount) : "0.00";
            String fAvg = (fCount > 0) ? String.format("%.2f", fSum / fCount) : "0.00";
            String title = movieTitleMap.getOrDefault(key.toString(), "Unknown Movie");
            context.write(new Text(title), new Text("Male: " + mAvg + ", Female: " + fAvg));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Gender Analysis");
        job.setJarByClass(ex3.class);
        job.setMapperClass(GenderMapper.class);
        job.setReducerClass(GenderReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.addCacheFile(new URI("hdfs:///input/movies.txt#movies.txt"));
        job.addCacheFile(new URI("hdfs:///input/users.txt#users.txt"));
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}