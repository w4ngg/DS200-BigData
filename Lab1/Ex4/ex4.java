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

public class ex4 {

    public static class AgeMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Map<String, String> userAgeMap = new HashMap<>();
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
                int age = Integer.parseInt(parts[2].trim());
                String ageGroup = (age < 18) ? "0-18" : (age < 35) ? "18-35" : (age < 50) ? "35-50": "50+";
                if (parts.length >= 2) userAgeMap.put(parts[0].trim(), ageGroup);
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

                String ageGroup = userAgeMap.get(userId);
                if (ageGroup != null) {
                    double[] stats = combinedData.getOrDefault(movieId, new double[8]);
                    if (ageGroup.equalsIgnoreCase("0-18")) {
                        stats[0] += rating; // 0-18 Sum
                        stats[1] += 1;      // 0-18 Count
                    } else if (ageGroup.equalsIgnoreCase("18-35")) {
                        stats[2] += rating; // 18-35 Sum
                        stats[3] += 1;      // 18-35 Count
                    } else if (ageGroup.equalsIgnoreCase("35-50")) {
                        stats[4] += rating; // 35-50 Sum
                        stats[5] += 1;      // 35-50 Count
                    } else {
                        stats[6] += rating; // 50+ Sum
                        stats[7] += 1;      // 50+ Count
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
                // Gửi: Title -> 
                context.write(new Text(movieId), new Text(s[0] + "," + s[1] + "," + s[2] + "," + s[3] + "," + s[4] + "," + s[5] + "," + s[6] + "," + s[7]));
            }
        }   
    }

    public static class AgeReducer extends Reducer<Text, Text, Text, Text> {
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
            double sum1 = 0, sum2 = 0, sum3 = 0, sum4 = 0;
            long count1 = 0, count2 = 0, count3 = 0, count4 = 0;

            for (Text val : values) {
                String[] p = val.toString().split(",");
                sum1 += Double.parseDouble(p[0]);
                count1 += Long.parseLong(p[1].split("\\.")[0]);
                sum2 += Double.parseDouble(p[2]);
                count2 += Long.parseLong(p[3].split("\\.")[0]);
                sum3 += Double.parseDouble(p[4]);
                count3 += Long.parseLong(p[5].split("\\.")[0]);
                sum4 += Double.parseDouble(p[6]);
                count4 += Long.parseLong(p[7].split("\\.")[0]);
            }

            String avg1 = (count1 > 0) ? String.format("%.2f", sum1 / count1) : "NA";
            String avg2 = (count2 > 0) ? String.format("%.2f", sum2 / count2) : "NA";
            String avg3 = (count3 > 0) ? String.format("%.2f", sum3 / count3) : "NA";
            String avg4 = (count4 > 0) ? String.format("%.2f", sum4 / count4) : "NA";
            String title = movieTitleMap.getOrDefault(key.toString(), "Unknown Movie");
            context.write(new Text(title), new Text("0-18: " + avg1 + ", 18-35: " + avg2 + ", 35-50: " + avg3 + ", 50+: " + avg4));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Age Analysis");
        job.setJarByClass(ex4.class);
        job.setMapperClass(AgeMapper.class);
        job.setReducerClass(AgeReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.addCacheFile(new URI("hdfs:///input/movies.txt#movies.txt"));
        job.addCacheFile(new URI("hdfs:///input/users.txt#users.txt"));
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}