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

// kỹ thuật map-side join
// mapper
class GenreMapper extends Mapper<LongWritable, Text, Text, Text> {
    private Map<String, String[]> movieGenresMap = new HashMap<>();
    private Map<String, double[]> combinedData = new HashMap<>();

    @Override
    protected void setup(Context context) throws IOException {
        // Đọc file từ Distributed Cache (đã được Hadoop tải về máy cục bộ của Mapper)
        URI[] cacheFiles = context.getCacheFiles();
        if (cacheFiles != null && cacheFiles.length > 0) {
            // "movies.txt" là tên định danh (symlink) chúng ta đặt ở hàm main
            BufferedReader reader = new BufferedReader(new FileReader("movies.txt"));
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(",");
                if (parts.length >= 3) {
                    movieGenresMap.put(parts[0].trim(), parts[2].trim().split("\\|"));
                }
            }
            reader.close();
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) {
        String[] parts = value.toString().split(",");
        if (parts.length >= 3) {
            String movieId = parts[1].trim();
            try {
                double rating = Double.parseDouble(parts[2].trim());
                if (movieGenresMap.containsKey(movieId)) {
                    for (String genre : movieGenresMap.get(movieId)) {
                        double[] stats = combinedData.getOrDefault(genre, new double[2]);
                        stats[0] += rating;
                        stats[1] += 1;
                        combinedData.put(genre, stats);
                    }
                }
            } catch (NumberFormatException e) {
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (Map.Entry<String, double[]> entry : combinedData.entrySet()) {
            String outputValue = entry.getValue()[0] + "," + (int)entry.getValue()[1];
            context.write(new Text(entry.getKey()), new Text(outputValue));
        }
    }
}

// ================= REDUCER =================
class GenreReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) 
            throws IOException, InterruptedException {
        double totalSum = 0;
        long totalCount = 0;

        for (Text val : values) {
            String[] parts = val.toString().split(",");
            totalSum += Double.parseDouble(parts[0]);
            totalCount += Long.parseLong(parts[1]);
        }

        double average = totalSum / totalCount;
        String result = String.format("Avg: %.2f, Count: %d", average, totalCount);
        context.write(key, new Text(result));
    }
}

// ================= MAIN JOB =================
public class ex2 {
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: ex2 <input_path> <output_path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Genre Rating Analysis");
        
        job.setJarByClass(ex2.class);   
        job.setMapperClass(GenreMapper.class);
        job.setReducerClass(GenreReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // ĐẶC BIỆT: Lấy file movies.txt từ HDFS /input/ đưa vào Cache
        // #movies.txt là tạo một symlink để trong code Java chỉ cần gọi FileReader("movies.txt")
        job.addCacheFile(new URI("hdfs:///input/movies.txt#movies.txt"));

        // Truyền đường dẫn ratings (ví dụ: /input/ratings_*.txt)
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}