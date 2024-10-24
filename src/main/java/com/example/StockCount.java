package com.example;

import com.opencsv.CSVReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.io.StringReader; // 导入 StringReader
import java.util.StringTokenizer;
import java.util.TreeMap;

public class StockCount {

    // Mapper类
    public static class StockMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final IntWritable one = new IntWritable(1);
        private Text stockCode = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // 跳过CSV的header
            String line = value.toString();
            if (line.startsWith("index")) return;

            try (CSVReader csvReader = new CSVReader(new StringReader(line))) { // 使用 StringReader 来解析行
                String[] columns = csvReader.readNext(); // 解析当前行

                if (columns != null && columns.length > 3) {
                    stockCode.set(columns[3]); // 提取stock列
                    context.write(stockCode, one);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    // Reducer类
    public static class StockReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();
        private TreeMap<Integer, String> stockMap = new TreeMap<>();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            // 将结果放入TreeMap以便排序，按次数降序排列
            stockMap.put(sum, key.toString());
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            int rank = 1;
            for (Integer count : stockMap.descendingKeySet()) {
                context.write(new Text(rank + "：" + stockMap.get(count)), new IntWritable(count));
                rank++;
            }
        }
    }

    // Driver类
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "stock count");

        job.setJarByClass(StockCount.class);
        job.setMapperClass(StockMapper.class);
        job.setReducerClass(StockReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
