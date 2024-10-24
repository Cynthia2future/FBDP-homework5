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

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.HashSet;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.regex.Pattern;
import java.util.Set;

public class WordCount {

    // Mapper类
    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private Set<String> stopWords = new HashSet<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            // 获取stop-word文件路径
            String stopWordFilePath = conf.get("stopWordFilePath");
            
            // 读取stop-word文件，存入HashSet
            BufferedReader reader = new BufferedReader(new FileReader(stopWordFilePath));
            String stopWord;
            while ((stopWord = reader.readLine()) != null) {
                stopWords.add(stopWord.trim().toLowerCase());
            }
            reader.close();
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // 使用OpenCSV解析CSV
            String line = value.toString();
            if (line.startsWith("index")) return;

            try (CSVReader csvReader = new CSVReader(new StringReader(line))) {
                String[] columns = csvReader.readNext();
                if (columns.length > 1) {
                    String headline = columns[1];  // 提取headline列
                    // 正则表达式去除标点符号，忽略大小写
                    String cleanedHeadline = headline.replaceAll("[^a-zA-Z ]", " ").toLowerCase();
                    StringTokenizer itr = new StringTokenizer(cleanedHeadline);

                    // 逐个单词处理
                    while (itr.hasMoreTokens()) {
                        String token = itr.nextToken();
                        if (!stopWords.contains(token)) {
                            word.set(token);
                            context.write(word, one);
                        }
                    }
                }
            } catch (com.opencsv.exceptions.CsvValidationException e) {
                e.printStackTrace(); // 打印异常堆栈信息，或者根据需求进行其他处理
            }
        }
    }

    // Reducer类
    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private TreeMap<Integer, String> wordCountMap = new TreeMap<>();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            // 将结果放入TreeMap以便排序，按出现次数降序排列
            wordCountMap.put(sum, key.toString());
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            int rank = 1;
            // 输出前100个高频单词
            for (Map.Entry<Integer, String> entry : wordCountMap.descendingMap().entrySet()) {
                if (rank > 100) break;
                context.write(new Text(rank + "：" + entry.getValue()), new IntWritable(entry.getKey()));
                rank++;
            }
        }
    }

    // Driver类
    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: WordCount <input path> <output path> <stop-word file path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        // 将stop-word文件路径传递给Mapper
        conf.set("stopWordFilePath", args[2]);

        Job job = Job.getInstance(conf, "word count top 100");

        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
