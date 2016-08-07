package edu.fzu.gsr.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


/**
 * Created by Gsr on 2016/8/7.
 */
public class WordCount {
    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        WordCount wc = new WordCount();
        String inputStringPath = "E:\\GSRKLIVE\\Project\\WorkSpaceGsr\\testhadoop2\\data\\input";
        String outputStringPath = "E:\\GSRKLIVE\\Project\\WorkSpaceGsr\\testhadoop2\\data\\out7";
        wc.run(inputStringPath,outputStringPath);

    }
    public void run(String inputStringPath,String outputStringPath) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration hadoopConf = new Configuration();

//        hadoopConf.set("mapred.job.tracker","");//集群上要配置jobtracker 地址
        Job job = Job.getInstance(hadoopConf);
        job.setJobName("word count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(WordCountMap.class);
        job.setReducerClass(WordCountReduce.class);

        //设置Mapper类的输出key-value类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        //设置combiner
        job.setCombinerClass(WordCountReduce.class);
//        job.setNumReduceTasks(0);//没有reduce
        //设置Reducer类的输出key-value类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        Path inputPath = new Path(inputStringPath);
        Path outputPath = new Path(outputStringPath);
        FileInputFormat.setInputPaths(job,inputPath);
        FileOutputFormat.setOutputPath(job,outputPath);
        //提交本次作业，并打印出详细信息
        job.waitForCompletion(true);

    }

}
class WordCountMap extends Mapper<LongWritable, Text, Text, LongWritable> {
    protected void setup(Context context) throws IOException, InterruptedException {

    }
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String []words = line.split(" ");
        for(String word:words){
            context.write(new Text(word),new LongWritable(1L));
        }
    }
    protected void cleanup(Context context) throws IOException, InterruptedException {

    }
}
class WordCountReduce extends Reducer<Text,LongWritable,Text,LongWritable>{
    protected void reduce(Text key,Iterable<LongWritable> values,Context context) throws IOException, InterruptedException {
        long sum = 0;
        for(LongWritable value: values){
            sum += value.get();
        }
        context.write(key,new LongWritable(sum));
    }
}
