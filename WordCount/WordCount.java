import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class App {

    public static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
            String [] words = value.toString().split("\\s+");
            Text newKey = new Text();
            IntWritable newVal = new IntWritable(1);
            for(String word: words){
                if(!word.isEmpty()){
                    newKey.set(word);
                    context.write(newKey, newVal);
                }
            }
        }
    
    }
    public static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
            int sum = 0;
            for(IntWritable val: values){
                sum += val.get();
            }
            IntWritable newVal = new IntWritable(sum);
            context.write(key, newVal);
        }
    }
    
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Custom Partitioner");
        job.setJarByClass(App.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path("/input"));
        FileOutputFormat.setOutputPath(job, new Path("/output"));
        System.exit(job.waitForCompletion(true)?0:1);

    }
}
