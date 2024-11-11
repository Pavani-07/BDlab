import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import java.util.*;
import java.io.*;

public class App {

    public static class MyMapper1 extends Mapper<Object, Text, Text, IntWritable> {
        
        @Override
        public void map(Object key, Text value, Context context) throws IOException,InterruptedException{
            String[] k=value.toString().split(" ");
            for(String m:k){
                context.write(new Text(m),new IntWritable(1));
            }
        }
        
    }

    public static class MyReducer1 extends Reducer<Text, IntWritable, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int count=0;
            for(IntWritable m:values){
                count++;
            }
            context.write(key,new Text(""+count));
        }
    }

    public static class MyMapper2 extends Mapper<LongWritable, Text, Text, Text> {
        
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException{
            String[] s=value.toString().split("\\s+");
            context.write(new Text(s[1]),new Text(s[0]));
        }
        
    }

    public static class MyReducer2 extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String s="";
            for(Text m:values){
                s+=m.toString()+",";
            }
            context.write(key,new Text(s));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf, "Mapp1");
        job1.setJarByClass(App.class);
        job1.setMapperClass(MyMapper1.class);
        job1.setReducerClass(MyReducer1.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(IntWritable.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job1, new Path("/inp1"));
        FileOutputFormat.setOutputPath(job1, new Path("/opt1"));

        // System.exit(!job1.waitForCompletion(true) ? 0 : 1);
        boolean s=job1.waitForCompletion(true);
        if(!s){
            System.exit(1);
        }

        Job job2 = Job.getInstance(conf, "Mapp2");
        job2.setJarByClass(App.class);
        job2.setMapperClass(MyMapper2.class);
        job2.setReducerClass(MyReducer2.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job2, new Path("/opt1"));
        FileOutputFormat.setOutputPath(job2, new Path("/opt2"));

        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}
