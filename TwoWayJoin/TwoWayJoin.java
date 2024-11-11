import java.io.IOException;
import java.util.ArrayList;
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
//import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
public class TwoWayJoin {

    public static class RMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
      
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
            String[] words = value.toString().split(",");
            int a = Integer.parseInt(words[0]);
            IntWritable newKey = new IntWritable(Integer.parseInt(words[1])) ;
            context.write(newKey,new Text(value.toString()+ "#R"));
         } 
       }

    
    public static class SMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
      
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
            
            String[] words = value.toString().split(",");
            int a = Integer.parseInt(words[0]);
            IntWritable newKey = new IntWritable(Integer.parseInt(words[0])) ;
            context.write(newKey,new Text(value.toString()+ "#S")); 
       }
    }    

    public static class MyReducer extends Reducer<IntWritable, Text, IntWritable, Text>{
        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
            ArrayList<String> RList = new ArrayList<String>();
            ArrayList<String> SList = new ArrayList<String>();
            for(Text val: values){
                if(val.toString().endsWith("#R")){
                    RList.add(val.toString());
                }else{
                    SList.add(val.toString());
                }
            }
            for(int i = 0 ;i<RList.size();i++)
            {
                for(int j=0; j< SList.size();j++){
                    context.write(key, new Text(RList.get(i)+","+ SList.get(j)));
                }

            }
        }
    }
    public static class MyPartitioner extends Partitioner<Text, IntWritable>{

        @Override
        public int getPartition(Text key, IntWritable value, int numPartitions) {
            // TODO Auto-generated method stub
            if(numPartitions==0){
                return 0;
            } else{
                Pattern pat1 = Pattern.compile("^[a-iA-I]+");
                Pattern pat2 = Pattern.compile("^[j-rJ-R]+");
                Matcher mat1 = pat1.matcher(key.toString());
                Matcher mat2 = pat2.matcher(key.toString());
                
                if(mat1.find()){
                    return 0;
                }else if (mat2.find()){
                    return 1;
                }else{
                    return 2;
                }
            }

        }
        
    }
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "WC");
        job.setJarByClass(WC.class);
        //job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class); 
        job.setNumReduceTasks(3);
       // job.setPartitionerClass(MyPartitioner.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        MultipleInputs.addInputPath(job, new Path("/RInput"), TextInputFormat.class, RMapper.class);
        MultipleInputs.addInputPath(job, new Path("/SInput"), TextInputFormat.class, SMapper.class);
        //FileInputFormat.addInputPath(job, new Path("/input"));
        FileOutputFormat.setOutputPath(job, new Path("/output"));
        System.exit(job.waitForCompletion(true)?0:1);
        
    }
}
