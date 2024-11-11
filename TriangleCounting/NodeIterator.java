import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TC1 {

    public static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
            String [] nodes = value.toString().split(",");
            int v1 = Integer.parseInt(nodes[0]);
            int v2 = Integer.parseInt(nodes[1]);
            
            if(v2>v1){
                Text newKey = new Text(nodes[0]);
                Text newValue = new Text(nodes[1]);
                context.write(newKey,newValue);
            }
        }
    
    }
    public static class MyReducer extends Reducer<Text, Text, Text, Text>{
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
            ArrayList<Integer> arr = new ArrayList<Integer>();
            for(Text val: values){
                String temp = val.toString();
                int t = Integer.parseInt(temp);
                arr.add(t);
            }
            arr.sort(null);
            System.out.println(arr.size());
            for(int i = 0 ;i<arr.size();i++)
            {
                for(int j=i+1; j< arr.size();j++){
                    String s1 = Integer.toString(arr.get(i));
                    String s2 = Integer.toString(arr.get(j));
                    String ans = s1+","+s2;
                    context.write(key, new Text(ans));
                }

            }
        }
    }
  public static class MyMapper1 extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
            String line = value.toString();
            System.out.println(line.length());
            if(line.length() > 3){
                String [] nodes = line.split("\\s+");
                Text key1 = new Text(nodes[1]);
                Text value1 = new Text(nodes[0]);
                System.out.println(nodes[1]);
                System.out.println(nodes[0]);
                context.write(key1,value1);
            }else{
                context.write(value,new Text("$"));
                System.out.println(value);
                System.out.println("$");
            }
        }
    
    }
    public static class MyReducer1 extends Reducer<Text, Text, Text, Text>{
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
            for(Text val: values){
                String v = val.toString();
                if(v.compareTo("$")==0){
                    System.out.println(key);
                    for(Text v2 : values){
                        String v3 = v2.toString();
                        if(v3.compareTo("$")!=0){
                            System.out.println(v3);
                            context.write(v2,key);
                        }
                    }
                    break;
                }
            }
        }
    }
  public static void main(String[] args){

  }
}
