import org.apache.hadoop.io.IntWritable;
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

    public static class MyMapper extends Mapper<Object, Text, Text, Text> {
        

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            int sq=8;
            String[] k=value.toString().split(" ");
            int low=Integer.parseInt(k[1]);
            int left=Integer.parseInt(k[2]);
            for(int i=(low/2)*2;i<sq;i+=2){
                for(int j=(left/2)*2;j<sq;j+=2){
                    context.write(new Text(i+" "+j), value);
                }
            }
        }
    }

    public static class MyReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            ArrayList<String> Pc=new ArrayList<>();
            ArrayList<String> Qc=new ArrayList<>();
            ArrayList<String> Rc=new ArrayList<>();
            ArrayList<String> Sc=new ArrayList<>();
            for(Text t:values){
                String[] k=t.toString().split(" ");
                if(k[0].charAt(0)=='P'){
                    Pc.add(t.toString());
                }
                else if(k[0].charAt(0)=='Q'){
                    Qc.add(t.toString());
                }
                else if(k[0].charAt(0)=='R'){
                    Rc.add(t.toString());
                }
                else if(k[0].charAt(0)=='S'){
                    Sc.add(t.toString());
                }
            }
            for(String p:Pc){
                for(String q:Qc){
                    for(String r:Rc){
                        for(String s:Sc){
                            if(overlap(p,q)&& overlap(q,r) && overlap(r,s)){
                                String[] point=key.toString().split(" ");
                                int leftc=Integer.parseInt(point[0]);
                                int bttmc=Integer.parseInt(point[1]);
                                int rightc=leftc+2;
                                int topc=bttmc+2;
                                String m="kwjrfb"+" "+leftc+" "+bttmc+" "+rightc+" "+topc;
                                String[] l=s.split(" ");
                                int rights=Integer.parseInt(l[3]);
                                int tops=Integer.parseInt(l[4]);
                                if(leftc<=rights && rights<=rightc && bttmc<=tops && tops<=topc){
                                    context.write(new Text(m),new Text(p+" "+q+" "+r+" "+s));
                                }
                            }
                        }
                    }
                }
            }

        }
        public static boolean overlap(String a, String b){
            String[] k1=a.split(" ");
            String[] k2=b.split(" ");
            int x1=Integer.parseInt(k1[1]);
            int y1=Integer.parseInt(k1[2]);
            int x2=Integer.parseInt(k1[3]);
            int y2=Integer.parseInt(k1[4]);

            int x3=Integer.parseInt(k2[1]);
            int y3=Integer.parseInt(k2[2]);
            int x4=Integer.parseInt(k2[3]);
            int y4=Integer.parseInt(k2[4]);

            boolean x=x2<x3 || x1>x4;
            boolean y=y2<y3 || y1>y4;

            return !(x||y);

        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Rectangle");
        job.setJarByClass(App.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path("/allin"));
        FileOutputFormat.setOutputPath(job, new Path("/allout"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
