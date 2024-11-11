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

    public static class MyMapper1 extends Mapper<Object, Text, Text, Text> {
        
        //private ArrayList<String> config=new ArrayList<>();

        @Override
        public void map(Object key, Text value, Context context) throws IOException,InterruptedException{
            String[] k={"j,i,h",
                	"p,n,m,d,c,b,a",
                	"o"
                };
            ArrayList<String> s=new ArrayList<>();
            String[] v=value.toString().split(":");
            String[] doc=v[1].split(" ");
            for(String word:doc){
                for(int i=0;i<k.length;i++){
                    String[] sortword=k[i].split(",");
                    int c=0;
                    for(int j=0;j<sortword.length;j++){
                        if(word.equals(sortword[j])){
                            s.add(i+":"+word);
                        }
                    }
                }
            }
            System.out.println(s+" printing s");
            Collections.sort(s,(c1,c2)->{
                return Integer.compare(Integer.parseInt(""+c2.charAt(0)),Integer.parseInt(""+c1.charAt(0)));
            });
            String d="";
            for(int i=0;i<s.size();i++){
                String word=s.get(i).split(":")[1];
                d+=word+" ";
            }
            System.out.println(d+"idurhobthoirbroihb");
            int preLen=2;
            String[] ans=d.split(" ");
            for(int i=0;i<2;i++){
                System.out.println(ans[i]);
                context.write(new Text(ans[i]),new Text(v[0]+":"+d));
            }
        }
        
    }

    public static class MyReducer1 extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            ArrayList<String> docs=new ArrayList<>();
            for(Text t:values){
                docs.add(t.toString());
            }
            for(int i=0;i<docs.size();i++){
                for(int j=i+1;j<docs.size();j++){
                    boolean p=passesfilter(docs.get(i),docs.get(j));
                    System.out.println(p+"filter");
                    if(p){
                        context.write(new Text(docs.get(i).split(":")[0]+","+docs.get(j).split(":")[0]+"0.4"),new Text(" "));
                    }
                }
            }
        }
        public boolean passesfilter(String d1,String d2){
            String[] ans1=d1.split(":")[1].split(" ");
            String[] ans2=d2.split(":")[1].split(" ");
            double l1=ans1.length;
            double l2=ans2.length;
            if(l1/l2<=0.4){
                return false;
            }
            for(int i=0;i<2;i++){
                if(!ans1[i].equals(ans2[i])) return false;
            }
            ArrayList<String> suff1=new ArrayList<>();
            ArrayList<String> suff2=new ArrayList<>();
            for(int i=2;i<ans1.length;i++){
                suff1.add(ans1[i]);
            }
            for(int i=2;i<ans2.length;i++){
                suff2.add(ans2[i]);
            }
            suff1.retainAll(suff2);
            if(suff1.size()/Math.max(l1,l2)<0.4) return false;
            return true;
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job1 = Job.getInstance(conf, "Mapp1");
        job1.setJarByClass(App.class);
        job1.setMapperClass(MyMapper1.class);
        job1.setReducerClass(MyReducer1.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job1, new Path("/inpt1"));
        FileOutputFormat.setOutputPath(job1, new Path("/stage2"));

        System.exit(job1.waitForCompletion(true) ? 0 : 1);
    }
}
