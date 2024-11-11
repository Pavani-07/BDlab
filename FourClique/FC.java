import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.HashSet;
import java.util.Collections;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class FC {

    public static class AdjacencyListMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text vertex = new Text();
        private Text neighbor = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] vertices = value.toString().split(",");
            if (vertices.length == 2) {
                String vertex1 = vertices[0];
                String vertex2 = vertices[1];
                int u = Integer.parseInt(vertex1);
                int v = Integer.parseInt(vertex2);
                if(v>u){
                    vertex.set(vertex1);
                    neighbor.set(vertex2);
                    context.write(vertex, neighbor);
                }
            }
        }
    }

    public static class AdjacencyListReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<String> neighbors = new ArrayList<>();
            for (Text val : values) {
                neighbors.add(val.toString());
            }
            neighbors = new ArrayList<>(new HashSet<>(neighbors));
            Collections.sort(neighbors);
            for(int i=0;i<neighbors.size();i++){
                for(int j=i+1;j<neighbors.size();j++){
                    String v1 = neighbors.get(i)+","+neighbors.get(j);
                    Text vt = new Text(v1+"#p1");
                    context.write(key, vt);
                    for(int k=j+1;k<neighbors.size();k++){
                        String v2 = v1 + "," + neighbors.get(k);
                        context.write(key,new Text(v2+"#p0"));
                    }
                }
            }
        }
    }

    public static class Mapper2 extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] parts = value.toString().trim().split("\\t");
        // String val = parts[0]+","
            if (parts.length == 2) {
                if(parts[1].endsWith("#p0")){
                    String[] nbrs = parts[1].split(",");
                    context.write(new Text(nbrs[0]),new Text(parts[0]+","+parts[1]));
                }else if(parts[1].endsWith("#p1")){
                    context.write(new Text(parts[0]),new Text(parts[0]+","+parts[1]));
                }
            }
        }
    }

    public static class Reducer2 extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            List<String> l1 = new ArrayList<>();
            List<String> l2 = new ArrayList<>();
            for (Text val : values) {
                String v = val.toString();
                if(v.endsWith("#p0")){
                    l1.add(v);
                }else{
                    l2.add(v);
                }
            }
            for(String rec1 : l1){
                for(String rec2 : l2){
                    String[] p0 = rec1.split(",");
                    String[] p1 = rec2.split(",");
                    if(p0[2].compareTo(p1[1])==0){
                        String[] sp0 = p0[3].split("#");
                        String[] sp1 = p1[2].split("#");
                        if(sp0[0].compareTo(sp1[0])==0){
                            context.write(new Text(p0[2]),new Text(rec1+"$"+rec2));
                        }
                    }
                }
            }
        }
    }

    public static class Mapper3 extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            if(line.length()>3){
                String[] parts = line.split("\\t");
                context.write(new Text(parts[0]),new Text(parts[1]));
            }else{
                String[] parts = line.split(",");
                context.write(new Text(parts[0]),new Text(parts[0]+","+parts[1]+",#p2"));
            }
        }
    }

    public static class Reducer3 extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            List<String> l1 = new ArrayList<>();
            List<String> l2 = new ArrayList<>();
            for (Text val : values) {
                String v = val.toString();
                System.out.println(v + " " + v.length());
                if(v.length()>7){
                    l1.add(v);
                }else{
                    l2.add(v);
                }
            }
            for(String rec1 : l1){
                for(String rec2 : l2){
                    String[] p0 = rec1.split("$");
                    String[] p1 = p0[0].split(",");
                    String[] p2 = p1[3].split("#");
                    String[] r1 = rec2.split(",");
                    if(p2[0].compareTo(r1[1])==0){
                        context.write(new Text(p2[0]),new Text(rec1+"$"+rec2));
                    }
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job1 = Job.getInstance(conf, "Adjacency List Construction");
        job1.setJarByClass(FC2.class);
        job1.setMapperClass(AdjacencyListMapper.class);
        job1.setReducerClass(AdjacencyListReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job1, new Path("/fqinput2"));
        FileOutputFormat.setOutputPath(job1, new Path("/fqou1"));
        boolean success = job1.waitForCompletion(true);
        if (!success) {
            System.exit(1);
        }
        Job job2 = Job.getInstance(conf, "Four Clique 2");
        job2.setJarByClass(FC2.class);
        job2.setMapperClass(Mapper2.class);
        job2.setReducerClass(Reducer2.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job2, new Path("/fqou1"));
        FileOutputFormat.setOutputPath(job2, new Path("/fqou2"));
        success = job2.waitForCompletion(true);
        // System.exit(success ? 0 : 1);
        if (!success) {
            System.exit(1);
        }

        Job job3 = Job.getInstance(conf, "Four Clique 3");
        job3.setJarByClass(FC2.class);
        // job3.setMapperClass(Mapper3.class);
        job3.setReducerClass(Reducer3.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);
        // FileInputFormat.addInputPath(job3, new Path("/fqinput1"));
        MultipleInputs.addInputPath(job3, new Path("/fqou2"), TextInputFormat.class, Mapper3.class);
        MultipleInputs.addInputPath(job3, new Path("/fqinput2"), TextInputFormat.class, Mapper3.class);
        FileOutputFormat.setOutputPath(job3, new Path("/fqou3"));
        success = job3.waitForCompletion(true);
        System.exit(success ? 0 : 1);
    }
}
