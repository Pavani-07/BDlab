import org.apache.hadoop.io.DoubleWritable;
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

    public static class TriangleMapper extends Mapper<Object, Text, Text, Text> {
        
        private int hashFunction(int vertex) {
            return (vertex-1)/3+1;
        }
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] vertices = value.toString().split(" ");
            int u = Integer.parseInt(vertices[0]);
            int v = Integer.parseInt(vertices[1]);
            int hU = hashFunction(u);
            int hV = hashFunction(v);
            if (hU == hV) {
                for(int i=1;i<=5;i++){
                    if(i!=hU){
                        context.write(new Text("T2_"+hU+"_"+i),new Text(u+","+v));
                    }
                }
            } else {
                for(int i=1;i<=5;i++){
                    for(int j=i+1;j<=5;j++){
                        for(int k=j+1;k<=5;k++){
                            if((hU==i || hU==j || hU==k) &&( hV==i || hV==j || hV==k)){
                                context.write(new Text("T1_" +i + "_" + j + "_" + k), new Text(u + "," + v));
                            }
                        }
                        if((hU==i || hU==j) &&( hV==i || hV==j)){
                            context.write(new Text("T2_" +i + "_" + j ), new Text(u + "," + v));
                        }
                    }
                }
            }
        }
    }

    public static class TriangleReducer extends Reducer<Text, Text, Text, DoubleWritable> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String[] keyParts = key.toString().split("_");
            String type = keyParts[0];
            if (type.equals("T1")) {
                List<int[]> edges = new ArrayList<>();
                for (Text val : values) {
                    String[] edge = val.toString().split(",");
                    int u = Integer.parseInt(edge[0]);
                    int v = Integer.parseInt(edge[1]);
                    edges.add(new int[]{u, v});
                }
                HashMap<Integer, Set<Integer>> adjList = buildAdjList(edges);
                HashMap<String,Integer> dup=new HashMap<>();
                for (int[] edge : edges) {
                    int u = edge[0];
                    int v = edge[1];
                    if (adjList.containsKey(u)) {
                        for (int w : adjList.get(u)) {
                            if (adjList.get(v) != null && adjList.get(v).contains(w)) {
                                int a=u;
                                int b=v;int c=w;
                                if(u<v && u<w){
                                    a=u;
                                    if(v<w){
                                        b=v;
                                        c=w;
                                    }
                                    else{
                                        b=w;
                                        c=v;
                                    }
                                }
                                else if(u<v && u>w){
                                    a=w;
                                    b=u;
                                    c=v;
                                }
                                else if(u>v && u>w){
                                    c=u;
                                    if(v<w){
                                        a=v;
                                        b=w;
                                    }
                                    else{
                                        a=w;
                                        b=v;
                                    }
                                }
                                String ans=a+" "+b+" "+c;
                                System.out.println(ans);
                                int h1=hashFunction(u);
                                int h2=hashFunction(v);
                                int h3=hashFunction(w);
                        }
                    }
                }
            } 
            else if (type.equals("T2")) {
                Set<String> triangles = new HashSet<>();
                List<int[]> edges = new ArrayList<>();
                for (Text val : values) {
                    String[] edge = val.toString().split(",");
                    int u = Integer.parseInt(edge[0]);
                    int v = Integer.parseInt(edge[1]);
                    edges.add(new int[]{u, v});
                }

                HashMap<Integer, Set<Integer>> adjList = buildAdjList(edges);
                HashMap<String,Integer> dup=new HashMap<>();
                for (int[] edge : edges) {
                    int u = edge[0];
                    int v = edge[1];
                    if (adjList.containsKey(u)) {
                        for (int w : adjList.get(u)) {
                            if (adjList.get(v) != null && adjList.get(v).contains(w)) {
                                int a=u;
                                int b=v;int c=w;
                                if(u<v && u<w){
                                    a=u;
                                    if(v<w){
                                        b=v;
                                        c=w;
                                    }
                                    else{
                                        b=w;
                                        c=v;
                                    }
                                }
                                else if(u<v && u>w){
                                    a=w;
                                    b=u;
                                    c=v;
                                }
                                else if(u>v && u>w){
                                    c=u;
                                    if(v<w){
                                        a=v;
                                        b=w;
                                    }
                                    else{
                                        a=w;
                                        b=v;
                                    }
                                }
                                String ans=a+" "+b+" "+c;
                                System.out.println(ans+" in type 2");
                                if(!dup.containsKey(ans)){
                                    dup.put(ans,1);
                                    triangles.add(a + "," + b + "," + c);
                                }
                                
                            }
                        }
                    }
                }
                for (String triangle : triangles) {
                    String[] vertex=triangle.split(",");
                    int h1=hashFunction(Integer.parseInt(vertex[0]));
                    int h2=hashFunction(Integer.parseInt(vertex[1]));
                    int h3=hashFunction(Integer.parseInt(vertex[2]));
                    if(h1!=h2 && h2!=h3 && h3!=h1){
                        continue;
                    }
                    else if (h1==h2 && h2==h3 && h3==h1){
                        context.write(new Text(triangle), new DoubleWritable(0.25));
                    }
                    else{
                        context.write(new Text(triangle), new DoubleWritable(1));
                    }
                }
            }
        }

        private HashMap<Integer, Set<Integer>> buildAdjList(List<int[]> edges) {
            HashMap<Integer, Set<Integer>> adjList = new HashMap<>();
            for (int[] edge : edges) {
                adjList.computeIfAbsent(edge[0], k -> new HashSet<>()).add(edge[1]);
                adjList.computeIfAbsent(edge[1], k -> new HashSet<>()).add(edge[0]);
            }
            return adjList;
        }

        private int hashFunction(int vertex) {
            return (vertex-1)/3+1;
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Triangle Count");
        job.setJarByClass(App.class);
        job.setMapperClass(TriangleMapper.class);
        job.setReducerClass(TriangleReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path("/input"));
        FileOutputFormat.setOutputPath(job, new Path("/op1"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
