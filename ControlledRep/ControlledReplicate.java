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

    public static class Mapper1 extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] k = value.toString().split(",");
            int x1 = Integer.parseInt(k[1]);
            int y1 = Integer.parseInt(k[2]);
            int x2 = Integer.parseInt(k[3]);
            int y2 = Integer.parseInt(k[4]);
            for (int i = (x1 / 2); i <= (x2 / 2); i++) {
                for (int j = (y1 / 2); j <= (y2 / 2); j++) {
                    context.write(new Text(i + "," + j), value);
                }
            }
        }
    }

    public static boolean crossesBoundary(String rect) {
        String[] s = rect.split(",");

        int x1 = Integer.parseInt(s[1]);
        int y1 = Integer.parseInt(s[2]);
        int x2 = Integer.parseInt(s[3]);
        int y2 = Integer.parseInt(s[4]);

        int lx = x1 / 2;
        int ly = y1 / 2;
        int rx = x2 / 2;
        int ry = y2 / 2;

        if(lx!=rx || ly!=ry) return true;
        return false;
    }
        
    public static boolean overlap(String a, String b){
        String[] k1=a.split(",");
        String[] k2 = b.split(",");
        
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
    public static class Reducer1 extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String[] k = key.toString().split(",");

            int lc = Integer.parseInt(k[0]);
            int rc = Integer.parseInt(k[1]);

            ArrayList<String> Pc=new ArrayList<>();
            ArrayList<String> Qc=new ArrayList<>();
            ArrayList<String> Rc=new ArrayList<>();
            ArrayList<String> Sc = new ArrayList<>();
            HashSet<String> marked = new HashSet<>();

            for(Text t : values){
                String v = t.toString();
                if(v.startsWith("P")){
                    Pc.add(v);
                }
                if(v.startsWith("Q")){
                    Qc.add(v);
                }
                if(v.startsWith("R")){
                    Rc.add(v);
                }
                if(v.startsWith("S")){
                    Sc.add(v);
                }
            }
            for (String q : Qc) {
                for (String r : Rc) {
                    if (overlap(q, r)) {
                        if (!crossesBoundary(q) && !crossesBoundary(r)) {
                            for (String p : Pc) {
                                if (overlap(p, q)) {
                                    for (String s : Sc) {
                                        if (overlap(r, s)) {
                                            context.write(new Text("Joined"),
                                                    new Text(p + "$" + q + "$" + r + "$" + s));
                                        }
                                    }
                                }
                            }
                        } else {
                            for (String p : Pc) {
                                if (overlap(p, q)) {
                                    marked.add(p);
                                }
                            }
                            for (String s : Sc) {
                                if (overlap(r, s)) {
                                    marked.add(s);
                                }
                            }
                            marked.add(q);
                            marked.add(r);
                        }
                    }
                }
            }
            for (String q : Qc) {
                if (crossesBoundary(q)) {
                    for (String p : Pc) {
                        if (overlap(p, q)) {
                            marked.add(p);
                        }
                    }
                }
            }
            for (String r : Rc) {
                if (crossesBoundary(r)) {
                    for (String s : Sc) {
                        if (overlap(r, s)) {
                            marked.add(s);
                        }
                    }
                }
            }
            for (String p : Pc) {
                if (crossesBoundary(p)) {
                    marked.add(p);
                }
            }
            for (String s : Sc) {
                if (crossesBoundary(s)) {
                    marked.add(s);
                }
            }
            for (String p : marked) {
                String[] s = p.split(",");

                int x1 = Integer.parseInt(s[1]);
                int y1 = Integer.parseInt(s[2]);

                // if ((x1 / 2) == lc && (y1 / 2 ) == rc) {
                    context.write(new Text("Process"), new Text(p));
                // }
            }
        }
    }

    public static class Mapper2 extends Mapper<Object, Text, Text, Text> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            int end = 10;
            String[] k = value.toString().split("\\s+");
            System.out.println("Maper2out" + k[1]);
            if (k[0].compareTo("Process") == 0) {
                System.out.println("Maper2" + k[1]);
                String[] parts = k[1].split(",");
                int x1 = Integer.parseInt(parts[1]);
                int y1 = Integer.parseInt(parts[2]);
                for(int i=(x1/2)*2;i<end;i+=2){
                    for(int j=(y1/2)*2;j<end;j+=2){
                        context.write(new Text(i+","+j), new Text(k[1]));
                    }
                }
            }
        }
    }

    public static class Reducer2 extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            HashSet<String> Pc = new HashSet<>();
            HashSet<String> Qc = new HashSet<>();
            HashSet<String> Rc = new HashSet<>();
            HashSet<String> Sc = new HashSet<>();
            for(Text t : values){
                String v = t.toString();
                if(v.startsWith("P")){
                    Pc.add(v);
                }
                if(v.startsWith("Q")){
                    Qc.add(v);
                }
                if(v.startsWith("R")){
                    Rc.add(v);
                }
                if(v.startsWith("S")){
                    Sc.add(v);
                }
            }
            for(String p:Pc){
                for(String q:Qc){
                    if(overlap(p, q)){
                        for(String r:Rc){
                            if(overlap(q, r)){
                                for(String s:Sc){
                                    if(overlap(r,s)){
                                        String[] point = key.toString().split(",");
                                        int xc1=Integer.parseInt(point[0]);
                                        int yc1=Integer.parseInt(point[1]);
                                        int xc2=xc1+2;
                                        int yc2=yc1+2;
                                        String cell = "Cell"+" ("+xc1+","+yc1+") ("+xc2+" "+yc2+")";
                                        String[] coords = s.split(",");
                                        int xs2 = Integer.parseInt(coords[3]);
                                        int ys2 = Integer.parseInt(coords[4]);
                                        if(xc1<=xs2 && xs2<=xc2 && yc1<=ys2 && ys2<=yc2){
                                            context.write(new Text(cell),new Text(p+"$"+q+"$"+r+"$"+s));
                                        }
                                    }
                                }
                            }
                            
                        }
                    }
                    
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Rectangle");
        job.setJarByClass(App.class);
        job.setMapperClass(Mapper1.class);
        job.setReducerClass(Reducer1.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path("/inp2"));
        FileOutputFormat.setOutputPath(job, new Path("/jout1"));

        boolean success = job.waitForCompletion(true);

        if (!success) {
            System.exit(1);
        } 

        Job job2 = Job.getInstance(conf, "Replicate");
        job2.setJarByClass(App.class);
        job2.setMapperClass(Mapper2.class);
        job2.setReducerClass(Reducer2.class);
        // job2.setMapOutputKeyClass(Text.class);
        // job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job2, new Path("/jout1"));
        FileOutputFormat.setOutputPath(job2, new Path("/jout2"));

        success = job2.waitForCompletion(true);
        System.exit(success ? 0 : 1);


    }
}
