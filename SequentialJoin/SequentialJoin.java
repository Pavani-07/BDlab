import java.io.*;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class App {
    public static ArrayList<String> split(String region) {
        String[] corners = region.split(",");
        int[] lbCor = new int[]{Integer.parseInt(corners[0]), Integer.parseInt(corners[1])};
        int[] rtCor = new int[]{Integer.parseInt(corners[2]), Integer.parseInt(corners[3])};
        ArrayList<String> cells = new ArrayList<>();
        
        int xcl = (int)Math.floor((float)lbCor[0] / 16) * 16;
        int ycb = (int)Math.floor((float)lbCor[1] / 16) * 16;
        int xcr = (int)Math.ceil((float)rtCor[0] / 16) * 16;
        int yct = (int)Math.ceil((float)rtCor[1] / 16) * 16;
        
        for (int xl = xcl; xl < xcr; xl += 16) {
            for (int yb = ycb; yb < yct; yb += 16) {
                cells.add(xl + "," + yb + "," + (xl + 16) + "," + (yb + 16));
            }
        }
        
        return cells;
    }
    
    
    public static boolean OverlapAndDuplicateAvoidance(String p, String q, String c) {
        String[] cor1 = p.split(",");
        String[] cor2 = q.split(",");
        int[] Pc1 = new int[]{Integer.parseInt(cor1[0]), Integer.parseInt(cor1[1])};
        int[] Pc2 = new int[]{Integer.parseInt(cor1[2]), Integer.parseInt(cor1[3])};
        int[] Qc1 = new int[]{Integer.parseInt(cor2[0]), Integer.parseInt(cor2[1])};
        int[] Qc2 = new int[]{Integer.parseInt(cor2[2]), Integer.parseInt(cor2[3])};
        
        int overlapX1 = Math.max(Pc1[0], Qc1[0]);
        int overlapY1 = Math.max(Pc1[1], Qc1[1]);
        int overlapX2 = Math.min(Pc2[0], Qc2[0]);
        int overlapY2 = Math.min(Pc2[1], Qc2[1]);
        
        if (overlapX1 < overlapX2 && overlapY1 < overlapY2) {
            String[] cor3 = c.split(",");
            int C_left = Integer.parseInt(cor3[0]);
            int C_bottom = Integer.parseInt(cor3[1]);
            int C_right = Integer.parseInt(cor3[2]);
            int C_top = Integer.parseInt(cor3[3]);
            
            
            if (overlapX2 <= C_right && overlapX2 >= C_left && overlapY2 <= C_top && overlapY2 >= C_bottom) {
                return true;
            } else {
                return false;
            }
            
        } else {
            return false;
        }
    }
    
    

    
    public static class SJMapper1 extends Mapper<LongWritable, Text ,Text, Text>{
        public void map(LongWritable id, Text value, Context context) throws IOException,InterruptedException{
            String[] entries = value.toString().split("@");
            String regType = entries[1];
            ArrayList<String> Cp = split(entries[0]);
            for(String c : Cp){
                context.write(new Text(c), new Text(entries[0]+"@"+regType));
            }
        }
    }

    public static class SJReducer1 extends Reducer<Text, Text, NullWritable, Text>{
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
            String c = key.toString();
            ArrayList<String> Pc = new ArrayList<>();
            ArrayList<String> Qc = new ArrayList<>();
            for(Text value : values){
                String[] entries = value.toString().split("@");
                if(entries[1].equals("P")) Pc.add(entries[0]);
                else Qc.add(entries[0]);
            }
            for(String p : Pc){
                for(String q : Qc){
                    if(OverlapAndDuplicateAvoidance(p, q, c)) context.write(null, new Text(p+"#"+q+"@pq"));
                }
            }
        }
    }

    public static class SJMapper2 extends Mapper<LongWritable, Text, Text, Text>{
        public void map(LongWritable id, Text value, Context context) throws IOException, InterruptedException{
            String[] entries = value.toString().split("@");
            String inputType = entries[1];
            if(inputType.equals("pq")){
                String[] regions = entries[0].split("#");
                String p = regions[0];
                String q = regions[1];
                ArrayList<String> Cq = split(q);
                for(String c : Cq){
                    context.write(new Text(c), new Text(p+"#"+q+"@PQ"));
                }
            }
            else if(inputType.equals("R")){
                String rRegion = entries[0];
                ArrayList<String> Cr = split(rRegion);
                for(String c : Cr){
                    context.write(new Text(c), new Text(entries[0]+"@R"));
                }
            }
        }
    }

    public static class SJReducer2 extends Reducer<Text, Text, NullWritable, Text>{
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
            String c = key.toString();
            ArrayList<String> PQc = new ArrayList<>();
            ArrayList<String> Rc = new ArrayList<>();
            for(Text value : values){
                String[] entries= value.toString().split("@");
                String reg = entries[0];
                String type = entries[1];
                if(type.equals("PQ")){
                    PQc.add(reg);
                }
                else{
                    Rc.add(reg);
                }

            }
            for(String pq : PQc){
                for(String r : Rc){
                    String[] join = pq.split("#");
                    String p = join[0];
                    String q = join[1];
                    if(OverlapAndDuplicateAvoidance(q, r, c)) context.write(null, new Text(p+"#"+q+"#"+r+"@PQR"));
                }
            }
        }
    }

    public static class SJMapper3 extends Mapper<LongWritable, Text, Text, Text>{
        public void map(LongWritable id, Text value, Context context) throws IOException,InterruptedException{
            String entries[] = value.toString().split("@");
            String type = entries[1];
            if(type.equals("PQR")){
                String[] regs = entries[0].split("#");
                String p = regs[0];
                String q =regs[1];
                String r = regs[2];
                ArrayList<String> Cr = split(r);
                for(String c : Cr) context.write(new Text(c), new Text(p+"#"+q+"#"+r+"@PQR"));

            }
            else if(type.equals("S")){
                String s = entries[0];
                ArrayList<String> Cs = split(s);
                for(String c : Cs) context.write(new Text(c), new Text(s+"@S"));
            }
        }
    }

    public static class SJReducer3 extends Reducer<Text,Text,NullWritable, Text>{
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
            String c = key.toString();
            ArrayList<String> PQRc = new ArrayList<>();
            ArrayList<String> Sc = new ArrayList<>();
            for(Text value : values){
                String[] entries= value.toString().split("@");
                String reg = entries[0];
                String type = entries[1];
                if(type.equals("PQR")){
                    PQRc.add(reg);
                }
                else{
                    Sc.add(reg);
                }

            }
            for(String pqr : PQRc){
                for(String s : Sc){
                    String[] join = pqr.split("#");
                    String p = join[0];
                    String q = join[1];
                    String r = join[2];
                    if(OverlapAndDuplicateAvoidance(s, r, c)) context.write(null, new Text(p+"#"+q+"#"+r+"#"+s+"@PQRS"));
                }
            }
        }
    }
    public static void main(String[] args) throws Exception {
        Configuration config = new Configuration();
    
     
        Job job1 = Job.getInstance(config, "First Job");
        job1.setJarByClass(App.class);
        job1.setMapperClass(SJMapper1.class);
        job1.setReducerClass(SJReducer1.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(NullWritable.class);
        job1.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job1, new Path("/PInput"));
        FileInputFormat.addInputPath(job1, new Path("/QInput"));
        Path output1 = new Path("/output1");
        FileOutputFormat.setOutputPath(job1, output1);
        
        
        FileSystem fs = FileSystem.get(config);
        if (fs.exists(output1)) {
            fs.delete(output1, true);
        }
    
        if (!job1.waitForCompletion(true)) {
            System.exit(1);
        }
    
        
        Job job2 = Job.getInstance(config, "Second Job");
        job2.setJarByClass(App.class);
        job2.setMapperClass(SJMapper2.class);
        job2.setReducerClass(SJReducer2.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(NullWritable.class);
        job2.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job2, new Path("/RInput"));
        FileInputFormat.addInputPath(job2, output1); 
        Path output2 = new Path("/output2");
        FileOutputFormat.setOutputPath(job2, output2);
    
        if (fs.exists(output2)) {
            fs.delete(output2, true);
        }
    
        if (!job2.waitForCompletion(true)) {
            System.exit(1);
        }
    
       
        Job job3 = Job.getInstance(config, "Third Job");
        job3.setJarByClass(App.class);
        job3.setMapperClass(SJMapper3.class);
        job3.setReducerClass(SJReducer3.class);
        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(Text.class);
        job3.setOutputKeyClass(NullWritable.class);
        job3.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job3, new Path("/SInput"));
        FileInputFormat.addInputPath(job3, output2);
        Path output3 = new Path("/output3");
        FileOutputFormat.setOutputPath(job3, output3);
    
        if (fs.exists(output3)) {
            fs.delete(output3, true);
        }
    
        if (!job3.waitForCompletion(true)) {
            System.exit(1);
        }
    
        
        System.out.println("All jobs completed successfully!");
        System.exit(0);
    }
    
}
