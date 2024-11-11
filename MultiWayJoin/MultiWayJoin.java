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
    import org.apache.logging.log4j.Logger;
    import org.apache.logging.log4j.LogManager;
    public class MultiWayJoin {
        public static int K=9, m=3;
        private static final Logger theLogger = LogManager.getLogger("Multi-Way-Join");
        public static class RMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
            
            
            public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
                String[] words = value.toString().split(",");
                
                for(int i=0;i<m;i++){
                    IntWritable newKey = new IntWritable((Integer.parseInt(words[1])%m)*m+i) ;
                    theLogger.info("Logger4J" +String.format("%d", (Integer.parseInt(words[1])%m)*m+i) +value.toString()+ "#R");
                    context.write(newKey,new Text(value.toString()+ "#R"));
                }
             } 
           }
    
        
        public static class SMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
          
            public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
                
                String[] words = value.toString().split(",");
                IntWritable newKey = new IntWritable((Integer.parseInt(words[0])%m)*m+(Integer.parseInt(words[1])%m)) ;
                theLogger.info("Logger4J" +String.format("%d",(Integer.parseInt(words[0])%m)*m+(Integer.parseInt(words[1])%m))); 
                context.write(newKey,new Text(value.toString()+ "#S")); 
           }
        }    
    
        public static class TMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
          
            public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
                String[] words = value.toString().split(",");
                for(int i=0;i<m;i++){
                    IntWritable newKey = new IntWritable((Integer.parseInt(words[0])%m)+i*m) ;
                    theLogger.info("Logger4J" + String.format("%d",(Integer.parseInt(words[0])%m)+i*m));
                    context.write(newKey,new Text(value.toString()+ "#T"));
                }
             } 
           }
    
        public static class MyReducer extends Reducer<IntWritable, Text, IntWritable, Text>{
            public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
                ArrayList<String> RList = new ArrayList<String>();
                ArrayList<String> SList = new ArrayList<String>();
                ArrayList<String> TList = new ArrayList<String>();
                for(Text val: values){
                    if(val.toString().endsWith("#R")){
                        RList.add(val.toString().replace("#R", ""));
                    }else if(val.toString().endsWith("#S")) {
                        SList.add(val.toString().replace("#S", ""));
                    }else{
                        TList.add(val.toString().replace("#T", ""));
                    }
                }
                System.out.println(RList.toString());
                System.out.println(SList.toString());
                System.out.println(TList.toString());
                
                
                for(int i = 0 ;i<RList.size();i++)
                {
                    String[] rfields = RList.get(i).split(","); 
                    for(int j=0; j< SList.size();j++){
                        String[] sfields = SList.get(j).split(",");

                        if(rfields[1].equals(sfields[0])){
                            for(int k=0; k< TList.size();k++){
                                String[] tfields = TList.get(k).split(",");
                                if(sfields[1].equals(tfields[0])){
                                    context.write(key, new Text(rfields[0]+","+ rfields[1]+","+tfields[0]+","+tfields[1]));
                                }
                            }    
                            
                        }
                        
                    }
    
                }
            }
        }
        public static class MyPartitioner extends Partitioner< IntWritable, Text>{
    
            @Override
            public int getPartition(IntWritable key, Text value, int numPartitions) {
                // TODO Auto-generated method stub
                if(numPartitions==0){
                    return 0;
                } else{
                    /*                     Pattern pat1 = Pattern.compile("^[a-iA-I]+");
                    Pattern pat2 = Pattern.compile("^[j-rJ-R]+");
                    Matcher mat1 = pat1.matcher(key.toString());
                    Matcher mat2 = pat2.matcher(key.toString());
                    
                    if(mat1.find()){
                        return 0;
                    }else if (mat2.find()){
                        return 1;
                    }else{
                        return 2;
                    }*/
                    return key.get();
                }
            }
            
        }
        public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "WC");
            job.setJarByClass(App.class);
            //job.setMapperClass(MyMapper.class);
            job.setReducerClass(MyReducer.class); 
            job.setNumReduceTasks(K);
            job.setPartitionerClass(MyPartitioner.class);
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(Text.class);
            MultipleInputs.addInputPath(job, new Path("/RInput"), TextInputFormat.class, RMapper.class);
            MultipleInputs.addInputPath(job, new Path("/SInput"), TextInputFormat.class, SMapper.class);
            MultipleInputs.addInputPath(job, new Path("/TInput"), TextInputFormat.class, TMapper.class);
            
            //FileInputFormat.addInputPath(job, new Path("/input"));
            FileOutputFormat.setOutputPath(job, new Path("/output"));
            System.exit(job.waitForCompletion(true)?0:1);
            
        }
    }
