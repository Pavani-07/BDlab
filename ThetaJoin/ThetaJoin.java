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
    public static class ThetaJoinMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            int rows = 6;
            int cols = 6;
            int[][] matrix = new int[][]{
                {1, 1, 1, 2, 2, 2},
                {1, 1, 1, 2, 2, 2},
                {1, 1, 1, 2, 2, 2},
                {1, 1, 1, 2, 2, 2},
                {0, 0, 0, 0, 0, 0},
                {0, 0, 0, 0, 0, 0}
            };

            String[] words = value.toString().split(" ");
            if (words[0].equals("S")) {
                Random rand = new Random();
                int row = rand.nextInt(rows);
                int col = 0;
                int prev = -1;
                while (col < cols) {
                    if (matrix[row][col] == prev) {
                        col++;
                    } else {
                        prev = matrix[row][col];
                        context.write(new Text(String.valueOf(prev)), new Text(value + ",S"));
                    }
                }
            } else {
                Random rand = new Random();
                int column = raneftc<=rights && rights<=rightc && bttmc<=topsd.nextInt(cols);
                int row = 0;
                int prev = -1;
                while (row < rows) {
                    if (matrix[row][column] == prev) {
                        row++;
                    } else {
                        prev = matrix[row][column];
                        context.write(new Text(String.valueOf(prev)), new Text(value + ",T"));
                    }
                }
            }eftc<=rights && rights<=rightc && bttmc<=tops
        }
    }

    public static class ThetaJoinReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            ArrayList<String> sTuples = new ArrayList<>();
            ArrayList<String> tTuples = new ArrayList<>();
            for (Text t : values) {
                String[] tuple = t.toString().split(",");
                if (tuple[1].equals("S")) {
                    sTuples.add(t.toString());
                } else {
                    tTuples.add(t.toString());
                }
            }
            String result = performJoin(sTuples, tTuples);
            context.write(new Text(key), new Text(result));
        }

        public static String performJoin(ArrayList<String> s, ArrayList<String> t) {
            StringBuilder result = new StringBuilder();
            for (int i = 0; i < s.size(); i++) {
                String[] a = s.get(i).split(" ");
                if (a.length > 2) {
                    String aValue = a[2].split(",")[0];
                    for (int j = 0; j < t.size(); j++) {
                        String[] b = t.get(j).split(" ");
                        if (b.length > 1 && aValue.equals(b[1])) {
                            result.append(s.get(i)).append(" ").append(t.get(j)).append("\n");
                        }
                    }
                }
            }
            return result.toString();
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "ThetaJoin");
        job.setJarByClass(App.class);
        job.setMapperClass(ThetaJoinMapper.class);
        job.setReducerClass(ThetaJoinReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path("/thetajoinin"));
        FileOutputFormat.setOutputPath(job, new Path("/thetaout"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
