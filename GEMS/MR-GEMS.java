import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MR_GEMS {

    static class Rectangle {
        String type;
        int x1, y1, x2, y2;
        boolean marked;

        public Rectangle(String type, int x1, int y1, int x2, int y2) {
            this.type = type;
            this.x1 = x1;
            this.x2 = x2;
            this.y1 = y1;
            this.y2 = y2;
            this.marked = false;
        }

        public List<Integer> getReducersList(int grid, int gridSize) {
            List<Integer> reducers = new ArrayList<>();
            int start_x = x1 / gridSize;
            int end_x = x2 / gridSize;
            int start_y = y1 / gridSize;
            int end_y = y2 / gridSize;
            for (int i = start_x; i <= end_x; i++) {
                for (int j = start_y; j <= end_y; j++) {
                    reducers.add(j * grid + i);
                }
            }
            return reducers;
        }

        public boolean overlaps(Rectangle other) {
            return !(x2 < other.x1 || other.x2 < x1 || y2 < other.y1 || other.y2 < y1);
        }

        public boolean crossesBoundary(int gridNum, int grid, int cellSize) {
            int grid_x = gridNum % grid;
            int grid_y = gridNum / grid;
            return x2 > (grid_x + 1) * cellSize || y2 > (grid_y + 1) * cellSize;
        }

        public String toString() {
            return type + "," + x1 + "," + y1 + "," + x2 + "," + y2;
        }
    }

    public static class Phase1Mapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split(",");
            Rectangle rect = new Rectangle(parts[0], Integer.parseInt(parts[1]), Integer.parseInt(parts[2]),
                    Integer.parseInt(parts[3]), Integer.parseInt(parts[4]));
            int grid = Integer.parseInt(context.getConfiguration().get("grid"));
            int gridSize = Integer.parseInt(context.getConfiguration().get("gridSize"));
            List<Integer> reducers = rect.getReducersList(grid, gridSize);

            for (Integer reducer : reducers) {
                context.write(new Text(reducer.toString()), new Text(rect.toString()));
            }
        }
    }

    public static class Phase1Reducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<Rectangle> pRec = new ArrayList<>();
            List<Rectangle> qRec = new ArrayList<>();
            List<Rectangle> rRec = new ArrayList<>();
            List<Rectangle> sRec = new ArrayList<>();
            int gridNum = Integer.parseInt(key.toString());
            int grid = Integer.parseInt(context.getConfiguration().get("grid"));
            int cellSize = Integer.parseInt(context.getConfiguration().get("gridSize"));

            for (Text val : values) {
                String[] parts = val.toString().split(",");
                Rectangle rect = new Rectangle(parts[0], Integer.parseInt(parts[1]), Integer.parseInt(parts[2]),
                        Integer.parseInt(parts[3]), Integer.parseInt(parts[4]));
                switch (rect.type) {
                    case "P":
                        pRec.add(rect);
                        break;
                    case "Q":
                        qRec.add(rect);
                        break;
                    case "R":
                        rRec.add(rect);
                        break;
                    case "S":
                        sRec.add(rect);
                        break;
                }
            }

            for (Rectangle q : qRec) {
                for (Rectangle r : rRec) {
                    if (q.overlaps(r) && !q.crossesBoundary(gridNum, grid, cellSize) && !r.crossesBoundary(gridNum, grid, cellSize)) {
                        for (Rectangle p : pRec) {
                            if (p.overlaps(q)) {
                                for (Rectangle s : sRec) {
                                    if (s.overlaps(r)) {
                                        context.write(new Text("Output"), new Text(p + ";" + q + ";" + r + ";" + s));
                                    }
                                }
                            }
                        }
                    } else {
                        q.marked = true;
                        r.marked = true;
                    }
                }
            }

            // Mark boundary-crossing rectangles for full replication
            for (Rectangle rec : Arrays.asList(pRec, qRec, rRec, sRec).stream().flatMap(List::stream).collect(Collectors.toList())) {
                if (rec.crossesBoundary(gridNum, grid, cellSize)) {
                    rec.marked = true;
                }
            }

            // Output marked rectangles for the second phase
            for (Rectangle rec : Arrays.asList(pRec, qRec, rRec, sRec).stream().flatMap(List::stream).collect(Collectors.toList())) {
                if (rec.marked) {
                    context.write(new Text("Rep"), new Text(rec.toString()));
                }
            }
        }
    }

    public static class Phase2Mapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split(",");
            if (parts[0].equals("Output")) return;
            Rectangle rect = new Rectangle(parts[0], Integer.parseInt(parts[1]), Integer.parseInt(parts[2]),
                    Integer.parseInt(parts[3]), Integer.parseInt(parts[4]));
            int grid = Integer.parseInt(context.getConfiguration().get("grid"));
            int gridSize = Integer.parseInt(context.getConfiguration().get("gridSize"));
            List<Integer> reducers = rect.getReducersList(grid, gridSize);

            for (Integer reducer : reducers) {
                context.write(new Text(reducer.toString()), new Text(rect.toString()));
            }
        }
    }

    public static class Phase2Reducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<Rectangle> pRec = new ArrayList<>();
            List<Rectangle> qRec = new ArrayList<>();
            List<Rectangle> rRec = new ArrayList<>();
            List<Rectangle> sRec = new ArrayList<>();

            for (Text val : values) {
                String[] parts = val.toString().split(",");
                Rectangle rect = new Rectangle(parts[0], Integer.parseInt(parts[1]), Integer.parseInt(parts[2]),
                        Integer.parseInt(parts[3]), Integer.parseInt(parts[4]));
                switch (rect.type) {
                    case "P":
                        pRec.add(rect);
                        break;
                    case "Q":
                        qRec.add(rect);
                        break;
                    case "R":
                        rRec.add(rect);
                        break;
                    case "S":
                        sRec.add(rect);
                        break;
                }
            }

            for (Rectangle p : pRec) {
                for (Rectangle q : qRec) {
                    if (p.overlaps(q)) {
                        for (Rectangle r : rRec) {
                            if (q.overlaps(r)) {
                                for (Rectangle s : sRec) {
                                    if (s.overlaps(r)) {
                                        context.write(new Text("Output"), new Text(p + ";" + q + ";" + r + ";" + s));
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
        conf.set("grid", "3");
        conf.set("gridSize", "5");

        Job phase1 = Job.getInstance(conf, "MR_GEMSJoin_Phase1");
        phase1.setJarByClass(MR_GEMS.class);
        phase1.setMapperClass(Phase1Mapper.class);
        phase1.setReducerClass(Phase1Reducer.class);
        phase1.setOutputKeyClass(Text.class);
        phase1.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(phase1, new Path(args[0]));
        FileOutputFormat.setOutputPath(phase1, new Path(args[1] + "_phase1"));

        if (phase1.waitForCompletion(true)) {
            Job phase2 = Job.getInstance(conf, "MR_GEMSJoin_Phase2");
            phase2.setJarByClass(MR_GEMS.class);
            phase2.setMapperClass(Phase2Mapper.class);
            phase2.setReducerClass(Phase2Reducer.class);
            phase2.setOutputKeyClass(Text.class);
            phase2.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(phase2, new Path(args[1] + "_phase1"));
            FileOutputFormat.setOutputPath(phase2, new Path(args[1] + "_output"));

            System.exit(phase2.waitForCompletion(true) ? 0 : 1);
        }
    }
}
