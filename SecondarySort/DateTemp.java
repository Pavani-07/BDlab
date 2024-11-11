import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.StringTokenizer;

// DateTemperaturePair Class
public class DateTemperaturePair implements Writable, WritableComparable<DateTemperaturePair> {
    private Text yearMonth = new Text();
    private Text day = new Text();
    private IntWritable temperature = new IntWritable();

    public Text getYearMonth() { return yearMonth; }
    public Text getDay() { return day; }
    public IntWritable getTemperature() { return temperature; }

    public void setYearMonth(String yearMonth) { this.yearMonth.set(yearMonth); }
    public void setDay(String day) { this.day.set(day); }
    public void setTemperature(int temperature) { this.temperature.set(temperature); }

    @Override
    public void write(DataOutput out) throws IOException {
        yearMonth.write(out);
        day.write(out);
        temperature.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        yearMonth.readFields(in);
        day.readFields(in);
        temperature.readFields(in);
    }

    @Override
    public int compareTo(DateTemperaturePair pair) {
        int compareValue = this.yearMonth.compareTo(pair.getYearMonth());
        if (compareValue == 0) {
            compareValue = temperature.compareTo(pair.getTemperature());
        }
        return -1 * compareValue; // Sort descending
    }

    @Override
    public String toString() {
        return yearMonth.toString() + "-" + day.toString() + ": " + temperature.toString();
    }
}

// DateTemperaturePartitioner Class
public class DateTemperaturePartitioner extends Partitioner<DateTemperaturePair, Text> {
    @Override
    public int getPartition(DateTemperaturePair pair, Text text, int numberOfPartitions) {
        return Math.abs(pair.getYearMonth().hashCode() % numberOfPartitions);
    }
}

// DateTemperatureGroupingComparator Class
public class DateTemperatureGroupingComparator extends WritableComparator {
    public DateTemperatureGroupingComparator() {
        super(DateTemperaturePair.class, true);
    }

    @Override
    public int compare(WritableComparable wc1, WritableComparable wc2) {
        DateTemperaturePair pair1 = (DateTemperaturePair) wc1;
        DateTemperaturePair pair2 = (DateTemperaturePair) wc2;
        return pair1.getYearMonth().compareTo(pair2.getYearMonth());
    }
}

// Mapper Class
public class SecondarySortingTemperatureMapper extends Mapper<Object, Text, DateTemperaturePair, IntWritable> {
    private DateTemperaturePair pair = new DateTemperaturePair();
    private IntWritable temperature = new IntWritable();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split(",");
        String yearMonth = tokens[0] + tokens[1];
        String day = tokens[2];
        int temp = Integer.parseInt(tokens[3]);

        pair.setYearMonth(yearMonth);
        pair.setDay(day);
        pair.setTemperature(temp);
        temperature.set(temp);

        context.write(pair, temperature);
    }
}

// Reducer Class
public class SecondarySortingTemperatureReducer extends Reducer<DateTemperaturePair, IntWritable, Text, NullWritable> {
    private Text result = new Text();

    public void reduce(DateTemperaturePair key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        StringBuilder sortedTemperatureList = new StringBuilder();
        for (IntWritable val : values) {
            sortedTemperatureList.append(val.get()).append(",");
        }
        // Remove the trailing comma
        if (sortedTemperatureList.length() > 0) {
            sortedTemperatureList.setLength(sortedTemperatureList.length() - 1);
        }
        result.set(key.getYearMonth() + ": " + sortedTemperatureList.toString());
        context.write(result, NullWritable.get());
    }
}

// Driver Class
public class DateTemp extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "Secondary Sort");
        job.setJarByClass(SecondarySortDriver.class);

        job.setMapperClass(SecondarySortingTemperatureMapper.class);
        job.setReducerClass(SecondarySortingTemperatureReducer.class);

        job.setOutputKeyClass(DateTemperaturePair.class);
        job.setOutputValueClass(IntWritable.class);

        job.setPartitionerClass(DateTemperaturePartitioner.class);
        job.setGroupingComparatorClass(DateTemperatureGroupingComparator.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
