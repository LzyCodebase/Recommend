package recommend;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Sort extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Sort(),args));
    }

    static class SortMappper extends Mapper<LongWritable,Text,Tuple,NullWritable>{
        private Tuple key = new Tuple();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split("[\t]");
            String[] split1 = split[0].split("[,]");
            this.key.set(split1[0],split1[1],split[1]);
            context.write(this.key,NullWritable.get());
        }
    }
    /*static class SortReducer extends Reducer<Tuple,Text,Text,Text>{
        private Text key = new Text();
        private Text value = new Text();

        @Override
        protected void reduce(Tuple key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String[] split = key.toString().split("\t");
            this.key.set(split[0]+","+values.toString());
            this.value.set(split[1]);
            context.write(this.key,this.value);
        }
    }
*/
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        Properties properties = new Properties();
        InputStream is=this.getClass().getResourceAsStream("/conf.properties");
        properties.load(is);
        Path SortInput = new Path(properties.getProperty("SortInput"));
        Path SortOutput = new Path(properties.getProperty("SortOutput"));

        Job job = Job.getInstance(conf,this.getClass().getSimpleName());
        job.setJarByClass(this.getClass());

        job.setMapperClass(SortMappper.class);
        job.setMapOutputKeyClass(Tuple.class);
        job.setMapOutputValueClass(NullWritable.class);

        //job.setReducerClass(SortReducer.class);
        job.setOutputKeyClass(Tuple.class);
        job.setOutputValueClass(NullWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextInputFormat.addInputPath(job,SortInput);
        TextOutputFormat.setOutputPath(job,SortOutput);

        return job.waitForCompletion(true) ?  0 : 1;
    }
}
