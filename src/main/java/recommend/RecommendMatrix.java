package recommend;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class RecommendMatrix extends Configured implements Tool{
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new RecommendMatrix(),args));
    }

    static class RecommendMatrixMapper extends Mapper<LongWritable,Text,Text,IntWritable>{
        private Text user_item = new Text();
        private IntWritable target = new IntWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split("\t");
            user_item.set(split[0]);
            target.set(Integer.parseInt(split[1]));
            context.write(user_item,target);
        }
    }

    static class RecommendMatrixReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
        private IntWritable target_sum = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            for (IntWritable v : values){
                count += v.get();
            }
            target_sum.set(count);
            context.write(key,target_sum);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        Properties properties = new Properties();
        InputStream is=this.getClass().getResourceAsStream("/conf.properties");
        properties.load(is);
        Path RecommendMatrixInput = new Path(properties.getProperty("RecommendMatrixInput"));
        Path RecommendMatrixOutput = new Path(properties.getProperty("RecommendMatrixOutput"));

        Job job = Job.getInstance(conf,this.getClass().getSimpleName());
        job.setJarByClass(this.getClass());

        job.setMapperClass(RecommendMatrixMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(RecommendMatrixReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextInputFormat.addInputPath(job,RecommendMatrixInput);
        TextOutputFormat.setOutputPath(job,RecommendMatrixOutput);

        return job.waitForCompletion(true) ?  0 : 1;
    }
}
