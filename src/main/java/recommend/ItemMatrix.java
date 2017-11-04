package recommend;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
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

public class ItemMatrix extends Configured implements Tool{
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new ItemMatrix(),args));
    }

    static class ItemMatrixMapper extends Mapper<LongWritable,Text,Text,Text>{
        private Text item = new Text();
        private Text user_number = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split("[ ]");
            item.set(split[1]);
            user_number.set(split[0]+":"+split[2]);
            context.write(item,user_number);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        Properties properties = new Properties();
        InputStream is=this.getClass().getResourceAsStream("/conf.properties");
        properties.load(is);
        Path input = new Path( properties.getProperty("ItemMatrixInput"));
        Path output = new Path( properties.getProperty("ItemMatrixOutput"));

        //构建job对象，并设置驱动类名和job名
        Job job = Job.getInstance(conf);
        job.setJarByClass(this.getClass());//设置job驱动类
        job.setJobName(this.getClass().getSimpleName());//设置job名字

        //给job设置mapper类及map方法产生的键值类型
        job.setMapperClass(ItemMatrixMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        //给job设置reducer类及reduce方法产生的键值类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //设置文件的读取方式（文本文件），输出方式（文本文件）
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        //给job指定输入文件的路径和输出结果的路径
        TextInputFormat.addInputPath(job,input);
        TextOutputFormat.setOutputPath(job,output);

        //向集群提交作业
        return job.waitForCompletion(true) ?  0 : 1;
    }
}
