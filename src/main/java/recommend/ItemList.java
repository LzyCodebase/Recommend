package recommend;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ItemList extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new ItemList(),args));
    }

    static class ItemListMapper extends Mapper<LongWritable,Text,Text,Text>{
        private Text user = new Text();
        private Text itemnumber = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split("[ ]");
            user.set(split[0]);
            itemnumber.set(split[1]);
            context.write(user,itemnumber);
        }
    }

    static class ItemListReducer extends Reducer<Text,Text,Text,Text>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuffer sb = new StringBuffer();
            for (Text v :values){
                if (sb.length()==0){
                    sb.append(v);
                }else {
                    sb.append(","+v);
                }
            }
            context.write(key,new Text(sb.toString()));
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();

        Properties properties = new Properties();
//        InputStream is = ClassLoader.getSystemResourceAsStream("conf.properties");
        InputStream is=this.getClass().getResourceAsStream("/conf.properties");
        properties.load(is);
        Path input = new Path( properties.getProperty("ItemListInput"));
        Path output = new Path( properties.getProperty("ItemListOutput"));

        Job job = Job.getInstance(conf,this.getClass().getSimpleName());
        job.setJarByClass(this.getClass());

        job.setMapperClass(ItemListMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(ItemListReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //设置文件的读取方式（文本文件），输出方式（文本文件）
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        //给job指定输入文件的路径和输出结果的路径
        TextInputFormat.addInputPath(job,input);
        TextOutputFormat.setOutputPath(job,output);

        return job.waitForCompletion(true) ? 0 : 1 ;
    }
}
