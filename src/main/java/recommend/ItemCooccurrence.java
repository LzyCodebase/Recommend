package recommend;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ItemCooccurrence extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new ItemCooccurrence(),args));
    }

    static class ItemCooccurrenceMapper extends Mapper<Text,Text,Text,IntWritable>{
        private Text items = new Text();
        private IntWritable num = new IntWritable();

        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split("[,]");
            for (int i=0 ;i<split.length ;i++){
                for (int j=0 ;j<split.length ;j++){
                    items.set(split[i]+"\t"+split[j]);
                    num.set(1);
                    context.write(items,num);
                }
            }
        }
    }

    static class ItemCooccurrenceReducer extends Reducer<Text,IntWritable,Text,Text>{
        private Text item = new Text();
        private Text item_number = new Text();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            String[] split = key.toString().split("\t");
            int count=0;
            for (IntWritable v : values){
                count += v.get();
            }
            item.set(split[0]);
            item_number.set(split[1]+","+count);
            context.write(item,item_number);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        Properties properties = new Properties();
        InputStream is=this.getClass().getResourceAsStream("/conf.properties");
        properties.load(is);
        Path input = new Path( properties.getProperty("ItemCooccurrenceInput"));
        Path output = new Path( properties.getProperty("ItemCooccurrenceOutput"));

        conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator","\t");

        Job job = Job.getInstance(conf,this.getClass().getSimpleName());
        job.setJarByClass(this.getClass());

        job.setMapperClass(ItemCooccurrenceMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(ItemCooccurrenceReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        KeyValueTextInputFormat.addInputPath(job,input);
        TextOutputFormat.setOutputPath(job,output);

        return job.waitForCompletion(true) ?  0 : 1;
    }
}
