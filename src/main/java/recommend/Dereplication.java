package recommend;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.Properties;

public class Dereplication extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Dereplication(),args));
    }

    static class Mapper1 extends Mapper<LongWritable,Text,Text,Text>{
        private Text user = new Text();
        private Text item = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split("[ ]");
            user.set(split[0]+","+split[1]);
            item.set(split[2]);
            context.write(user,item);
        }
    }
    static class Mapper2 extends Mapper<LongWritable,Text,Text,Text>{
        private Text user = new Text();
        private Text item = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split("[\t]");
            user.set(split[0]);
            item.set(split[1]);
            context.write(user,item);
        }
    }
    static class Reducer1 extends Reducer<Text,Text,Text,Text> {
        private Text value = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Iterator<Text> iterator = values.iterator();
            Text next = iterator.next();
            if (!iterator.hasNext()){
                this.value.set(next);
                context.write(key,this.value);
            }

            /*StringBuffer stringBuffer1 = new StringBuffer();
            StringBuffer stringBuffer2 = new StringBuffer();
            for (Text v : values){
                if (v.toString().contains("*")){
                    stringBuffer1.append(v.toString());
                }else if (v.toString().contains("?")){
                    stringBuffer2.append(v.toString());
                }
            }
            String[] split1 = stringBuffer1.toString().split("[*]");
            String[] split2 = stringBuffer2.toString().split("[?]");
            for (int j=0 ;j<split2.length ;j++) {
                for (int i=0 ;i<split1.length ;i++) {
                    if (split2[j].contains(split1[i])) {
                        break;
                    }else if (i==split1.length-1){
                        String[] ss = split2[j].split("\t");
                        this.key.set(key + "," + ss[0]);
                        this.value.set(ss[1]);
                        context.write(this.key, this.value);
                    }
                }
            }*/
        }
    }


    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        Properties properties = new Properties();
        InputStream is=this.getClass().getResourceAsStream("/conf.properties");
        properties.load(is);
        Path DereplicationInput_user = new Path(properties.getProperty("DereplicationInput_user"));
        Path DereplicationInput_recommend = new Path(properties.getProperty("DereplicationInput_recommend"));
        Path DereplicationOutput = new Path(properties.getProperty("DereplicationOutput"));

        Job job = Job.getInstance(conf,this.getClass().getSimpleName());
        job.setJarByClass(this.getClass());

        MultipleInputs.addInputPath(job,DereplicationInput_user,TextInputFormat.class,Mapper1.class);
        MultipleInputs.addInputPath(job,DereplicationInput_recommend,TextInputFormat.class,Mapper2.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,DereplicationOutput);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(Reducer1.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        return job.waitForCompletion(true) ?  0 : 1;
    }
}
