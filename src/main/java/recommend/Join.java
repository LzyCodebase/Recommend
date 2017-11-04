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
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Join extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Join(),args));
    }

    static class JoinMapper1 extends Mapper<Text,Text,Text,Text>{
        private Text value = new Text();
        private String star = "*";

        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            this.value.set(value.toString()+star);
            context.write(key,this.value);
        }
    }

    static class JoinMapper2 extends Mapper<Text,Text,Text,Text>{
        private Text value = new Text();
        private String star = "?";

        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            this.value.set(value.toString()+star);
            context.write(key,this.value);
        }
    }

    static class JoinReducer extends Reducer<Text,Text,Text,IntWritable>{
        private Text user_item = new Text();
        private IntWritable number = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuffer stringBuffer1 = new StringBuffer();
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
            for (int i=0 ;i<split1.length ;i++){
                String[] split11 = split1[i].toString().split("[:]");
                for (int j=0 ;j<split2.length ;j++){
                    String[] split22 = split2[j].toString().split(",");
                    user_item.set(split11[0]+","+split22[0]);
                    number.set(Integer.parseInt(split11[1])*Integer.parseInt(split22[1]));
                    context.write(user_item,number);
                }
            }

        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        Properties properties = new Properties();
        InputStream is=this.getClass().getResourceAsStream("/conf.properties");
        properties.load(is);
        Path ItemCoocJoinInputuser_item = new Path(properties.getProperty("ItemCoocJoinInputuser_item"));
        Path ItemCoocJoinInput_cooc = new Path(properties.getProperty("ItemCoocJoinInput_cooc"));
        Path ItemCoocJoinOutput = new Path(properties.getProperty("ItemCoocJoinOutput"));

        conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator","\t");

        Job job = Job.getInstance(conf,this.getClass().getSimpleName());
        job.setJarByClass(this.getClass());

        MultipleInputs.addInputPath(job,ItemCoocJoinInputuser_item,KeyValueTextInputFormat.class,JoinMapper1.class);
        MultipleInputs.addInputPath(job,ItemCoocJoinInput_cooc,KeyValueTextInputFormat.class,JoinMapper2.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,ItemCoocJoinOutput);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(JoinReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        return job.waitForCompletion(true) ?  0 : 1;
    }
}
