package recommend;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Database extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Database(),args));
    }

    static class DBMapper extends Mapper<LongWritable,Text,DBTuple,NullWritable>{
        private DBTuple dbTuple = new DBTuple();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split("\t");
            String[] split1 = split[0].split("[,]");
            dbTuple.set(split1[0],split1[1],split[1]);
            context.write(dbTuple,NullWritable.get());
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf=getConf();
        Properties properties = new Properties();
        InputStream is=this.getClass().getResourceAsStream("/conf.properties");
        properties.load(is);
        Path DatabaseInput = new Path(properties.getProperty("DatabaseInput"));

        Job job=Job.getInstance(conf,this.getClass().getSimpleName());
        job.setJarByClass(this.getClass());

        job.setMapperClass(DBMapper.class);
        job.setMapOutputKeyClass(DBTuple.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(DBTuple.class);
        job.setOutputValueClass(NullWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(DBOutputFormat.class);

        TextInputFormat.addInputPath(job,DatabaseInput);

        String driver = properties.getProperty("driver");
        String url = properties.getProperty("url");
        String user = properties.getProperty("user");
        String password = properties.getProperty("password");
        DBConfiguration.configureDB(job.getConfiguration(),driver,url,user,password);

        String tableName=properties.getProperty("tableName");
        String[] fieldNames=properties.getProperty("fieldNames").split("[,]");
        DBOutputFormat.setOutput(job,tableName,fieldNames);

        return job.waitForCompletion(true)?0:1;
    }
}
