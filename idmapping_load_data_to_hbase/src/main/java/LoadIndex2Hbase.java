import ids.Index;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * Created by chentao on 16/6/24.
 */
public class LoadIndex2Hbase implements Tool {

    private String zkPath = "idmapping-hbase-node1,idmapping-hbase-node2,idmapping-hbase-node3";

    public void setConf(Configuration configuration) {

    }

    public Configuration getConf() {
        return null;
    }

    public static class LoadIndex2HbaseMapper extends Mapper<AvroKey<Index>, NullWritable, ImmutableBytesWritable, Put> {

        byte[] family=Bytes.toBytes("global_id");
        byte[] qualifier=Bytes.toBytes("value");
        byte[] rowKey = null;
        byte[] hValue = null;

        protected void map(AvroKey<Index> key, NullWritable value, Context context) throws IOException, InterruptedException {
            String id = key.datum().getId();
            String globalID = key.datum().getGlobalId();
            byte[] rowKey= Bytes.toBytes(id);
            ImmutableBytesWritable rowKeyWritable=new ImmutableBytesWritable(rowKey);
            byte[] hValue=Bytes.toBytes(globalID);
            Put put=new Put(rowKey);
            put.addColumn(family, qualifier, hValue);
            context.write(rowKeyWritable, put);
        }
    }

    public int run(String[] strings) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapreduce.job.queuename", "dmp");
        Job job = new Job(conf);
        job.setJarByClass(LoadIndex2Hbase.class);
        job.setMapperClass(LoadIndex2HbaseMapper.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);
        FileInputFormat.addInputPath(job, new Path(strings[1]));
        FileOutputFormat.setOutputPath(job, new Path(strings[2]));
        Configuration hbaseConfiguration= HBaseConfiguration.create();
        hbaseConfiguration.set("mapreduce.job.queuename", "dmp");
        hbaseConfiguration.set("mapreduce.job.name", "idmapping-bulkload-index" + strings[1]);
        hbaseConfiguration.set("hbase.zookeeper.quorum", zkPath);
        HTable table =new HTable(hbaseConfiguration, "hbase_index");
        HFileOutputFormat2.configureIncrementalLoad(job, table, table);
        job.setNumReduceTasks(1000);
        int exitCode = job.waitForCompletion(true) == true ? 0 : 1;
        return  exitCode;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new LoadIndex2Hbase(), args);
        System.exit(exitCode);
    }
}
