import ids.IDs;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import java.io.IOException;

/**
 * Created by chentao on 16/6/24.
 */
public class LoadIDs2Hbase2 implements Tool {

    private String zkPath = "10.10.12.82,10.10.12.83,10.10.12.84";

    public void setConf(Configuration configuration) {

    }

    public Configuration getConf() {
        return null;
    }

    public static class LoadIDs2HbaseMapper extends Mapper<AvroKey<IDs>, NullWritable, ImmutableBytesWritable, Put> {
        private byte[] familyIds = Bytes.toBytes("ids");
        private byte[] qualifier=Bytes.toBytes("value");
        private byte[] rowKey = null;
        private byte[] hValue = null;

        protected void map(AvroKey<IDs> key, NullWritable value, Context context) throws IOException, InterruptedException {
            String global_id = key.datum().getGlobalId();
            String ids = key.datum().toString();
            rowKey = Bytes.toBytes(global_id);
            ImmutableBytesWritable rowKeyWritable = new ImmutableBytesWritable(rowKey);
            Put put = new Put(rowKey);
            hValue = Bytes.toBytes(ids);
            put.addColumn(familyIds, qualifier, hValue);
            context.write(rowKeyWritable, put);
        }
    }

    public int run(String[] strings) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapreduce.job.queuename", "dmp");
        conf.set("mapreduce.job.name", "idmapping_load_ids_to_hbase");
        Job job = new Job(conf);
        FileSystem fs = FileSystem.get(conf);
        Path output = new Path(strings[2]);
        if (fs.exists(output)) {
            fs.delete(output, true);//如果输出路径存在，就将其删除
        }
        job.setJarByClass(LoadIndex2Hbase.class);
        job.setMapperClass(LoadIDs2HbaseMapper.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);
        FileInputFormat.addInputPath(job, new Path(strings[1]));
        FileOutputFormat.setOutputPath(job, new Path(strings[2]));
        AvroJob.setInputKeySchema(job, ids.IDs.getClassSchema());
        job.setInputFormatClass(AvroKeyInputFormat.class);
//        job.setOutputFormatClass(HFileOutputFormat2.class);
        Configuration hbaseConfiguration= HBaseConfiguration.create();
        hbaseConfiguration.set("mapreduce.job.queuename", "dmp");
        hbaseConfiguration.set("mapreduce.job.name", "idmapping-bulkload-ids" + strings[1]);
        hbaseConfiguration.set("hbase.zookeeper.quorum", zkPath);
        HTable table =new HTable(hbaseConfiguration, "idmapping_ids_2");
        Connection connection = ConnectionFactory.createConnection(hbaseConfiguration);
        TableName tableName = TableName.valueOf("idmapping_ids_2");
        HFileOutputFormat2.configureIncrementalLoad(job, connection.getTable(tableName), connection.getRegionLocator(tableName));
        int exitCode = job.waitForCompletion(true) == true ? 0 : 1;
        if (exitCode == 0) {
            LoadIncrementalHFiles loadFfiles = new LoadIncrementalHFiles(hbaseConfiguration);
            loadFfiles.doBulkLoad(new Path(strings[2]), table);//导入数据
            System.out.println("Bulk Load Completed..");
        }
        return  exitCode;
    }
}
