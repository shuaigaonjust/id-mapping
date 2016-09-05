import ids.Index;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
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
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * Created by chentao on 16/6/24.
 */
public class LoadIndex2Hbase implements Tool {

    private String zkPath = "10.10.12.82,10.10.12.83,10.10.12.84";
    private String zkIndexPath = "/idmapping/active_index";
    private ConnectWatcher connectWatcher = new ConnectWatcher();
    private String zkIndexName = new String();

    public void setConf(Configuration configuration) {}
    public Configuration getConf() {
        return null;
    }

    public static class LoadIndex2HbaseMapper extends Mapper<AvroKey<Index>, NullWritable, ImmutableBytesWritable, Put> {
        byte[] family = Bytes.toBytes("global_id");
        byte[] qualifier = Bytes.toBytes("value");
        byte[] rowKey = null;
        byte[] hValue = null;

        protected void map(AvroKey<Index> key, NullWritable value, Context context) throws IOException, InterruptedException {
            String id = key.datum().getId();
            String globalID = key.datum().getGlobalId();
            rowKey = Bytes.toBytes(id);
            ImmutableBytesWritable rowKeyWritable=new ImmutableBytesWritable(rowKey);
            hValue = Bytes.toBytes(globalID);
            Put put=new Put(rowKey);
            put.addColumn(family, qualifier, hValue);
            context.write(rowKeyWritable, put);
        }
    }

    public int run(String[] strings) throws Exception {
        connectWatcher.connect(zkPath);
        zkIndexName = "";
        zkIndexName = connectWatcher.getData(zkIndexPath, null);
        if (zkIndexName.equals("idmapping_index") || zkIndexName.equals("idmapping_index_1")) {
            zkIndexName = "idmapping_index_2";
        } else if (zkIndexName.equals("idmapping_index_2")) {
            zkIndexName = "idmapping_index_1";
        } else {
            throw new Exception("index table name is not valid :" + zkIndexName);
        }
        Configuration conf = new Configuration();
        HBaseConfiguration.addHbaseResources(conf);
        conf.set("mapreduce.job.queuename", "dmp");
        conf.set("mapreduce.job.name", "idmapping_load_index_to_hbase");
        Job job = new Job(conf);
        FileSystem fs = FileSystem.get(conf);
        Path output = new Path(strings[2]);
        if (fs.exists(output)) {
            fs.delete(output, true);//如果输出路径存在，就将其删除
        }
        job.setJarByClass(LoadIndex2Hbase.class);
        job.setMapperClass(LoadIndex2HbaseMapper.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);
        AvroJob.setInputKeySchema(job, ids.Index.getClassSchema());
        job.setInputFormatClass(AvroKeyInputFormat.class);
//        job.setOutputFormatClass(HFileOutputFormat2.class);
        FileInputFormat.addInputPath(job, new Path(strings[1]));
        FileOutputFormat.setOutputPath(job, new Path(strings[2]));
        Configuration hbaseConfiguration= HBaseConfiguration.create();
        hbaseConfiguration.set("mapreduce.job.queuename", "dmp");
        hbaseConfiguration.set("mapreduce.job.name", "idmapping-bulkload-index" + strings[1]);
        hbaseConfiguration.set("hbase.zookeeper.quorum", zkPath);
        HTable table = new HTable(hbaseConfiguration, zkIndexName);
//        Connection connection = ConnectionFactory.createConnection(hbaseConfiguration);
//        TableName tableName = TableName.valueOf(zkIndexName);
        HFileOutputFormat2.configureIncrementalLoad(job, table, table);
        int exitCode = job.waitForCompletion(true) == true ? 0 : 1;
        if (exitCode == 0) {
            LoadIncrementalHFiles loadFfiles = new LoadIncrementalHFiles(hbaseConfiguration);
            loadFfiles.doBulkLoad(new Path(strings[2]), table);//导入数据
            System.out.println("Bulk Load Completed..");
        }
        connectWatcher.setData(zkIndexPath, zkIndexName);
        connectWatcher.close();
        return  exitCode;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new LoadIndex2Hbase(), args);
        System.exit(exitCode);
    }
}
