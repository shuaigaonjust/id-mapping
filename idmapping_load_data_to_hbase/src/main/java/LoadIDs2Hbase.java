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
public class LoadIDs2Hbase implements Tool {

    private String zkPath = "10.10.12.82,10.10.12.83,10.10.12.84";

    public void setConf(Configuration configuration) {

    }

    public Configuration getConf() {
        return null;
    }

    public static class LoadIDs2HbaseMapper extends Mapper<AvroKey<IDs>, NullWritable, ImmutableBytesWritable, Put> {
        private byte[] familyImei = Bytes.toBytes("imei");
        private byte[] familyMac = Bytes.toBytes("mac");
        private byte[] familyImsi = Bytes.toBytes("imsi");
        private byte[] familyPhoneNum = Bytes.toBytes("phone_number");
        private byte[] familyIdfa = Bytes.toBytes("idfa");
        private byte[] familyOpenudid = Bytes.toBytes("openudid");
        private byte[] familyUid = Bytes.toBytes("uid");
        private byte[] familyDid = Bytes.toBytes("did");
        private byte[] familyAndroidId = Bytes.toBytes("android_id");
        private byte[] qualifier=Bytes.toBytes("value");
        private byte[] rowKey = null;
        private byte[] hValue = null;

        protected void map(AvroKey<IDs> key, NullWritable value, Context context) throws IOException, InterruptedException {
            String global_id = key.datum().getGlobalId();
            String imei = key.datum().getImei().toString();
            String mac = key.datum().getMac().toString();
            String imsi = key.datum().getImsi().toString();
            String phoneNum = key.datum().getPhoneNumber().toString();
            String idfa = key.datum().getIdfa().toString();
            String openudid = key.datum().getOpenudid().toString();
            String uid = key.datum().getUid().toString();
            String did = key.datum().getDid().toString();
            String android_id = key.datum().getAndroidId().toString();

            rowKey = Bytes.toBytes(global_id);
            ImmutableBytesWritable rowKeyWritable = new ImmutableBytesWritable(rowKey);
            Put put=new Put(rowKey);
            hValue=Bytes.toBytes(imei);
            put.addColumn(familyImei, qualifier, hValue);
            hValue=Bytes.toBytes(mac);
            put.addColumn(familyMac, qualifier, hValue);
            hValue=Bytes.toBytes(imsi);
            put.addColumn(familyImsi, qualifier, hValue);
            hValue=Bytes.toBytes(phoneNum);
            put.addColumn(familyPhoneNum, qualifier, hValue);
            hValue=Bytes.toBytes(idfa);
            put.addColumn(familyIdfa, qualifier, hValue);
            hValue=Bytes.toBytes(openudid);
            put.addColumn(familyOpenudid, qualifier, hValue);
            hValue=Bytes.toBytes(uid);
            put.addColumn(familyUid, qualifier, hValue);
            hValue=Bytes.toBytes(did);
            put.addColumn(familyDid, qualifier, hValue);
            hValue=Bytes.toBytes(android_id);
            put.addColumn(familyAndroidId, qualifier, hValue);
            context.write(rowKeyWritable, put);
        }
    }

    public int run(String[] strings) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapreduce.job.queuename", "dmp");
        conf.set("mapreduce.job.name", "idmapping_load_ids_to_hbase");
        conf.set("mapreduce.reduce.memory.mb", "4096");
        conf.set("mapreduce.reduce.java.opts", "4096");
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
        HTable table =new HTable(hbaseConfiguration, "idmapping_ids");
        Connection connection = ConnectionFactory.createConnection(hbaseConfiguration);
        TableName tableName = TableName.valueOf("idmapping_ids");
//        HFileOutputFormat2.configureIncrementalLoad(job, connection.getTable(tableName), connection.getRegionLocator(tableName));
        HFileOutputFormat2.configureIncrementalLoad(job, table, table);
        int exitCode = job.waitForCompletion(true) == true ? 0 : 1;
        if (exitCode == 0) {
            LoadIncrementalHFiles loadFfiles = new LoadIncrementalHFiles(conf);
            loadFfiles.doBulkLoad(new Path(strings[2]), table);//导入数据
            System.out.println("Bulk Load Completed..");
        }
        return  exitCode;
    }
}
