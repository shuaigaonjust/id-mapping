import ids.IDs;
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

import java.io.IOException;

/**
 * Created by chentao on 16/6/24.
 */
public class LoadIDs2Hbase implements Tool {

    private String zkPath = "idmapping-hbase-node1,idmapping-hbase-node2,idmapping-hbase-node3";

    public void setConf(Configuration configuration) {

    }

    public Configuration getConf() {
        return null;
    }

    public static class LoadIDs2HbaseMapper extends Mapper<AvroKey<IDs>, NullWritable, ImmutableBytesWritable, Put> {
        byte[] familyImei = Bytes.toBytes("imei");
        byte[] familyMac = Bytes.toBytes("mac");
        byte[] familyImsi = Bytes.toBytes("imsi");
        byte[] familyPhoneNum = Bytes.toBytes("phone_number");
        byte[] familyIdfa = Bytes.toBytes("idfa");
        byte[] familyOpenudid = Bytes.toBytes("openudid");
        byte[] familyUid = Bytes.toBytes("uid");
        byte[] familyDid = Bytes.toBytes("did");
        byte[] qualifier=Bytes.toBytes("value");
        byte[] rowKey = null;
        byte[] hValue = null;

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
            context.write(rowKeyWritable, put);
        }
    }

    public int run(String[] strings) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapreduce.job.queuename", "dmp");
        Job job = new Job(conf);
        job.setJarByClass(LoadIndex2Hbase.class);
        job.setMapperClass(LoadIDs2HbaseMapper.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);
        FileInputFormat.addInputPath(job, new Path(strings[1]));
        FileOutputFormat.setOutputPath(job, new Path(strings[2]));
        Configuration hbaseConfiguration= HBaseConfiguration.create();
        hbaseConfiguration.set("mapreduce.job.queuename", "dmp");
        hbaseConfiguration.set("mapreduce.job.name", "idmapping-bulkload-ids" + strings[1]);
        hbaseConfiguration.set("hbase.zookeeper.quorum", zkPath);
        HTable table =new HTable(hbaseConfiguration, "hbase_index");
        HFileOutputFormat2.configureIncrementalLoad(job, table, table);
        job.setNumReduceTasks(1000);
        int exitCode = job.waitForCompletion(true) == true ? 0 : 1;
        return  exitCode;
    }
}
