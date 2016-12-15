package com.iflytek.hadoop.idmapping.main;

import com.iflytek.hadoop.idmapping.mapreduce.ExtractIndexToTextMR;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ExtractIndexToText extends Configured implements Tool {

    public static final Log log = LogFactory.getLog(ExtractIndexToText.class);

    @Override
    public int run(String[] args) throws Exception {
        if (args == null || args.length < 1) {
            System.err.println("Usage: <desc>");
            return -1;
        }
        getConf().set("mapreduce.job.queuename", "dmp");
        FileSystem fs = FileSystem.get(getConf());
        Path idIndexParentPath = new Path("/user/compass/public/hive/idmapping/index");
        FileStatus[] fileStatus = fs.listStatus(idIndexParentPath);
        FileStatus idIndexFileStatus = null;
        for (FileStatus fis : fileStatus) {
            if (idIndexFileStatus == null || idIndexFileStatus.getModificationTime()< fis.getModificationTime()) {
                idIndexFileStatus = fis;
            }
        }
        Path idIndexPath = idIndexFileStatus.getPath();
        System.out.println("input path is :" + idIndexPath.toString());

        String output = args[0];
        Path tmpOutput = new Path(output);
        if (fs.exists(tmpOutput)) {
            fs.delete(tmpOutput, true);
        }
        Job job = new Job(getConf());
        job.setJarByClass(ExtractIndexToText.class);

        job.setJobName(ExtractIndexToText.class.getName());
        job.setMapperClass(ExtractIndexToTextMR.ExtractIndexToTextMapper.class);
        job.setNumReduceTasks(0);
        AvroJob.setInputKeySchema(job, ids.Index.getClassSchema());
        FileOutputFormat.setOutputPath(job, tmpOutput);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        MultipleInputs.addInputPath(job, idIndexPath,
                AvroKeyInputFormat.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        System.out.println(org.apache.commons.lang.StringUtils.join(args));
        int res = ToolRunner.run(new ExtractIndexToText(), args);
        System.exit(res);
    }
}







