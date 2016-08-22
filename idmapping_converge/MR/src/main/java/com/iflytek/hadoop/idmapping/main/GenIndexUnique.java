package com.iflytek.hadoop.idmapping.main;

import com.iflytek.hadoop.idmapping.mapreduce.GenIndexUniqueMR;
import ids.Index;
import ids.IndexOutputFormat;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class GenIndexUnique implements Tool {

    public static final Log log = LogFactory.getLog(GenIndexUnique.class);
    private Configuration conf = new Configuration();

    @Override
    public int run(String[] args) throws Exception {

        if (args == null || args.length < 2) {
            System.out.println("Usage: <input> <output>");
            throw new IllegalArgumentException("this method need two args at least!");
        }

        int length = args.length;
        ArrayList<String> input = new ArrayList<String>();
        input.add(args[length-2]);
        String output = args[length-1];
        Job job = new Job(conf);
        job.setJarByClass(GenIndexUnique.class);

        job.setJobName(GenIndexUnique.class.getName());
        job.setMapperClass(GenIndexUniqueMR.GenIndexUniqueMapper.class);
        job.setReducerClass(GenIndexUniqueMR.GenIndexUniqueReducer.class);
        AvroJob.setInputKeySchema(job, ids.IDs.getClassSchema());
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Index.class);

        job.setOutputFormatClass(IndexOutputFormat.class);
        FileSystem fs = FileSystem.get(getConf());
        for(int i = 0; i < input.size(); i++){
            List<Path> lstTmpInput = parseWordcardDir(fs, input.get(i).toString());
            if (null == lstTmpInput || lstTmpInput.size() <= 0) {
                continue;
            }

            for (Path tmpInput : lstTmpInput) {
                log.info("input file[" + tmpInput.toString());
                MultipleInputs.addInputPath(job, tmpInput,
                        AvroKeyInputFormat.class);
            }
        }
        job.waitForCompletion(true);
        return 0;
    }

    @Override
    public void setConf(Configuration configuration) {
        this.conf = configuration;
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    public static List<Path> parseWordcardDir(FileSystem fs, String wordcardDir)
            throws IOException {
        List<Path> lstPath = new ArrayList<Path>();

        FileStatus[] aStatus = fs.globStatus(new Path(wordcardDir));
        if (null == aStatus || aStatus.length <= 0) {
            return lstPath;
        }

        Path[] tmpLstPath = FileUtil.stat2Paths(aStatus);
        lstPath.addAll(Arrays.asList(tmpLstPath));
        return lstPath;
    }

    public static void main(String[] args) throws Exception {
        System.out.println(org.apache.commons.lang.StringUtils.join(args));
        int res = ToolRunner.run(new GenIndexUnique(), args);
        System.exit(res);
    }
}







