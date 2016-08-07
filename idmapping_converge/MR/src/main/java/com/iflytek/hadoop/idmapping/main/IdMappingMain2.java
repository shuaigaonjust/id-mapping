package com.iflytek.hadoop.idmapping.main;

import com.iflytek.hadoop.idmapping.constants.ShareConstants;
import com.iflytek.hadoop.idmapping.mapreduce.IdMappingMR2;
import ids.IDs;
import ids.IDsOutputFormat;
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


public class IdMappingMain2 implements Tool {

	public static final Log log = LogFactory
			.getLog(IdMappingMain2.class);

	private Configuration conf = new Configuration();

	@Override
	public int run(String[] args) throws Exception {

		if (args == null || args.length != 4) {
			System.out.println("Usage: <input> <output> <dataname> <starttime>");
			throw new IllegalArgumentException("this method need five args at least!");
		}

		int length = args.length;
		ArrayList<String> input = new ArrayList<String>();
		for(int i = 0; i < length-3; i++){
			input.add(args[i]);
		}

		String output = args[length-3];
		String dataname = args[length-2];
		String starttime = args[length-1];

		@SuppressWarnings("deprecation")
		Job job = new Job(conf);
		job.setJarByClass(IdMappingMain2.class);
		job.getConfiguration().set(ShareConstants.EXTRACT_TIME, starttime);
		job.getConfiguration().set(ShareConstants.EXTRACT_DATANAME, dataname);
		job.setJobName(IdMappingMain2.class.getName() + ":" + dataname
				+ "-" + starttime);
		job.setMapperClass(IdMappingMR2.IdMappingM2.class);
		job.setReducerClass(IdMappingMR2.IdMappingR2.class);

		String outputDir = output + "/" + "product=" + dataname + "/" + "day=" + starttime;
		FileOutputFormat.setOutputPath(job, new Path(outputDir));

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IDs.class);

    	job.setOutputFormatClass(IDsOutputFormat.class);

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
		int res = ToolRunner.run(new IdMappingMain2(), args);
		System.exit(res);
	}
}








