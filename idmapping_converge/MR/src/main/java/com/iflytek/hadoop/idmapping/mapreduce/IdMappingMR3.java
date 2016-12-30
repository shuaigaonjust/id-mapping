package com.iflytek.hadoop.idmapping.mapreduce;
import com.iflytek.hadoop.idmapping.util.IdMappingUtil;
import ids.IDs;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.TreeSet;

public class IdMappingMR3 {
    public static class IdMappingM3 extends Mapper<AvroKey<IDs>, NullWritable, Text, IDs>{
		/* step III map class
		*  将ids中的计算id的String作为key输出
		* */
		@Override
		public void map(AvroKey<IDs> key,NullWritable value,Context context) throws IOException,InterruptedException {
			StringBuffer sb = new StringBuffer();
			sb.append(new TreeSet(key.datum().getImei().keySet()));
			sb.append(new TreeSet(key.datum().getMac().keySet()));
			sb.append(new TreeSet(key.datum().getOpenudid().keySet()));
			sb.append(new TreeSet(key.datum().getIdfa().keySet()));
			sb.append(new TreeSet(key.datum().getImsi().keySet()));
			sb.append(new TreeSet(key.datum().getAndroidId().keySet()));
			sb.append(new TreeSet(key.datum().getPhoneNumber().keySet()));
			context.write(new Text(sb.toString()), key.datum());
		}
    }

    public static class IdMappingR3 extends Reducer<Text,IDs,NullWritable,IDs>{
        /* step III reduce
        *  聚合并产生global_id
        * */
		@Override
		public void reduce(Text key,Iterable<IDs> values,Context context) throws IOException,InterruptedException {
			IDs ids = new IDs();
			for (IDs tmpIDs : values ) {
				ids.updateIDs(tmpIDs);
			}
			String tempGlobalId = IdMappingUtil.getGlobalId(ids);
			ids.setGlobalId(tempGlobalId);
			context.write(null, ids);
		}
    }
}