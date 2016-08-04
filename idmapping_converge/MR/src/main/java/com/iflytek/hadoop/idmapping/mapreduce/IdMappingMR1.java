package com.iflytek.hadoop.idmapping.mapreduce;

import com.iflytek.hadoop.idmapping.constants.ShareConstants;
import com.iflytek.hadoop.idmapping.util.IdMappingUtil;
import ids.IDs;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

public class IdMappingMR1 {
	/*  idmapping step I mapper class
	*   1) 将global_id初始化为空;
	*   2) 如果该ID size > 0，则选取一个作为step II的key做还原
	*   3) 每个ID作为key，输出一遍，用于聚合
	* */
    public static class IdMappingM1 extends Mapper<AvroKey<IDs>,NullWritable,Text,IDs> {
		@Override
		public void map(AvroKey<IDs> key,NullWritable value,Context context) throws IOException,InterruptedException {
			String id = context.getConfiguration().get(ShareConstants.ID);
			context.getCounter("Counters", id + "-before").increment(1);
            Map<String,Integer> ids = IdMappingUtil.getMapByIdType(key.datum() ,id);
            // 初始化global_id为空
            key.datum().setGlobalId("");
            if( ids!= null && ids.size() != 0){
				// 选取一个ID用来做stpe II的还原操作， 增加随机值是为了按原样还原
            	String secondKey = (String) ids.keySet().toArray()[0] + IdMappingUtil.getRandomString(context.getTaskAttemptID().toString(),Integer.MAX_VALUE);
            	key.datum().setGlobalId(secondKey);
            	context.getCounter("idmapping","befor").increment(1);
	        	for(Map.Entry<String, Integer> entry: ids.entrySet()){
	        		String idValue = entry.getKey();
					context.write(new Text(idValue), key.datum());
					context.getCounter("idmapping","decentralization").increment(1);
	        	}
            }else{
				// 如果ID为空，则标识保证不参与计算，并增加随机值保证负载平衡
				context.write(new Text(id + "_" + IdMappingUtil.getRandomString(context.getTaskAttemptID().toString(),context.getNumReduceTasks())), key.datum());
            }
         }
    }
	/* idmapping step I reduce class
	*  补全相同ID对应的全部ID，不去重，去重在step II
	*  注：对ID为空值处理，不处理直接输出，使用random做key来保证负载平衡
	* */
    public static class IdMappingR1 extends Reducer<Text,IDs,NullWritable,IDs> {
		@Override
		public void reduce(Text key,Iterable<IDs> values,Context context)
                   throws IOException,InterruptedException{
			// 0. 获取ID类型
			String id = context.getConfiguration().get(ShareConstants.ID);
	     	// 1. 判断ID是否为空值，即key是否以ID类型+'_'开头，如果是直接输出
			if(key.toString().startsWith(id + "_")){
				for(IDs ids: values){
	                   context.write(NullWritable.get(), ids);
				}
				return;
	     	}
			// 2. 聚合数据
			IDs tempIDs = new IDs();
			IdMappingUtil.initIDs(tempIDs);
			ArrayList<String> secondKeys = new ArrayList<String>();
			// 2.1 聚合ID，并输出，发现超过10个则停止
			Boolean isOverTen = false;
			for (IDs ids : values) {
				context.write(null, ids);
				context.getCounter("idmapping","reduce_key_is_not_null").increment(1);
				String tempGlobalId = ids.getGlobalId();
				if(!secondKeys.contains(tempGlobalId))
				   secondKeys.add(tempGlobalId);
				if (IdMappingUtil.convergeIDAndCheckSize(ids, tempIDs) == true) {
					  isOverTen = true;
					  break;
				}else{
					  continue;
				}
			}
			/* 2.2 输出阶段，如果聚合ID超过10个，直接输出，否则将聚合后的结果输出
			*  无论有没有超过10个，原始结果都会输出一遍，不影响后续还原聚合（没有增加数据）
			*  目的是为了处理异常情况，否则大于10个要保存，会造成reduce OOM问题
			 */
			if (isOverTen == true) {
			   for(IDs ids:values){
				context.write(null, ids);
			   }
			} else {
			   for(String secondKey : secondKeys){
			   	context.getCounter("idmapping","secondkeywrite").increment(1);
					// 用global_id来保存secondKey，用作step II还原的key
			   	tempIDs.setGlobalId(secondKey);
			        context.write(null, tempIDs);
			   }
			}
		}
    }
}