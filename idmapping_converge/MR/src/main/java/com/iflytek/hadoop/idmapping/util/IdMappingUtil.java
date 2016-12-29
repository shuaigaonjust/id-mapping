package com.iflytek.hadoop.idmapping.util;

import ids.IDs;

import java.io.IOException;
import java.security.MessageDigest;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class IdMappingUtil {
	public static Random random = null;
	public static String Random = "Random";
	private static final int ACTIVITY_LIMIT = 60;

	public synchronized static String getRandomString(String taskId, int taskNum) {
		if (random == null) {
			String key = taskId.split("_")[4];
			random = new Random(Integer.valueOf(key));
		}
		return Integer.toString(random.nextInt(taskNum));
     }


	public synchronized static IDs cloneIds(IDs ids){
		IDs temp = new IDs();
    	temp.setGlobalId(ids.getGlobalId());
    	temp.setImei(ids.getImei());
    	temp.setMac(ids.getMac());
    	temp.setIdfa(ids.getIdfa());
    	temp.setOpenudid(ids.getOpenudid());
    	temp.setImsi(ids.getImsi());
    	temp.setPhoneNumber(ids.getPhoneNumber());
    	temp.setUid(ids.getUid());
    	temp.setDid(ids.getDid());
    	temp.setAndroidId(ids.getAndroidId());
    	return temp;
	}

	public synchronized static String getGlobalId(IDs ids){
		return MD5(ids.toString());
	}

	public synchronized static Map<String, Integer> getMapByIdType(IDs key, String type) throws IOException {
		switch(type){
			case "imei":
				return key.getImei();
			case "mac":
				return  key.getMac();
			case "idfa":
				return key.getIdfa();
			case "openudid":
				return key.getOpenudid();
			case "imsi":
				return key.getImsi();
			case "phonenumber":
				return key.getPhoneNumber();
			case "android_id":
				return key.getAndroidId();
			case "all":
				Map<String, Integer> map = new HashMap<String, Integer>();
				map.putAll(key.getImei());
				map.putAll(key.getMac());
				map.putAll(key.getIdfa());
				map.putAll(key.getOpenudid());
				map.putAll(key.getImsi());
				map.putAll(key.getPhoneNumber());
				map.putAll(key.getAndroidId());
				return map;
			default:
				throw new IOException();
		}
	}

	/* addIdToMap
		*  将mapIn的数据添加到mapOut，如果存在则更新mapOut的value为较小值
		*  如果mapOut size大于10,true,否则返回false
		* */
	private static String idKey = "";
	private static Integer idValue = Integer.MAX_VALUE;
	public synchronized static boolean addIdToMap(Map<String, Integer> mapIn, Map<String, Integer> mapOut) {
			for ( Map.Entry<String, Integer> id :mapIn.entrySet()) {
				idKey = id.getKey();
				idValue = id.getValue();
				if (idValue > ACTIVITY_LIMIT) {
					continue;
				}
				if(mapOut.containsKey(idKey) == true) {
					if(idValue < mapOut.get(idKey)){
					    mapOut.put(idKey, idValue);
					}
				}else{
					mapOut.put(idKey, idValue);
				}
		    }
			if (mapOut.size() > 10) {
				return true;
			}
		return false;
	}

	// 因为IDs的成员没有初始化，所以做一次初始化
	public synchronized static void initIDs(IDs ids) {
		ids.setGlobalId("");
		ids.setImei(new HashMap<String, Integer>());
		ids.setDid(new HashMap<String, Integer>());
		ids.setUid(new HashMap<String, Integer>());
		ids.setOpenudid(new HashMap<String, Integer>());
		ids.setIdfa(new HashMap<String, Integer>());
		ids.setImsi(new HashMap<String, Integer>());
		ids.setPhoneNumber(new HashMap<String, Integer>());
		ids.setMac(new HashMap<String, Integer>());
		ids.setAndroidId(new HashMap<String, Integer>());
	}

	public synchronized static boolean convergeIDAndCheckSize(IDs ids, IDs tempIDs) {
		return addIdToMap(ids.getImei(), tempIDs.getImei())
				|| addIdToMap(ids.getUid(),  tempIDs.getUid())
		        || addIdToMap(ids.getMac(),  tempIDs.getMac())
				|| addIdToMap(ids.getIdfa(), tempIDs.getIdfa())
				|| addIdToMap(ids.getOpenudid(), tempIDs.getOpenudid())
				|| addIdToMap(ids.getImsi(), tempIDs.getImsi())
				|| addIdToMap(ids.getPhoneNumber(), tempIDs.getPhoneNumber())
				|| addIdToMap(ids.getDid(), tempIDs.getDid())
				|| addIdToMap(ids.getAndroidId(), tempIDs.getAndroidId());
	}

	public synchronized static void convergeID(IDs ids, IDs tempIDs) {
		addIdToMap(ids.getImei(), tempIDs.getImei());
		addIdToMap(ids.getUid(),  tempIDs.getUid());
		addIdToMap(ids.getMac(),  tempIDs.getMac());
		addIdToMap(ids.getIdfa(), tempIDs.getIdfa());
		addIdToMap(ids.getOpenudid(), tempIDs.getOpenudid());
		addIdToMap(ids.getImsi(), tempIDs.getImsi());
		addIdToMap(ids.getPhoneNumber(), tempIDs.getPhoneNumber());
		addIdToMap(ids.getDid(), tempIDs.getDid());
		addIdToMap(ids.getAndroidId(), tempIDs.getAndroidId());
	}

	public final static String MD5(String s) {
		char hexDigits[]={'0','1','2','3','4','5','6','7','8','9','A','B','C','D','E','F'};
		try {
			byte[] btInput = s.getBytes();
			// 获得MD5摘要算法的 MessageDigest 对象
			MessageDigest mdInst = MessageDigest.getInstance("MD5");
			// 使用指定的字节更新摘要
			mdInst.update(btInput);
			// 获得密文
			byte[] md = mdInst.digest();
			// 把密文转换成十六进制的字符串形式
			int j = md.length;
			char str[] = new char[j * 2];
			int k = 0;
			for (int i = 0; i < j; i++) {
				byte byte0 = md[i];
				str[k++] = hexDigits[byte0 >>> 4 & 0xf];
				str[k++] = hexDigits[byte0 & 0xf];
			}
			return new String(str);
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}
}