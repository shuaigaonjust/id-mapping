package com.iflytek.hadoop.idmapping.util;

import ids.IDs;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class IdMappingUtil {
	public static Random random = null;

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

	public synchronized static IDs setIds(IDs tempIDs, boolean isNeedGID)
	{
		IDs idss = new IDs();
		idss.setGlobalId("");
		idss.setImei(tempIDs.getImei());
		idss.setUid(tempIDs.getUid());
		idss.setMac(tempIDs.getMac());
		idss.setIdfa(tempIDs.getIdfa());
		idss.setOpenudid(tempIDs.getOpenudid());
		idss.setImsi(tempIDs.getImsi());
		idss.setPhoneNumber(tempIDs.getPhoneNumber());
		idss.setDid(tempIDs.getDid());
		idss.setAndroidId(tempIDs.getAndroidId());
		if (isNeedGID == true) {
			idss.setGlobalId(getOneGlobal_Id(idss));
		}
		return idss;
	}

	public synchronized static String getOneGlobal_Id(IDs ids){
		if(ids.getImei().size() != 0) {
			return ids.getImei().keySet().iterator().next();
		} else if(ids.getMac().size() != 0){
			return ids.getMac().keySet().iterator().next();
		} else if(ids.getOpenudid().size() != 0){
			return ids.getOpenudid().keySet().iterator().next();
		} else if(ids.getIdfa().size() != 0){
		    return ids.getIdfa().keySet().iterator().next();
		} else if(ids.getImsi().size() != 0){
			return ids.getImsi().keySet().iterator().next();
		} else if(ids.getPhoneNumber().size() != 0){
			return ids.getPhoneNumber().keySet().iterator().next();
		} else if(ids.getDid().size() != 0) {
			return ids.getDid().keySet().iterator().next();
		} else if(ids.getUid().size() != 0) {
			return ids.getUid().keySet().iterator().next();
		} else if(ids.getAndroidId().size() != 0) {
			return ids.getAndroidId().keySet().iterator().next();
		} else{
			return "";
		}
	}

	public synchronized static Map<String, Integer> getMapByIdType(IDs key, String type) {
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
		}
		return null;
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
}