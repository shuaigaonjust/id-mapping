package com.iflytek.cloudplatform.dmp.idmapping;

import com.google.gson.Gson;
import ids.IDs;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class IDMappingClient {

    private String zkPath;
    private Configuration conf;
    private HTable hTableIDs;
    private HTable hTableIndex;
    private Gson gson;
    private boolean inited = false;

    public synchronized void setZkPath(String path) {
        this.zkPath = path;
    }

    public synchronized void init() {
        if (inited == true) {
            return;
        }
        zkPath = "idmapping-hbase-node1,idmapping-hbase-node2,idmapping-hbase-node3";
        conf =  HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", zkPath);
        try {
            hTableIDs = new HTable(conf, "hbase_ids");
            hTableIndex = new HTable(conf, "hbase_index");
        } catch (IOException e) {
            System.err.println("HBase connect failed!");
            System.exit(-1);
        }
        gson = new Gson();
        inited = true;
    }

    public synchronized void close() throws IOException {
        hTableIDs.close();
        hTableIndex.close();
    }

    public String getGlobalID(String key) throws IOException {
        Get globalKeyGet = new Get(Bytes.toBytes(key));
        Result keyResult = hTableIndex.get(globalKeyGet);
        if (keyResult.isEmpty()) {
            return null;
        }
        return Bytes.toString(keyResult.getValue(Bytes.toBytes("global_id"), Bytes.toBytes("value")));
    }

    private synchronized Result getResult(String key) throws IOException {
        String globalID = getGlobalID(key);
        if (globalID == null) {
            return null;
        }
        Get idsKeyGet = new Get(Bytes.toBytes(globalID));
        Result idsResult = hTableIDs.get(idsKeyGet);
        return idsResult;
    }

    private Map<String, Integer> getItemByKey(String key, Result result, String item) throws IOException {
        Map<String, Integer> tmpMap = new HashMap<String, Integer>();
        if (key == null || item == null || key.length() == 0 || item.length() == 0) {
            return tmpMap;
        }

        if(result == null) {
            result = getResult(key);
        }

        if(result != null && !result.isEmpty()) {
            byte[] tmpBytes = result.getValue(Bytes.toBytes(item), Bytes.toBytes("value"));
            if (tmpBytes != null) {
                String tmp = Bytes.toString(tmpBytes);
                tmpMap = gson.fromJson(tmp, tmpMap.getClass());
            }
        }
        return tmpMap;
    }

    public  Map<String, Integer> getImei(String key) throws IOException {
        return getItemByKey(key, null, "imei");
    }

    public Map<String, Integer> getMac(String key) throws IOException {
        return getItemByKey(key, null, "mac");
    }

    public Map<String, Integer> getImsi(String key) throws IOException {
        return getItemByKey(key, null, "mac");
    }

    public Map<String, Integer> getPhoneNumber(String key) throws IOException {
        return getItemByKey(key, null, "phone_number");
    }

    public Map<String, Integer> getIdfa(String key) throws IOException {
        return getItemByKey(key, null, "phone_number");
    }

    public Map<String, Integer> getOpenudid(String key) throws IOException {
        return getItemByKey(key, null, "openudid");
    }

    public Map<String, Integer> getDid(String key) throws IOException {
        return getItemByKey(key, null, "did");
    }

    public Map<String, Integer> getUid(String key) throws IOException {
        return getItemByKey(key, null, "uid");
    }

    /**
     * @param key
     * @return null if not exsits, else IDs
     */
    public IDs getIDs(String key) {
        IDs ids = new IDs();
        Map<String, Integer> tmpMap = new HashMap<String, Integer>();

        try {
            Result idsResult = getResult(key);
            if(!idsResult.isEmpty()) {
                ids.setImei(getItemByKey(key, idsResult, "imei"));
                ids.setMac(getItemByKey(key, idsResult, "mac"));
                ids.setImsi(getItemByKey(key, idsResult, "imsi"));
                ids.setPhoneNumber(getItemByKey(key, idsResult, "phone_number"));
                ids.setIdfa(getItemByKey(key, idsResult, "idfa"));
                ids.setOpenudid(getItemByKey(key, idsResult, "openudid"));
                ids.setDid(getItemByKey(key, idsResult, "did"));
                ids.setUid(getItemByKey(key, idsResult, "uid"));
            } else {
                ids = null;
            }
            // Todo android_id
        } catch (IOException e) {
            e.printStackTrace();
            ids = null;
        }
        return ids;
    }

    public static void help() {
        System.out.println("Usage:");
        System.out.println("  java -jar idmapping_open_api.jar key type [zkPath]");
        System.out.println("  ---key the id which you wan to search");
        System.out.println("  ---type all | global_id | imei | imsi | mac | phone_number | idfa | openudid | did | uid");
        System.out.println("  ---zkPath Optional , hbase zookeeper path");
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 2 && args.length != 3 ) {
            help();
            System.exit(-1);
        }

        IDMappingClient idMappingClient = new IDMappingClient();
        if(args.length == 3) {
            idMappingClient.setZkPath(args[2]);
            System.out.println("set zkPath to " + idMappingClient.zkPath);
        }

        String result = "";
        String type = args[0];
        String key = args[1];

        if (type.equals("global_id")) {
            idMappingClient.init();
            result = idMappingClient.getGlobalID(key);
        } else if (type.equals("all")) {
            idMappingClient.init();
            IDs ids = idMappingClient.getIDs(key);
            if(ids == null) {
                result = null;
            } else {
                result = ids.toString();
            }
        } else if (type.equals("imei")) {
            idMappingClient.init();
            result = idMappingClient.getImei(key).toString();
        } else if (type.equals("mac")) {
            idMappingClient.init();
            result = idMappingClient.getMac(key).toString();
        } else if (type.equals("imsi")) {
            idMappingClient.init();
            result = idMappingClient.getImsi(key).toString();
        }else if (type.equals("phone_number")) {
            idMappingClient.init();
            result = idMappingClient.getPhoneNumber(key).toString();
        } else if (type.equals("idfa")) {
            idMappingClient.init();
            result = idMappingClient.getIdfa(key).toString();
        } else if (type.equals("openudid")) {
            idMappingClient.init();
            result = idMappingClient.getOpenudid(key).toString();
        } else if (type.equals("did")) {
            idMappingClient.init();
            result = idMappingClient.getDid(key).toString();
        } else if (type.equals("uid")) {
            idMappingClient.init();
            result = idMappingClient.getUid(key).toString();
        } else {
            System.out.println("type is error");
            help();
            System.exit(-1);
        }

        System.out.println("\n");
        System.out.println("  ###  ##    #     #       #      ###    ###    #    #      #   ######  ");
        System.out.println("   #   ###   ##   ##      # #     # ##   # ##   #    # #    #  #     ##  ");
        System.out.println("   #   # ##  # # # #     #   #    #  ##  #  ##  #    #  #   #  #         ");
        System.out.println("   #   # ##  #  #  #    # # # #   ###    ###    #    #   #  #  #    #### ");
        System.out.println("   #   ###   #     #   #       #  #      #      #    #    # #  #     ##  ");
        System.out.println("  ###  ##    #     #  #         # #      #      #    #      #   #####    ");
        System.out.println("\nThank you for using id mapping!\n");
        System.out.println("Search key : " + key);
        if(result != null && result.equals("{}")) {
            System.out.println("Key[" + key + "] is not find in database\n");

        } else {
            System.out.println("Result     : " + result + "\n");
        }
        idMappingClient.close();
    }
}