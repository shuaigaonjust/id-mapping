package com.iflytek.cloudplatform.dmp.idmapping;

import com.google.gson.Gson;
import ids.IDs;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.io.IOException;

public class IDMappingClient2 {

    private static final Logger LOG = LoggerFactory.getLogger(IDMappingClient2.class);
    private String zkPath;
    private String zkIdsPath = "/idmapping/active_ids";
    private String zkIndexPath = "/idmapping/active_index";
    private ConnectWatcher connectWatcher = new ConnectWatcher();
    private String zkTableName = new String();
    private String zkIndexName = new String();
    private Configuration conf;
    private HTable hTableIDs;
    private HTable hTableIndex;
    private Gson gson;
    private boolean inited = false;

    public synchronized void setZkPath(String path) {
        this.zkPath = path;
        System.out.println("set zkPath to " + zkPath);
    }

    public synchronized void init(){
        if (inited == true) {
            return;
        }
        zkPath = "hfa-pro0041.hadoop.cpcc.iflyyun.cn,hfa-pro0043.hadoop.cpcc.iflyyun.cn,hfa-pro0042.hadoop.cpcc.iflyyun.cn";
        conf =  HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", zkPath);
        try {
            connectWatcher.connect(zkPath);
            zkTableName = connectWatcher.getData(zkIdsPath, new Watcher() {
                public void process(WatchedEvent event) {
                    try {
                        zkTableName = connectWatcher.getData(zkIdsPath, this);
                        hTableIDs.close();
                        hTableIDs = new HTable(conf, zkTableName);
                        LOG.info("HTable IDs Changed :" + zkTableName);
                    } catch (KeeperException e) {
                        e.printStackTrace();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            });
            zkIndexName = connectWatcher.getData(zkIndexPath, new Watcher() {
                public void process(WatchedEvent event) {
                    try {
                        zkIndexName = connectWatcher.getData(zkIndexPath, this);
                        hTableIndex.close();
                        hTableIndex = new HTable(conf, zkIndexName);
                        LOG.info("HTable Index Changed :" + zkIndexName);
                    } catch (KeeperException e) {
                        e.printStackTrace();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            });
            hTableIDs = new HTable(conf, zkTableName);
            hTableIndex = new HTable(conf, zkIndexName);
            System.out.println("HTable IDs  :" + zkTableName);
            System.out.println("HTable Index :" + zkIndexName);
        } catch (IOException e) {
            System.err.println("HBase connect failed!");
            System.exit(-1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
        gson = new Gson();
        inited = true;
    }

    public synchronized void close() throws IOException, InterruptedException {
        hTableIDs.close();
        hTableIndex.close();
        connectWatcher.close();
        inited = false;
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

    public  IDs getIDs(String key) throws IOException {
        IDs ids = new IDs();
        Result  result = getResult(key);
        if(result != null && !result.isEmpty()) {
            byte[] tmpBytes = result.getValue(Bytes.toBytes("ids"), Bytes.toBytes("value"));
            if (tmpBytes != null) {
                String tmp = Bytes.toString(tmpBytes);
                ids = gson.fromJson(tmp, ids.getClass());
            }
        }
        return ids;
    }

    public static void help() {
        System.out.println("Usage:");
        System.out.println("  java -jar idmapping_open_api.jar key [zkPath]");
        System.out.println("  ---key the id which you wan to search");
        System.out.println("  ---zkPath Optional , hbase zookeeper path");
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        if (args.length != 1 && args.length != 2 ) {
            help();
            System.exit(-1);
        }

        IDMappingClient2 idMappingClient = new IDMappingClient2();
        idMappingClient.init();
        if(args.length == 2) {
            idMappingClient.setZkPath(args[1]);
        }

        String result = "";
        String key = args[0];

        IDs ids = idMappingClient.getIDs(key);
        if(ids == null) {
            result = null;
        } else {
            result = ids.toString();
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
        String empty_str = "{\"Global_Id\": null, \"Imei\": null, \"Mac\": null, \"Imsi\": null, \"Phone_Number\": null, \"Idfa\": null, \"Openudid\": null, \"Uid\": null, \"Did\": null, \"Android_Id\": null}";
        if(result != null && result.equals(empty_str)) {
            System.out.println("Key[" + key + "] is not find in database\n");
        } else {
            System.out.println("Result     : " + result + "\n");
        }
        idMappingClient.close();
    }
}