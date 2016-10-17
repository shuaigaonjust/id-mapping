import org.apache.hadoop.util.ToolRunner;

/**
 * Created by taochen4 on 2016/7/4.
 */
public class Main {

    private static void help() {
        System.out.println("Usage");
        System.out.println("  hadoop jar xx.jar [ids|index] input output");
    }

    public static void main(String[] args) throws Exception {
        int exitCode = 0;
        if (args[0].equals("ids")) {
            exitCode = ToolRunner.run(new LoadIDs2Hbase2(), args);
        } else if (args[0].equals("index")) {
            exitCode = ToolRunner.run(new LoadIndex2Hbase(), args);
        }else if (args[0].equals("update_zk")) {
            String zkPath = "10.10.12.82,10.10.12.83,10.10.12.84";
            String zkIndexPath = "/idmapping/active_index";
            String zkIdsPath = "/idmapping/active_ids";
            ConnectWatcher connectWatcher = new ConnectWatcher();
            String zkIndexName = new String();
            String zkTableName = new String();
            connectWatcher.connect(zkPath);
            zkIndexName = "";
            zkIndexName = connectWatcher.getData(zkIndexPath, null);
            if (zkIndexName.equals("idmapping_index_1")) {
                zkIndexName = "idmapping_index_2";
            } else if (zkIndexName.equals("idmapping_index_2")) {
                zkIndexName = "idmapping_index_1";
            } else {
                throw new Exception("index table name is not valid :" + zkIndexName);
            }
            zkTableName = "";
            zkTableName = connectWatcher.getData(zkIdsPath, null);
            zkTableName = zkTableName.equals("idmapping_ids_2")?"idmapping_ids_1":"idmapping_ids_2";
            connectWatcher.setData(zkIdsPath, zkTableName);
            connectWatcher.setData(zkIndexPath, zkIndexName);
        } else {
            help();
        }
        System.exit(exitCode);
    }
}
