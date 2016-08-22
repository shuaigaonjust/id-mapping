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
            exitCode = ToolRunner.run(new LoadIDs2Hbase(), args);
        } else if (args[0].equals("ids2")) {
            exitCode = ToolRunner.run(new LoadIDs2Hbase2(), args);
        } else if (args[0].equals("index")) {
            exitCode = ToolRunner.run(new LoadIndex2Hbase(), args);
        } else {
            help();
        }
        System.exit(exitCode);
    }
}
