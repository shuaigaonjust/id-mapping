//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

package ids;

import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class IDsOutputFormat extends FileOutputFormat<NullWritable, IDs> {
    static final DataFileWriter<IDs> dataFileWriter = new DataFileWriter(new GenericDatumWriter(IDs.getClassSchema()));

    public IDsOutputFormat() {
    }

    public RecordWriter<NullWritable, IDs> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
        Configuration conf = job.getConfiguration();
        Path file = this.getDefaultWorkFile(job, ".avro");
        FileSystem fs = file.getFileSystem(conf);
        FSDataOutputStream fileOut = fs.create(file, false);
        dataFileWriter.setCodec(CodecFactory.deflateCodec(8));
        dataFileWriter.create(IDs.getClassSchema(), fileOut);
        return new IDsOutputFormat.IDsRecordWriter();
    }

    public static class IDsRecordWriter extends RecordWriter<NullWritable, IDs> {
        public IDsRecordWriter() {
        }

        public void write(NullWritable nullWritable, IDs iDs) throws IOException, InterruptedException {
            IDsOutputFormat.dataFileWriter.append(iDs);
        }

        public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            IDsOutputFormat.dataFileWriter.close();
        }
    }
}
