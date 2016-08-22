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

public class IndexOutputFormat extends FileOutputFormat<NullWritable, Index> {
    static final DataFileWriter<Index> dataFileWriter = new DataFileWriter(new GenericDatumWriter(Index.getClassSchema()));

    public IndexOutputFormat() {
    }

    public RecordWriter<NullWritable, Index> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
        Configuration conf = job.getConfiguration();
        Path file = this.getDefaultWorkFile(job, ".avro");
        FileSystem fs = file.getFileSystem(conf);
        FSDataOutputStream fileOut = fs.create(file, false);
        dataFileWriter.setCodec(CodecFactory.deflateCodec(8));
        dataFileWriter.create(Index.getClassSchema(), fileOut);
        return new IndexOutputFormat.IndexRecordWriter();
    }

    public static class IndexRecordWriter extends RecordWriter<NullWritable, Index> {
        public IndexRecordWriter() {
        }

        public void write(NullWritable nullWritable, Index index) throws IOException, InterruptedException {
            IndexOutputFormat.dataFileWriter.append(index);
        }

        public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            IndexOutputFormat.dataFileWriter.close();
        }
    }
}
