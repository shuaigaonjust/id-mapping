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

/**
 * Created by work on 16/5/15.
 */

public class IDsOutputFormat extends FileOutputFormat<NullWritable, ids.IDs> {

    public RecordWriter<NullWritable, ids.IDs> getRecordWriter(TaskAttemptContext job)
            throws IOException, InterruptedException {

        Configuration conf = job.getConfiguration();
        Path file = getDefaultWorkFile(job, ".avro");
        FileSystem fs = file.getFileSystem(conf);
        FSDataOutputStream fileOut = fs.create(file, false);
        DataFileWriter<ids.IDs> dataFileWriter =
                new DataFileWriter<ids.IDs>(new GenericDatumWriter<ids.IDs>(ids.IDs.getClassSchema()));
        dataFileWriter.setCodec(CodecFactory.deflateCodec(8));
        dataFileWriter.create(ids.IDs.getClassSchema(), fileOut);
        return new IDsRecordWriter(dataFileWriter);
    }

    public static class IDsRecordWriter extends RecordWriter<NullWritable, ids.IDs> {

        private DataFileWriter<ids.IDs> dataFileWriter = null;
        public IDsRecordWriter(DataFileWriter<ids.IDs> dataFileWriter){
            this.dataFileWriter = dataFileWriter;
        }

        @Override
        public void write(NullWritable nullWritable, ids.IDs iDs) throws IOException, InterruptedException {
            dataFileWriter.append(iDs);
        }
        @Override
        public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            dataFileWriter.close();
        }
    }

}

