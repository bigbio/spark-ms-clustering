package org.big.bio.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileAlreadyExistsException;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Output clusters into .clustering file format
 *
 * @author Yasset Perez-Riverol
 */
public class ClusteringFileOutputFormat extends TextOutputFormat<String, String> {

    private static final String CLUSTERING_FILE_EXTENSION = ".clustering";

    @Override
    public Path getDefaultWorkFile(TaskAttemptContext context, String extension) throws IOException {
        Configuration configuration = context.getConfiguration();
        String prefix = configuration.get("pride.cluster.clustering.file.prefix", "");

        FileOutputCommitter committer = (FileOutputCommitter)this.getOutputCommitter(context);
        return new Path(committer.getWorkPath(), getUniqueFile(context, prefix, extension));
    }


    @Override
    public RecordWriter<String, String> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
        Configuration configuration = job.getConfiguration();

        Path outputFile = this.getDefaultWorkFile(job, CLUSTERING_FILE_EXTENSION);
        FileSystem fs = outputFile.getFileSystem(configuration);
        FSDataOutputStream fileOut = fs.create(outputFile, true);
        return new ClusteringFileRecordWriter(fileOut);
    }

    /**
     * The override method allow us to write the final output when the folder exists
     * @param job Job
     * @throws FileAlreadyExistsException File already exists exception.
     * @throws IOException IO Exception
     */
    @Override
    public void checkOutputSpecs(JobContext job) throws IOException {
        if(job.getConfiguration().get("spark.hadoop.validateOutputSpecs") == null || Boolean.parseBoolean(job.getConfiguration().get("spark.hadoop.validateOutputSpecs")))
            super.checkOutputSpecs(job);
    }

    private static class ClusteringFileRecordWriter extends RecordWriter<String, String> {

        private final DataOutputStream out;

        public ClusteringFileRecordWriter(DataOutputStream out) {
            this.out = out;
        }

        @Override
        public void write(String key, String value) throws IOException, InterruptedException {
            out.write(value.getBytes());
        }

        @Override
        public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            out.close();
        }
    }

}
