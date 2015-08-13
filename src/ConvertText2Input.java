
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



/**
 * Converts plain-text data into our integer-sequence input format.
 *
 * @author Klaus Berberich (kberberi@mpi-inf.mpg.de)
 */
public class ConvertText2Input  {

    public static class MapOne extends Mapper<LongWritable, Text, Text, LongWritable> {

        // singleton output key -- for efficiency reasons
        private final Text outKey = new Text();

        // singleton output value -- for efficiency reasons
        private final LongWritable outValue = new LongWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            Object2LongOpenHashMap<String> wordCounts = new Object2LongOpenHashMap<String>();

            // somewhat naive text normalization
            String text = value.toString();
            text = text.toLowerCase();
            text = text.replaceAll("[^a-z0-9']", " ");

            // aggregate word counts
            for (String word : text.split("\\s+")) {
                long count = wordCounts.getLong(word);
                wordCounts.put(word, count + 1);
            }

            // emit aggregated word counts
            for (String word : wordCounts.keySet()) {
                outKey.set(word);
                outValue.set(wordCounts.get(word));
                context.write(outKey, outValue);
            }
        }
    }

    public static class CombinerOne extends Reducer<Text, LongWritable, Text, LongWritable> {

        // singleton output key -- for efficiency reasons
        private final Text outKey = new Text();

        // singleton output value -- for efficiency reasons
        private final LongWritable outValue = new LongWritable();

        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long count = 0L;
            for (LongWritable value : values) {
                count += value.get();
            }
            outKey.set(key.toString());
            outValue.set(count);
            context.write(outKey, outValue);
        }
    }

    public static class ReduceOne extends Reducer<Text, LongWritable, Text, Text> {

        // reduce task's identifier
        private int taskId;

        // total number of reduce tasks
        private int numRed;

        // singleton output key -- for efficiency reasons
        private final Text outKey = new Text();

        // singleton output value -- for efficiency reasons
        private final Text outValue = new Text();

        // counts
        private final Object2LongOpenHashMap<String> counts = new Object2LongOpenHashMap<String>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            taskId = context.getTaskAttemptID().getTaskID().getId();
            numRed = context.getNumReduceTasks();
        }

        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long count = 0L;
            for (LongWritable value : values) {
                count += value.get();
            }
            counts.put(key.toString(), count);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            String[] words = counts.keySet().toArray(new String[0]);

            // sort words in ascending order of their count
            Arrays.sort(words, new Comparator<String>() {
                @Override
                public int compare(String s1, String s2) {
                    long cf1 = counts.get(s1);
                    long cf2 = counts.get(s2);
                    if (cf1 < cf2) {
                        return +1;
                    } else if (cf1 > cf2) {
                        return -1;
                    }
                    return 0;
                }
            });

            // assign term identifiers in acending order of count
            int tid = taskId + 1;
            for (String word : words) {
                outKey.set(word);
                outValue.set(tid + "\t" + counts.get(word));
                context.write(outKey, outValue);
                tid += numRed;
            }
        }
    }

    public static class MapTwo extends Mapper<LongWritable, Text, LongWritable, IntArrayWritable> {

        // singleton output key -- for efficiency reasons
        private final LongWritable outKey = new LongWritable();

        // singleton output value -- for efficiency reasons
        private final IntArrayWritable outValue = new IntArrayWritable();

        // dictionary mapping words to their term identifier
        private final Object2IntOpenHashMap<String> dict = new Object2IntOpenHashMap<String>();

        // buffer to collect document contents
        private final IntArrayList buffer = new IntArrayList();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            for (Path cachedPath : DistributedCache.getLocalCacheFiles(context.getConfiguration())) {
                if (cachedPath.toString().contains("dic") && cachedPath.toString().contains("part")) {
                    BufferedReader br = new BufferedReader(new FileReader(cachedPath.toString()));
                    while (br.ready()) {
                        String[] tokens = br.readLine().split("\\t");
                        String term = tokens[0];
                        int tid = Integer.parseInt(tokens[1]);
                        dict.put(term, tid);
                    }
                }
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            buffer.clear();

            // somewhat naive text normalization
            String text = value.toString();
            text = text.toLowerCase();
            text = text.replaceAll("[^a-z0-9']", " ");

            for (String word : text.split("\\s+")) {
                buffer.add(dict.getInt(word));
            }
            buffer.add(0);

            outKey.set(key.get());
            outValue.setContents(buffer.toIntArray());
            context.write(outKey, outValue);
        }
    }

}

