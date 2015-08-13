


import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Suffix-based computation of (all|closed|maximal) n-gram frequencies.
 *
 * @author Klaus Berberich (kberberi@mpi-inf.mpg.de)
 */
public class NGSuffixSigma  {

    public static final class MapOne extends Mapper<LongWritable, IntArrayWritable, IntArrayWritable, IntWritable> {

        // minimum support threshold
        private int minsup;

        // maximum n-gram length considered
        private int maxlen;

        // set of frequent terms
        private final IntOpenHashSet tidSet = new IntOpenHashSet();

        // singleton output key -- for efficiency reasons
        private final IntArrayWritable outKey = new IntArrayWritable();

        // singleton output value -- for efficiency reasons
        private final IntWritable outValue = new IntWritable(1);

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            minsup = context.getConfiguration().getInt("de.mpii.ngrams.minsup", 1);
            maxlen = context.getConfiguration().getInt("de.mpii.ngrams.maxlen", Integer.MAX_VALUE);
            try {
                HashMap<String, String> dictionaryFiles = new HashMap<String, String>();
                for (Path cachedPath : DistributedCache.getLocalCacheFiles(context.getConfiguration())) {
                    if (cachedPath.toString().contains("dic") && cachedPath.toString().contains("part")) {
                        String file = cachedPath.toString();
                        dictionaryFiles.put(file.substring(file.lastIndexOf("/")), file);
                    }
                }
                ArrayList<String> fileNames = new ArrayList<String>(dictionaryFiles.keySet());
                for (String fileName : fileNames) {
                    BufferedReader br = new BufferedReader(new FileReader(dictionaryFiles.get(fileName)));
                    while (br.ready()) {
                        String[] tokens = br.readLine().split("\t");
                        int support = Integer.parseInt(tokens[2]);
                        if (support >= minsup) {
                            int tid = Integer.parseInt(tokens[1]);
                            tidSet.add(tid);
                        }
                    }
                    br.close();
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        protected void map(LongWritable key, IntArrayWritable value, Context context) throws IOException, InterruptedException {
            int[] contents = value.getContents();
            for (int b = 0, e = 0; b < contents.length; b++) {

                // ignore suffixes starting with infrequent term
                if (contents[b] == 0 || !tidSet.contains(contents[b])) {
                    continue;
                }

                // determine suffix end
                while (e <= b || (e < contents.length && tidSet.contains(contents[e]))) {
                    e++;
                }

                // extract and emit suffix                        
                int pb = b;
                int pe = (e - b > maxlen ? b + maxlen : e);
                outKey.setContents(contents, pb, pe);
                context.write(outKey, outValue);

                // adjust suffix end
                e = (e == b ? e + 1 : e);
            }
        }
    }

    public static final class ReduceOne extends Reducer<IntArrayWritable, IntWritable, IntArrayWritable, IntWritable> {

        // minimum support threshold
        private int minsup;

        // type of n-grams to be mined
        private int type;

        // singleton output key -- for efficiency reasons
        private final IntArrayWritable outKey = new IntArrayWritable();

        // singleton output value -- for efficiency reasons
        private final IntWritable outValue = new IntWritable();

        // stack to keep track of path to currently visited node
        private final IntArrayList stack = new IntArrayList();

        // counts to keep track of support on path to currently visited node 
        private final IntArrayList supports = new IntArrayList();

        // last n-gram emitted
        private int[] lastContents = new int[0];

        // support of last n-gram emitted
        private int lastSupport = 0;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            minsup = context.getConfiguration().getInt("de.mpii.ngrams.minsup", 0);
            type = context.getConfiguration().getInt("de.mpii.ngrams.type", NG.ALL);
        }

        @Override
        protected void reduce(IntArrayWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            // clean up stack
            int[] suffix = key.getContents();
            int lcp = StringUtils.lcp(stack, suffix);
            while (lcp < stack.size()) {
                int support = supports.remove(supports.size() - 1);
                if (support >= minsup) {
                    boolean isPrefix = (type == NG.ALL ? false : StringUtils.isPrefix(stack, lastContents));
                    boolean sameCount = (support == lastSupport);
                    boolean emit = true;

                    // skip if maximal n-grams desired
                    if (type == NG.MAXIMAL && isPrefix) {
                        emit = false;
                    }

                    // skip if closed n-grams desired
                    if (type == NG.CLOSED && isPrefix && sameCount) {
                        emit = false;
                    }

                    if (emit) {
                        int[] contents = stack.toIntArray();
                        outKey.setContents(contents);
                        outValue.set(support);
                        context.write(outKey, outValue);

                        lastContents = contents;
                        lastSupport = support;
                    }
                }
                stack.remove(stack.size() - 1);
                if (stack.size() > 0) {
                    supports.set(supports.size() - 1, supports.get(supports.size() - 1) + support);
                }
            }

            // determine support
            int support = 0;
            for (IntWritable value : values) {
                support += value.get();
            }

            // update stack
            if (stack.size() == suffix.length) {
                supports.set(supports.size() - 1, supports.get(supports.size() - 1) + support);
            } else {
                for (int i = lcp; i < suffix.length; i++) {
                    stack.add(suffix[i]);
                    if (i == suffix.length - 1) {
                        supports.add(support);
                    } else {
                        supports.add(0);
                    }
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            while (stack.size() > 0) {
                int support = supports.remove(supports.size() - 1);
                if (support >= minsup) {
                    boolean isPrefix = (type == NG.ALL ? false : StringUtils.isPrefix(stack, lastContents));
                    boolean sameCount = (support == lastSupport);
                    boolean emit = true;

                    // skip if maximal n-grams desired
                    if (type == NG.MAXIMAL && isPrefix) {
                        emit = false;
                    }

                    // skip if closed n-grams desired
                    if (type == NG.CLOSED && isPrefix && sameCount) {
                        emit = false;
                    }

                    if (emit) {
                        int[] contents = stack.toIntArray();
                        outKey.setContents(contents);
                        outValue.set(support);
                        context.write(outKey, outValue);

                        lastContents = contents;
                        lastSupport = support;
                    }
                }
                stack.remove(stack.size() - 1);
                if (stack.size() > 0) {
                    supports.set(supports.size() - 1, supports.get(supports.size() - 1) + support);
                }
            }
        }
    }

  
}
