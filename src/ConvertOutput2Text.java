
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Converts n-gram statistics in our input-sequence format into plain text.
 *
 * @author Klaus Berberich (kberberi@mpi-inf.mpg.de)
 */
public class ConvertOutput2Text  {

    public static final class Map extends Mapper<IntArrayWritable, IntWritable, Text, IntWritable> {

        // dictionary mapping term identifiers to terms
        private final Int2ObjectOpenHashMap<String> dict = new Int2ObjectOpenHashMap<String>();

        // singleton output key -- for efficiency reasons
        private final Text outKey = new Text();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
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
                        String term = tokens[0];
                        int tid = Integer.parseInt(tokens[1]);
                        dict.put(tid, term);
                    }
                    br.close();
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        protected void map(IntArrayWritable key, IntWritable value, Context context) throws IOException, InterruptedException {
            int[] contents = key.getContents();
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < contents.length; i++) {
                sb.append((i > 0 ? " " : "") + dict.get(contents[i]));
            }
            outKey.set(sb.toString());
            context.write(outKey, value);
        }
    }

}