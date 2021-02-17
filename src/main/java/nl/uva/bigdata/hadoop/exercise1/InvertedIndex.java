package nl.uva.bigdata.hadoop.exercise1;

import nl.uva.bigdata.hadoop.HadoopJob;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.*;
import java.util.regex.Pattern;

public class InvertedIndex extends HadoopJob {

    @Override
    public int run(boolean onCluster, JobConf jobConf, String[] args) throws Exception {

        Map<String,String> parsedArgs = parseArgs(args);

        Path inputPath = new Path(parsedArgs.get("--input"));
        Path outputPath = new Path(parsedArgs.get("--output"));

        Job invertedIndex = prepareJob(onCluster, jobConf,
                inputPath, outputPath, TextInputFormat.class, PageParser.class,
                Text.class, IntWritable.class, IndexEntryBuilder.class, Text.class, NullWritable.class,
                TextOutputFormat.class);
        invertedIndex.waitForCompletion(true);


        return 0;
    }

    public static class PageParser extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable PAGE_ID = new IntWritable();
        private final Text WORD = new Text();

        private static final Pattern SEPARATOR = Pattern.compile("\t");

        public void map(Object key, Text value, Mapper.Context context) throws IOException, InterruptedException {

            String[] tokens = SEPARATOR.split(value.toString());
            int pageId = Integer.parseInt(tokens[0]);
            String pageText = tokens[2].replaceAll(",", "").replaceAll("\\.", "");

            PAGE_ID.set(pageId);

            HashSet<String> alreadySeen = new HashSet<>();

            StringTokenizer tokenizer = new StringTokenizer(pageText);

            while (tokenizer.hasMoreTokens()) {
                String currentWord = tokenizer.nextToken().toLowerCase();

                if (!alreadySeen.contains(currentWord)) {
                    alreadySeen.add(currentWord);
                    WORD.set(currentWord);
                    context.write(WORD, PAGE_ID);
                }
            }
        }
    }

    public static class IndexEntryBuilder extends Reducer<Text,IntWritable,Text,NullWritable> {

        private final Text OUTPUT = new Text();

        public void reduce(Text word, Iterable<IntWritable> pageIds, Context context)
                throws IOException, InterruptedException {

            List<Integer> allPageIds = new ArrayList<>();
            for (IntWritable pageId : pageIds) {
                allPageIds.add(pageId.get());
            }

            int numOccurrences = allPageIds.size();

            for (int pageId : allPageIds) {
                OUTPUT.set(word.toString() + "\t" + pageId + "\t" + numOccurrences);
                context.write(OUTPUT, NullWritable.get());
            }

        }
    }

}
