package nl.uva.bigdata.hadoop.exercise1;

import nl.uva.bigdata.hadoop.HadoopJob;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

public class BookAndAuthorBroadcastJoin extends HadoopJob {

  @Override
  public int run(boolean onCluster, JobConf jobConf, String[] args) throws Exception {

    Map<String,String> parsedArgs = parseArgs(args);

    Path authors = new Path(parsedArgs.get("--authors"));
    Path books = new Path(parsedArgs.get("--books"));
    Path outputPath = new Path(parsedArgs.get("--output"));

    Job wordCount = prepareJob(onCluster, jobConf,
            books, outputPath, TextInputFormat.class, MapSideJoinMapper.class,
            Text.class, NullWritable.class, TextOutputFormat.class);

    wordCount.addCacheFile(authors.toUri());
    wordCount.waitForCompletion(true);

    return 0;
  }

  public static class MapSideJoinMapper extends Mapper<Object, Text, Text, IntWritable> {

    private static final Pattern SEPARATOR = Pattern.compile("\t");
    private static final Map<Integer, String> AUTHORS = new HashMap<>();
    private static final Text OUTPUT = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      URI authorsFile = context.getCacheFiles()[0];
      FileSystem fs = FileSystem.get(authorsFile, context.getConfiguration());

      try (BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(new Path(authorsFile))))) {
        String line = reader.readLine();
        while (line != null) {
          String[] tokens = SEPARATOR.split(line);
          Integer authorId = Integer.parseInt(tokens[0]);
          String authorName = tokens[1];
          AUTHORS.put(authorId, authorName);

          line = reader.readLine();
        }
      }
    }

    public void map(Object key, Text value, Mapper.Context context) throws IOException, InterruptedException {

      String[] tokens = SEPARATOR.split(value.toString());
      int authorId = Integer.parseInt(tokens[0]);
      int year = Integer.parseInt(tokens[1]);
      String bookName = tokens[2];

      if (AUTHORS.containsKey(authorId)) {
        OUTPUT.set(AUTHORS.get(authorId) + "\t" + bookName + "\t" + Integer.toString(year));
        context.write(OUTPUT, NullWritable.get());
      }
    }

  }

}