package nl.uva.bigdata.hadoop.assignment1;

import com.google.common.collect.Maps;
import nl.uva.bigdata.hadoop.HadoopClusterTestCase;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.Map;

public class FilteringWordCountClusterTest extends HadoopClusterTestCase {

    public void test() throws Exception {

        Path inputFile = path("lotr.txt");
        Path outputDir = path("outputs/");

        writeLines(inputFile,
                "One Ring to rule them all,",
                "One Ring to find them,",
                "One Ring to bring them all,",
                "and in the darkness bind them");

        FilteringWordCount wordCount = new FilteringWordCount();

        wordCount.runOnCluster(createJobConf(),
                new String[] { "--input", inputFile.toString(), "--output", outputDir.toString() });

        Map<String, Integer> counts = getCounts(new Path(outputDir, "part-r-00000"));

        assertEquals(new Integer(3), counts.get("ring"));
        assertEquals(new Integer(2), counts.get("all"));
        assertEquals(new Integer(1), counts.get("darkness"));
        assertFalse(counts.containsKey("the"));
        assertFalse(counts.containsKey("to"));
    }

    protected Map<String,Integer> getCounts(Path outputFile) throws IOException {
        Map<String,Integer> counts = Maps.newHashMap();
        for (String line: readLines(outputFile)) {
            String[] tokens = line.split("\t");
            counts.put(tokens[0], Integer.parseInt(tokens[1]));
        }
        return counts;
    }
}

