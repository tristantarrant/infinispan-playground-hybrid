package net.dataforte.infinispan.playground.hybrid;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.Buffer;
import java.nio.CharBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import net.dataforte.infinispan.playground.hybrid.mapreduce.WordCountCollator;
import net.dataforte.infinispan.playground.hybrid.mapreduce.WordCountMapper;
import net.dataforte.infinispan.playground.hybrid.mapreduce.WordCountReducer;

import org.infinispan.Cache;
import org.infinispan.commons.util.Util;
import org.infinispan.distexec.mapreduce.MapReduceTask;
import org.infinispan.remoting.transport.Transport;

import com.martiansoftware.jsap.FlaggedOption;
import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.SimpleJSAP;

/**
 * Infinispan MapReduceTask demo
 *
 * @author Vladimir Blagojevic
 */
public class HybridWordCountDemo {
   private final String cfgFile;
   private final JSAPResult commandLineOptions;

   private String textFile;
   private int numPopularWords;

   public static void main(String... args) throws Exception {
      new HybridWordCountDemo(args).run();
   }

   HybridWordCountDemo(String[] args) throws Exception {
      commandLineOptions = parseParameters(args);
      cfgFile = commandLineOptions.getString("configFile");
      textFile = commandLineOptions.getString("textFile");
      numPopularWords = commandLineOptions.getInt("mostPopularWords");
   }

   private JSAPResult parseParameters(String[] args) throws Exception {
      SimpleJSAP jsap = buildCommandLineOptions();

      JSAPResult config = jsap.parse(args);
      if (!config.success() || jsap.messagePrinted()) {
         Iterator<?> messageIterator = config.getErrorMessageIterator();
         while (messageIterator.hasNext())
            System.err.println(messageIterator.next());
         System.err.println(jsap.getHelp());
         return null;
      }

      return config;
   }

   private Cache<String, String> startCache() throws IOException {
      HybridCluster cluster = new HybridCluster(cfgFile);

      Cache<String, String> cache = cluster.getCache();

      Transport transport = cache.getAdvancedCache().getRpcManager().getTransport();
      System.out.printf("Node %s joined as master. View is %s.%n", transport.getAddress(), transport.getMembers());

      return cache;
   }

   private void run() throws Exception {

      // Step 1: start cache.
      Cache<String, String> cache = startCache();

      // Step 2: load up data file
      if (textFile != null)
         loadData(cache);

      // Step 3: if slave, wait.  Else, start Map/Reduce task.
      try {

         long start = System.currentTimeMillis();
         MapReduceTask<String, String, String, Integer> mapReduceTask = new MapReduceTask<String, String, String, Integer>(cache);

         List<Entry<String, Integer>> topList = mapReduceTask.mappedWith(new WordCountMapper())
               .reducedWith(new WordCountReducer()).execute(new WordCountCollator(numPopularWords));

         System.out.printf(" ---- RESULTS: Top %s words in %s ---- %n%n", numPopularWords, textFile);
         int z = 0;
         for (Entry<String, Integer> e : topList)
            System.out.printf("  %s) %s [ %,d occurences ]%n", ++z, e.getKey(), e.getValue());
         System.out.printf("%nCompleted in %s%n%n", Util.prettyPrintTime(System.currentTimeMillis() - start));
      } finally {
         cache.getCacheManager().stop();
      }
   }

   private void loadData(Cache<String, String> cache) throws IOException {
      FileReader in = new FileReader(textFile);
      try (BufferedReader bufferedReader = new BufferedReader(in)) {

         //chunk and insert into cache
         int chunkSize = 10; // 10K
         int chunkId = 0;

         CharBuffer cbuf = CharBuffer.allocate(1024 * chunkSize);
         while (bufferedReader.read(cbuf) >= 0) {
            Buffer buffer = cbuf.flip();
            String textChunk = buffer.toString();
            cache.put(textFile + (chunkId++), textChunk);
            cbuf.clear();
            if (chunkId % 100 == 0)
               System.out.printf("  Inserted %s chunks from %s into grid%n", chunkId, textFile);
         }
      } finally {
         Util.close(in);
      }
   }

   private SimpleJSAP buildCommandLineOptions() throws JSAPException {
      return new SimpleJSAP("WordCountDemo", "Count words in Infinispan cache usin MapReduceTask ", new Parameter[] {
            new FlaggedOption("configFile", JSAP.STRING_PARSER, HybridCluster.JGROUPS_CONFIGURATION_FILE,
                  JSAP.NOT_REQUIRED, 'c', "configFile", "Infinispan transport config file"),
            new FlaggedOption("textFile", JSAP.STRING_PARSER, null, JSAP.REQUIRED, 'f', "textFile",
                  "Input text file to distribute onto grid"),
            new FlaggedOption("mostPopularWords", JSAP.INTEGER_PARSER, "100", JSAP.NOT_REQUIRED, 'n',
                  "mostPopularWords", "Number of most popular words to find") });
   }
}