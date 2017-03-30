import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public class LanguageModel {
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {

		int threashold;

		@Override
		public void setup(Context context) {
			// get the threashold parameter from the configuration?
			Configuration conf = context.getConfiguration();
			threashold = conf.getInt("threshold", 20);

			// hadoop args -> conf
			//Mapper conf.get
			// 20 here is default value
		}

		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// useless string handling
			if((value == null) || (value.toString().trim()).length() == 0) {
				return;
			}

			//this is cool\t20
			// separate word and count
			String line = value.toString().trim();
			String[] wordsPlusCount = line.split("\t");
			if(wordsPlusCount.length < 2) {
				return;
			}
			
			String[] words = wordsPlusCount[0].split("\\s+");
			int count = Integer.valueOf(wordsPlusCount[1]);

			//filter the n-gram lower than threashold
			if(count < threashold){
				return;
			}


			//this is --> cool = 20]
			//extra dependency of last word on the whole gram
			StringBuilder sb = new StringBuilder();
			// concat all til last one
			for(int i=0; i<words.size()-1; i++){
				sb.append(words[i].append(" "));
			}

			//write key-value to reducer?
			String outputKey = sb.toString().trim();
			String outputValue = words[words.length -1];
			if(outputKey != null && outputKey.length() >= 1){
				// output data + count
				context.write(new Text(outputKey), new Text(outputValue + "=" + count));
			}
		}
	}

	public static class Reduce extends Reducer<Text, Text, DBOutputWritable, NullWritable> {

		int n;
		// get the n parameter from the configuration
		@Override
		public void setup(Context context) {
			Configuration conf = context.getConfiguration();
			n = conf.getInt("n", 5);
		}

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			//use priorityQueue to rank topN n-gram, then write out to hdfs
			TreeMap<Integer, List<String>> tm = new TreeMap<Integer, List<String>>(Collections.reverseOrder());
			for(Text val: values) {
				String curValue = val.toString().trim();
				String word = curValue.split("=")[0].trim();
				int count = Integer.parseInt(curValue.split("=")[1].trim());
				if(tm.containsKey(count)) {
					tm.get(count).add(word);
				}
				else {
					List<String> list = new ArrayList<String>();
					list.add(word);
					tm.put(count, list);
				}
			}
			// get topK
			Iterator<Integer> iter = tm.keySet().iterator();
			for(int j=0; iter.hasNext() && j<n; j++) {
				int keyCount = iter.next();
				List<String> words = tm.get(keyCount);
				// write to db
				for(String curWord: words) {
					context.write(new DBOutputWritable(key.toString(), curWord, keyCount),NullWritable.get());
					j++;
					if(j >=n)break;
				}
			}




		}
	}
}
