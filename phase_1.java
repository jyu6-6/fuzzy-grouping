import java.io.IOException;
import java.util.*;
import java.nio.ByteBuffer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.io.WritableComparator;


public class phase_1 {
    // the first MR job, to do tokens count, example output: "William 4"
    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, Text> {

        //private final static IntWritable one = new IntWritable(1);
        private final Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
            String line = value.toString();
            String[] parts = line.split(",");
            String words = parts[1];
            //System.out.println(words);
            StringTokenizer itr = new StringTokenizer(words, " ");
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                //System.out.println(word);
                context.write(word, new Text("1"));
            }
        }

    }

    public static class IntSumReducer
            extends Reducer<Text, Text, Text, Text> {
        //private final IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            //System.out.println(key);
            for (Text val : values) {
                Integer count = Integer.valueOf(val.toString());
                sum += count;
            }
            //System.out.println(sum);
            context.write(key, new Text(String.valueOf(sum)));
        }
    }

    // Second MR job starts from here, to sort the result from the first MR job
    public static class IntComparator extends WritableComparator{
        public IntComparator(){
            super(IntWritable.class);
        }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2){
            int v1 = ByteBuffer.wrap(b1, s1, l1).getInt();
            int v2 = ByteBuffer.wrap(b2, s2, l2).getInt();

            return Integer.compare(v1, v2) * (-1);
        }
    }
    // Mapper: InverseMapper
    public static class InvMapper extends
            Mapper<Object,Text, IntWritable, Text>{
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException{
            String line = value.toString();
            //System.out.println(line);
            String[] tokens = line.split("\t");
            //System.out.println(Arrays.toString(tokens));
            String keyPart = tokens[0];
            Integer valuePart = Integer.valueOf(tokens[1]);
            //System.out.println(keyPart);
            context.write(new IntWritable(valuePart), new Text(keyPart));
        }
    }

    public static class SortReducer extends
            Reducer<IntWritable, Text, Text, Text>{
        //Map<String,Integer> map=new HashMap<String, Integer>();
        public void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException{
                for (Text val:values) {
                    //String v = val.toString();
                    //System.out.println(String.valueOf(val));
                    context.write(val, new Text(""));
                }
        }
    }



    public static void main(String[] args) throws Exception {
        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1);
        job1.setJarByClass(phase_1.class);
        job1.setMapperClass(TokenizerMapper.class);
        job1.setCombinerClass(IntSumReducer.class);
        job1.setReducerClass(IntSumReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        Path outputPath = new Path("FirstMapper");
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, outputPath);
        outputPath.getFileSystem(conf1).delete(outputPath, true);
        job1.waitForCompletion(true);

        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2);
        job2.setJarByClass(phase_1.class);
        job2.setMapperClass(InvMapper.class);
        job2.setNumReduceTasks(1);
        job2.setReducerClass(SortReducer.class);
        job2.setMapOutputKeyClass(IntWritable.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        job2.setSortComparatorClass(IntComparator.class);
        Path output = new Path(args[1]);
        FileInputFormat.addInputPath(job2, outputPath);
        FileOutputFormat.setOutputPath(job2, output);
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}

