import java.io.IOException;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class phase3 {
    //public static Map<String,String> map=new HashMap<String, String>();
    public static class mapper1 extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String record = value.toString();
            String[] parts = record.split(",");
            //map.put(parts[0],parts[1]);
            context.write(new Text(parts[0]), new Text("Original" + "," + parts[1]));
        }

    }

    public static class mapper2 extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String record = value.toString();
            String[] parts = record.split(",");
            String[] parts1 = parts[1].split("\t");
            String values = parts[2] + "," + parts[3]+","+parts[4];
            context.write(new Text(parts[0]), new Text("Phase2" + "," + values));
            //System.out.println(parts[0]);
            //System.out.println(values);
            context.write(new Text(parts1[0]), new Text("Phase2" + "," + values));
            //System.out.println(parts1[0]);
        }

    }

    public static class Reducer1 extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context
        ) throws IOException, InterruptedException {
            String v = "";
            String k = "";
            Set<String> set = new TreeSet<String>();
            for (Text t : values) {
                String[] parts = t.toString().split(",");
                if (parts[0].equals("Original")) {
                    v = parts[1];
                } else if (parts[0].equals("Phase2")) {
                    k = parts[1] + "," + parts[2]+","+parts[3];
                    set.add(k);
                }
            }
            for(String group_key:set){
                context.write(new Text(group_key),new Text(v));
        }
    }}
    public static class identicalmap extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            //System.out.println(value.toString());
            String[] parts = value.toString().split("\t");
            context.write(new Text(parts[0]),new Text(parts[1]));
        }

    }

    public static class Reducer2 extends Reducer<Text,Text,Text,Text > {
        private int correct_count = 0;
        private int all_count = 0;
        public void reduce(Text key, Iterable<Text> values, Context context
        ) throws IOException, InterruptedException
        {
            Set<String> setX = new TreeSet<String>();
            for (Text t :values) {
                String parts = t.toString();
                setX.add(parts);
            }

            //Evaluation
            if(setX.size()==4){
                for(String X:setX ){
                    String[] eval_list = X.split(" ");
                    if (eval_list.length==3){
                        if (eval_list[1].length()==2){
                            correct_count++;
                        }
                    }
                }
                System.out.println("Correct"+":"+correct_count);
            }
            all_count++;
            System.out.println("ALL"+":"+all_count);
            context.write(new Text(String.valueOf(all_count)),new Text(String.valueOf(setX)));
        }

    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        Job job = new Job(conf, "phase3_1");
        job.setJarByClass(phase3.class);
        job.setReducerClass(Reducer1.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        Path input = new Path(args[0]);//original record
        Path output2 = new Path(args[1]); //phase2_output
        MultipleInputs.addInputPath(job,input, TextInputFormat.class,mapper1.class);
        MultipleInputs.addInputPath(job,output2,TextInputFormat.class,mapper2.class);
        Path output3 = new Path(args[2]);
        FileOutputFormat.setOutputPath(job,output3);
        job.waitForCompletion(true);

        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2);
        job2.setJarByClass(phase3.class);
        job2.setMapperClass(identicalmap.class);
        job2.setReducerClass(Reducer2.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job2, output3);
        Path output4 = new Path(args[3]);
        FileOutputFormat.setOutputPath(job2, output4);
        System.exit(job2.waitForCompletion(true) ? 0 : 1);

    }
}

