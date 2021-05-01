import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;

public class phase2_simple{

    //load the frequency file
    public static ArrayList<String> loadfile(String pathname) throws IOException {
        ArrayList<String> keyWord = new ArrayList<String>();
        File filename = new File(pathname);
        InputStreamReader reader = new InputStreamReader( new FileInputStream(filename));
        BufferedReader br = new BufferedReader(reader);
        String line = "";
        while ((line=br.readLine())!=null) {
            String words = String.valueOf(line);
            keyWord.add(words);
        }
        br.close();
        return keyWord;
    }

    //get signature for grouping
    public static class mapper extends Mapper<Object, Text, Text, Text> {
        private ArrayList<String> words = new ArrayList<String>();

        public void setup(Context context) throws IOException,InterruptedException{
            Configuration conf = context.getConfiguration();
            String order = conf.get("global").replace("[","").replace("]","");
            String[] orders = order.split(",");
            for(String o:orders){
                String a = o.replaceAll(" ", "");
                String e = a.replaceAll("\t","");
                //System.out.println(e);
                //System.out.println(e.length());
                words.add(e);
            }
            //System.out.println(words);
        }
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            ArrayList<String> keyWord = new ArrayList<String>();
            String record = value.toString();
            String[] parts = record.split(",");
            String[] tokens = parts[1].split((" "));
            List<String> tempList = new ArrayList<String>(Arrays.asList(tokens));
            //System.out.println(tempList);
            if (tempList.size() == 3) {
                tempList.remove(1);
            }
            //Collections.sort(tempList);
            //context.write(new Text(tempList.get(0)+","+tempList.get(1)),new Text(record));
            for (String word : words) {
                if (tempList.contains(word)) {
                    keyWord.add(word);
                }
            }
            if(keyWord.size()>1){
            //System.out.println(keyWord);
            context.write(new Text(keyWord.get(0) + "," + keyWord.get(1)), new Text(record));
            }
        }
    }

    //group by signature, filter similarity
    public static class reducer extends
            Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            ArrayList<String> Words = new ArrayList<String>();
            ArrayList<String> IDs = new ArrayList<String>();
            if(!(String.valueOf(key).length() <3)){

            }
            for (Text val : values) {
                String[] parts = val.toString().split(",");
                if(!Words.contains(parts[1])){
                    IDs.add(parts[0]);
                    Words.add(parts[1]);
                }}
            //context.write(key,new Text(String.valueOf(Words)));
            //System.out.println(Words.size());
            for(int i=0;i<=IDs.size()-2;i++){
                for(int j =i+1;j<=IDs.size()-1;j++){
                    String name_1 = Words.get(i);
                    String name_2 = Words.get(j);
                    if (name_1 == name_2){
                        continue;
                    }
                    Float sim = filterSimilarity(name_1,name_2);
                    if(sim>=0.5){
                        String K = IDs.get(i)+","+ IDs.get(j);
                        String[] name1_list = name_1.split(" ");
                        String[] name2_list = name_2.split(" ");
                        if (name1_list.length==3){
                            if(name1_list[1].length()>2){
                                String V = String.valueOf(sim)+","+key.toString()+","+name1_list[1];
                                //System.out.println(V);
                                context.write(new Text(K),new Text(V));
                                continue;
                                }
                            }
                        if (name2_list.length==3){
                            if(name2_list[1].length()>2){
                                String V = String.valueOf(sim)+","+key.toString()+","+name2_list[1];
                                context.write(new Text(K),new Text(V));
                            }
                        }
                    }
                }
            }
        }
    }
    //calculate similarity(modified jaccard similarity)
    public static float filterSimilarity(String stringX, String stringY) {
        String[] x_list = stringX.split(" ");
        String[] y_list = stringY.split(" ");
        if (x_list.length ==3 &&y_list.length==3){
            String x = x_list[1].substring(0,1);
            //System.out.println(x.length());
            String y = y_list[1].substring(0,1);
            //System.out.println(y.length());
            if(!x.equals(y)){
                return 0.0f;
            }
        }
        String old_x = stringX.replace(" ","");
        int x_length = old_x.length();
        //System.out.println(x_length);
        String old_y = stringY.replace(" ","");
        int y_length = old_y.length();
        //System.out.println(y_length);
        Set<String> setX = new TreeSet<String>();
        StringTokenizer itr = new StringTokenizer(stringX, " ");
        while (itr.hasMoreTokens()) {
            setX.add(itr.nextToken());
        }
        Set<String> setY = new TreeSet<String>();
        StringTokenizer itr_y = new StringTokenizer(stringY, " ");
        while (itr_y.hasMoreTokens()) {
            setY.add(itr_y.nextToken());
        }
        setX.retainAll(setY);
        //System.out.println(setX);
        int union_size = 0;
        for(String x:setX){
            union_size+=x.length();
        }
        //System.out.println(union_size);
        return ((float) union_size) / (x_length + y_length - union_size);
    }
    //calculate similarity(jaccard similarity)
//    public static float filterSimilarity(String stringX, String stringY) {
//
//        Set<String> setX = new TreeSet<String>();
//        StringTokenizer itr = new StringTokenizer(stringX, " ");
//        while (itr.hasMoreTokens()) {
//            setX.add(itr.nextToken());
//        }
//
//        Set<String> setY = new TreeSet<String>();
//        StringTokenizer itr_y = new StringTokenizer(stringY, " ");
//        while (itr_y.hasMoreTokens()) {
//            setY.add(itr_y.nextToken());
//        }
//        int lengthX = setX.size();
//        int lengthY = setY.size();
//        setX.retainAll(setY);
//        System.out.println(setX);
//
//        return ((float) setX.size()) / (lengthX + lengthY - setX.size());
//    }

    public static void main(String[] args) throws Exception {
        ArrayList<String> keyWord= loadfile("./output/part-r-00000"); //phase1_output
        //System.out.println(keyWord);
        Configuration conf = new Configuration();
        conf.set("global", String.valueOf(keyWord));
        Job job = new Job(conf, "FilterWordCount");
        job.setJarByClass(phase2_simple.class);
        job.setMapperClass(mapper.class);
        //job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        Path input = new Path(args[0]);
        Path output2 = new Path(args[1]);
        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output2);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
