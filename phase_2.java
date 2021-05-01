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

public class phase_2{

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
    //filter similarity
    public static float filterSimilarity(String stringX, String stringY) {
        String[] x_list = stringX.split(" ");
        String[] y_list = stringY.split(" ");
        if (x_list.length ==3 &&y_list.length==3){
            String x = x_list[1].substring(0,1);
            System.out.println(x);
            String y = y_list[1].substring(0,1);
            System.out.println(y);
            if(!x.equals(y)){
                return 0.0f;
            }
        }
        System.out.println(x_list[1]);
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
        int lengthX = setX.size();
        int lengthY = setY.size();
        setX.retainAll(setY);
        //System.out.println(setX);

        return ((float) setX.size()) / (lengthX + lengthY - setX.size());
    }

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
            //ArrayList<String> keyWord = new ArrayList<String>();
            String record = value.toString();
            String[] parts = record.split(",");
            String[] tokens = parts[1].split((" "));
            List<String> tempList = Arrays.asList(tokens);
            int count =0;
            for(String o:words){
                if(tempList.contains(o)){
                    count++;
                    if (o.length()>2){
                        context.write(new Text(o),new Text(record));
                    }
                    //prefix =2
                    if(count==2){
                        break;
//                    keyWord.add(o);
//                    //prefix =2
//                    if(keyWord.size()==2){
//                        context.write(new Text(keyWord.get(0)),new Text(record));
//                        context.write(new Text(keyWord.get(1)),new Text(record));
//                        break;
                    }
                }
            }
        }
    }

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
                    if (Words.get(i) == Words.get(j)){
                        continue;
                    }
                    Float sim = filterSimilarity(Words.get(i),Words.get(j));
                    if(sim>=0.5){
                        String K = IDs.get(i)+","+IDs.get(j);
                        String V = String.valueOf(sim)+","+ key.toString();
                        context.write(new Text(K),new Text(V));
                    }
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        ArrayList<String> keyWord= loadfile("./output/part-r-00000");
        //System.out.println(keyWord);
        Configuration conf = new Configuration();
        conf.set("global", String.valueOf(keyWord));
        Job job = new Job(conf, "phase_2");
        job.setJarByClass(phase_2.class);
        job.setMapperClass(mapper.class);
        job.setReducerClass(reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}