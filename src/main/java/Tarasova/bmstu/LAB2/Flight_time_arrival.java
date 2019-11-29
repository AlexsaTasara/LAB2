package Tarasova.bmstu.LAB2;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Flight_time_arrival {
    //Подключаем документы
    private final static String FLIGHTS_CSV_PATH = "664600583_T_ONTIME_sample.csv";
    private final static String AIRPORT_CSV_PATH = "L_AIRPORT_ID.csv";

    public static void main(String[] args) throws Exception {
        if (args.length != 1){
            System.err.println("Problem with the time arrival of planes\nUsage: <output path>");
            System.exit(-1);
        }
        Job work = Job.getInstance();
        work.setJarByClass(Flight_time_arrival.class); //Определяем класс для работы
        work.setJobName("Reduce side join"); //Определяем имя для работы
        //Добавляем пути к документам
        MultipleInputs.addInputPath(work, new Path(FLIGHTS_CSV_PATH), TextInputFormat.class, Flights_CSV_mapper.class);
        MultipleInputs.addInputPath(work, new Path(AIRPORT_CSV_PATH), TextInputFormat.class, Airports_CSV_mapper.class);
        FileOutputFormat.setOutputPath(work, new Path(args[0]));

        work.setReducerClass(Flight_time_arrival_reducer.class);
        work.setGroupingComparatorClass(Flight_arrival_comparator.class);
        work.setPartitionerClass(Flight_arrival_partitioner.class);
        work.setMapOutputKeyClass(Shared_key.class);

        work.setOutputKeyClass(Text.class);
        work.setOutputValueClass(Text.class);
        work.setNumReduceTasks(2);
        System.exit(work.waitForCompletion(true) ? 0 : 1);
    }
}

