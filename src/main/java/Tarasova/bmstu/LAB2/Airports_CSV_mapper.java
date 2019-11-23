package Tarasova.bmstu.LAB2;
import javafx.util.Pair;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class Airports_CSV_mapper extends Mapper<LongWritable, Text, Shared_key, Text>{
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
        Airports_CSV_writable CSV_writable = new Airports_CSV_writable(value.toString());
        Pair<String, String> CSV_pair = CSV_writable.getAirCSVPair();
        try{
            context.write(new Shared_key(CSV_pair.getKey(), 0), new Text(CSV_pair.getValue()));
        }catch (NullPointerException err){
            System.out.println(err);
        }
    }
}