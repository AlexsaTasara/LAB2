package Tarasova.bmstu.LAB2;
import javafx.util.Pair;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class Flights_CSV_mapper extends Mapper<LongWritable, Text, Shared_key, Text> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Flights_CSV_writable flights_CSV_writable = new Flights_CSV_writable(value.toString());
        Pair<String, String> flights_CSV_pair = flights_CSV_writable.getFlightsCSVPair();
        try {
            context.write(new Shared_key(flights_CSV_pair.getKey(), 1), new Text(flights_CSV_pair.getValue()));
        }catch (NullPointerException err){
            System.out.println(err);
        }
    }
}