package Tarasova.bmstu.LAB2;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class Flight_arrival_partitioner extends Partitioner<Shared_key, Text> {
    public Flight_arrival_partitioner(){
    }
    public int getPartition(Shared_key key, Text value, int numReduceTasks){
        int airportID = new Integer(key.getAirportID()); //Получаем ID аэропорта
        return airportID % numReduceTasks;  //Возвраащаем
    }
}

