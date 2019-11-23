package Tarasova.bmstu.LAB2;
import java.util.Iterator;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Flight_time_arrival_reducer extends Reducer<Shared_key, Text, Text, Text> {
    @Override
    protected void reduce(Shared_key key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
        int counter = 0;
        //Минимальная задержка, максимальная задержка, задержка в данный момент и общая сумма задержек
        Double min, max, current, sum = 0.0;
        //Задаем максимальное значение для min и минимальное значение для max
        min = Double.MAX_VALUE;
        max = Double.MIN_VALUE;
        Iterator iterator = values.iterator();
        String airportName = iterator.next().toString();

        while(iterator.hasNext()){
            String token = iterator.next().toString();
            // Выходим, когда конец
            if (token.length() == 0){
                continue;
            }
            current = new Double(token);//Задержка на данный момент
            // Если нет задержки, идем дальше
            if (current == 0.0) {
                continue;
            }
            // Если задержка больше максимального значения или это первый заход, то заменяем максимальное значение
            if(counter == 0 || max < current){
                max = current;
            }
            // Если задержка меньше минимального значения или это первый заход, то заменяем минимальное значение
            if(counter == 0 || min > current){
                min = current;
            }
            sum += current;
            counter += 1;
        }
        //Если была хоть какая-то задержка, то выводим
        if(counter > 0){
            Double average = new Double(Math.round(100.0 * (sum/counter)) / 100.0);
            String infoString = "".concat("[Min: ".concat(min.toString().concat(", ")));
            infoString = infoString.concat("Max: ").concat(max.toString()).concat(", ");
            infoString = infoString.concat("Average: ").concat(average.toString().concat("];"));
            context.write(new Text(airportName), new Text(infoString));
        }
    }
}