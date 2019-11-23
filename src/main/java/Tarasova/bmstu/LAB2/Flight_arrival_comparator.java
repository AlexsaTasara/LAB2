package Tarasova.bmstu.LAB2;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class Flight_arrival_comparator extends WritableComparator {
    // Проверяем код аэропорта
    public Flight_arrival_comparator(){super(Shared_key.class, true);}
    @Override
    public int compare(WritableComparable key1, WritableComparable key2){ //Сравнение двух ключей
        Shared_key shared_key1 = (Shared_key) key1;
        Shared_key shared_key2 = (Shared_key) key2;
        return shared_key2.compareToFirstPart(shared_key1);
    }
}