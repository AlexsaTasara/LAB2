package Tarasova.bmstu.LAB2;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;

//Ключ для reduce side join (0 - аэропорт, 1 - перелет  )
public class Shared_key implements WritableComparable<Shared_key>  {
    private String airport_ID;
    private int flag;
    
    // Определяем ключ в начале
    public Shared_key(){
        this.airport_ID = "null";
        this.flag = -1;
    }
    //Функция заполнения ключа
    public Shared_key(String airport_ID, int flag){
        this.airport_ID = airport_ID;
        this.flag = flag;
    }
    //Функция вывода ключа
    public void write(DataOutput dataOutput) throws IOException{
        dataOutput.writeChars(airport_ID);
        dataOutput.writeInt(flag);
    }
    //Функция чтения ключа
    public void readFields(DataInput dataInput) throws IOException{
        String Line = dataInput.readLine();
        int sizeLine = Line.length();
        //Флаг
        flag = Line.charAt(sizeLine - 1);
        //ID
        airport_ID = Line.substring(0, sizeLine - 1);
    }
    //Функция получения ключа
    public String getAirportID(){
        return airport_ID;
    }
    //Функция получения флага
    public int getFlag(){
        return flag;
    }
    //Функция частичного сравнения
    public int compareToFirstPart(Shared_key other_key){
        return airport_ID.compareTo(other_key.getAirportID());
    }
    //Функция сравнения ключей
    public int compareTo(Shared_key other_key){
        //Сравнение по строкам ID
        int airport_ID_compare = airport_ID.compareTo(other_key.getAirportID());
        int otherFlag = other_key.getFlag();
        if (airport_ID_compare == 0){
            return flag - otherFlag;
        }
        return airport_ID_compare;
    }
}