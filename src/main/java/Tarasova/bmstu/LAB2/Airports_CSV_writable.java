package Tarasova.bmstu.LAB2;
import javafx.util.Pair;

public class Airports_CSV_writable{ 
    private Pair<String, String> airportCSVPair;
    public Airports_CSV_writable(String airportCSV){
        int comma;  //Индекс запятой
        String  airportsName, airportID;
        if(!airportCSV.contains("Description")){  //Если есть описание тогда
            comma = airportCSV.indexOf(",");  //Находим индекс запятой
            airportID = airportCSV.substring(1, comma - 1); //Выделяем в отдельную строку ID аэропорта не включая запятую
            airportsName = airportCSV.substring(comma + 2, airportCSV.length() - 1); //Выделяем в отдельную строку имя без запятой
            if(!airportID.isEmpty() && !airportsName.isEmpty()){ //Если имя и ID не пусты, создаем новую пару.
                airportCSVPair = new Pair<>(airportID, airportsName);
            }
        }
    }
    public Pair<String, String> getAirCSVPair(){return airportCSVPair;} //Функция выдачи пары.
}