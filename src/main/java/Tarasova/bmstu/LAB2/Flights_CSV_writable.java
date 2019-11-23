package Tarasova.bmstu.LAB2;
import javafx.util.Pair;

public class Flights_CSV_writable {
    private static final int TOTAL = 18;
    private static final int FLIGHT_DELAY_INDEX = 17;
    private static final int DEST_AIRPORT_ID_INDEX = 14;
    private Pair<String, String> csv_flight_pair;

    public Flights_CSV_writable(String flight_CSV){
        String id, delay;// Ключ и значение
        System.out.println(flight_CSV);
        //избавляемся от первой колонки
        if(flight_CSV.length() > 1 && !flight_CSV.contains("YEAR")) {
            String[] table = flight_CSV.split(",");
            //Если таблица меньше, то выходим
            if(table.length < TOTAL){
                return;
            }
            id = table[DEST_AIRPORT_ID_INDEX];
            delay = table[FLIGHT_DELAY_INDEX];
            //Если не пусты то создаем новую пару
            if(!id.isEmpty() && !delay.isEmpty()) {
                this.csv_flight_pair = new Pair<>(id, delay);
            }
        }
    }
    public Pair<String, String> getFlightsCSVPair(){
        return csv_flight_pair;
    } //Вызываем пару
    public String getDestAirportID(){
        return csv_flight_pair.getKey();
    } //Вызываем ID
    public String getFlightDelay(){
        return csv_flight_pair.getValue();
    } //Вызываем задержку
}