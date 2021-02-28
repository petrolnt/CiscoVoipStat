/*
 * Класс который будет слушать udp-порт rsyslog(514), принимать логи от cisco-voip шлюза
 * 
 */
package ciscovoipstat;

import com.mysql.jdbc.Connection;
import com.mysql.jdbc.PreparedStatement;
import java.io.*;
import java.net.*;
import java.sql.Timestamp;
import java.sql.Date;
import java.sql.DriverManager;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 * @author petr
 */
public class CiscoVoipStat {

    Connection connection;
    com.mysql.jdbc.PreparedStatement stmt;
    com.mysql.jdbc.PreparedStatement updStmt;
    //порт по умолчанию
    int udpPort = 514;
    String localIPAddress;
    String driverName = "com.mysql.jdbc.Driver";
    //строка запроса для занесения записи о звонке
    String firstQuery = "INSERT INTO CDRTable (receivedAt, connectionID, setupTime, localPeerAddress,"
            + " localPeerSubAddress, disconnectCause, connectTime, disconnectTime, callOrigin"
            + ", chargedUnits, infoType, transmitPackets, transmitBytes,"
            + "receivePackets, receiveBytes) values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    //строка запроса для занесения информации об удаленном номере телефона, в уже созданный кортеж
    String seccondQuery = "UPDATE CDRTable SET remotePeerAddress = ?, remotePeerSubAddress = ?"
            + " WHERE connectionID = ? AND receivedAt = ? AND setupTime BETWEEN ? AND ?";
    SimpleDateFormat timeFormat;
    //паттерн для парсинга времени
    Pattern datePattern = Pattern.compile("\\d{2}:\\d{2}:\\d{2}.\\d{3} \\w{4} \\w{3} \\w{3} \\d{1,2} \\d{4}");
    //паттерн для разбора сообщения на ключ - значение
    Pattern keyPattern = Pattern.compile("(.\\s?)(.*?)\\s");

    public CiscoVoipStat(Hashtable<String, String> params) {

        timeFormat = new SimpleDateFormat("hh:mm:ss.SSS z EEE MMM dd yyyy", Locale.US);
        try {

            Class.forName(driverName);

            String serverName = params.get("-dbs");
            String db = params.get("-dbn");
            String url = "jdbc:mysql://" + serverName + "/" + db;
            String user = params.get("-dbu");
            String password = params.get("-pass");
            if (params.containsKey("-i")) {
                localIPAddress = params.get("-i");
            }
            if (params.containsKey("-p")) {
                udpPort = Integer.parseInt(params.get("-p"));
            }


            connection = (Connection) DriverManager.getConnection(url, user, password);
            stmt = (com.mysql.jdbc.PreparedStatement) connection.prepareStatement(firstQuery);
            updStmt = (PreparedStatement) connection.prepareStatement(seccondQuery);

        } catch (Exception exp) {
            exp.printStackTrace();
        }
    }

    //Слушаем порт указанный в параметрах при запуске, если порт не был указан то по умолчанию порт 514
    private void listen() {
        byte[] receiveData = new byte[65536];
        DatagramSocket serverSocket;
        try {
            if (localIPAddress == null) {
                serverSocket = new DatagramSocket(udpPort);
            } else {
                InetAddress inetAddress = InetAddress.getByName(localIPAddress);
                serverSocket = new DatagramSocket(udpPort, inetAddress);
            }
            //полученные пакеты передаем и адрес источника передаем дальше для парсинга
            while (true) {
                DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
                serverSocket.receive(receivePacket);
                InetAddress IPAddress = receivePacket.getAddress();
                String ipAddress = IPAddress.toString().replace("/", "");
                String sentence = new String(receivePacket.getData());
                parseMessage(sentence, ipAddress);

            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    //метод парсящий сообщение и передающий его дальше для записи в БД
    private void parseMessage(String sentence, String srcAddr) {
        try {

            //если пакет Cisco Call History то начинаем парсинг
            if ((sentence.indexOf("CALL_HISTORY")) > -1) {
                //определяем расположение в строке параметра CallLegTyoe
                int cType = sentence.indexOf("CallLegType");
                //получаем значение параметра CallLegType
                String callLegType = sentence.substring((cType + 12), (cType + 13));
                //обрезаем остальную строку и заносим ее в массив в виде: "Параметр: Значение"
                String callHistory = sentence.substring(cType);
                String delimiter = "[,]";
                String[] fields = callHistory.split(delimiter);
                //первая запись о звонке сформированная нашей cisco, содержит все необходимые поля, кроме внешнего номера
                //с которого/на который был выполнен звонок
                if (callLegType.equals("2")) {
                    //создаем массив под значения
                    String[] values = new String[(fields.length)];
                    String value = "";
                    int j = 0;
                    //перебираем массив fields и выбираем из него все значения параметров
                    for (int i = 0; i < fields.length; i++) {

                        if (((fields[i].indexOf("DisconnectText")) < 0) && ((fields[i].indexOf("CallLegType")) < 0)) {
                            //находим в строке название параметра и обрезаем пробел
                            Matcher keyMatcher = keyPattern.matcher(fields[i]);
                            keyMatcher.find();
                            String key = keyMatcher.group(0).trim();
                            //если это время то находим значение по паттерну и присваиваем переменной value
                            if (key.indexOf("Time") > -1) {
                                Matcher valueMatcher = datePattern.matcher(fields[i]);
                                valueMatcher.find();
                                value = valueMatcher.group(0);
                                //если это не мремя то просто обрезаем значение ключа,
                                //убираем пробелы и также присваиваем полученное значение переменной value
                            } else {

                                value = (fields[i].substring(key.length() + 1)).trim();
                            }
                            //заносим переменную в массив
                            values[j] = value;
                            j++;
                        }

                    }
                    //добавляем в массив адрес источника сообщения
                    values[j] = srcAddr;
                    insertToMysql(values);
                    //вторая запись содержит необходимый нам внешний номер телефона
                } else if (callLegType.equals("1")) {
                    String peerAddress = "";
                    String peerSubAddress = "";
                    //следующие две переменных нужны для сопоставления записей в базе данных
                    String connectionID = "";
                    String setupTime = "";
                    //получаем значения и передаем дальше для занесения в базу
                    Matcher keyMatcher = keyPattern.matcher(fields[3]);
                    if (keyMatcher.find()) {
                        String key = keyMatcher.group(0).trim();
                        peerAddress = (fields[3].substring(key.length() + 1)).trim();
                    }

                    keyMatcher = keyPattern.matcher(fields[4]);
                    if (keyMatcher.find()) {
                        String key = keyMatcher.group(0).trim();
                        peerSubAddress = (fields[4].substring(key.length() + 1)).trim();
                    }

                    keyMatcher = keyPattern.matcher(fields[1]);
                    if (keyMatcher.find()) {
                        String key = keyMatcher.group(0).trim();
                        connectionID = (fields[1].substring(key.length() + 1)).trim();
                    }

                    Matcher valueMatcher = datePattern.matcher(fields[2]);
                    if (valueMatcher.find()) {
                        setupTime = valueMatcher.group(0);
                    }
                    if (setupTime.equals("")) {
                        System.out.println(sentence);
                        System.out.println(connectionID);
                    }
                    insertToMysql(connectionID, peerAddress, peerSubAddress, setupTime, srcAddr);
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            System.out.println(ex.getCause());
        }

    }

    //метод заносит в базу первую
    private void insertToMysql(String[] values) {
        try {

            String connectionID = values[0];
            java.util.Date d = new java.util.Date();
            Timestamp date = new Timestamp((new java.util.Date()).getTime());
            java.util.Date d1 = timeFormat.parse(values[1]);
            Timestamp setupTime = new Timestamp(timeFormat.parse(values[1]).getTime());
            String srcPeerAddress = values[2];
            String srcPeerSubAddress = values[3];
            String disconnectCause = values[4];
            Timestamp connectTime = new Timestamp(timeFormat.parse(values[5]).getTime());
            Timestamp disconnectTime = new Timestamp(timeFormat.parse(values[6]).getTime());;
            String callOrigin = values[7];
            String chargedUnits = values[8];
            String infoType = values[9];
            String src = values[10];
            long transmitPackets;
            long transmitBypes;
            long receivePackets;
            long receiveBytes;
            //иногда в логах приходят отрицательные значения о пакетах и трафике, игнорируем
            try {
                transmitPackets = Long.parseLong(values[10]);
                transmitBypes = Long.parseLong(values[11]);
                receivePackets = Long.parseLong(values[12]);
                receiveBytes = Long.parseLong(values[13]);
            } catch (NumberFormatException nfEx) {
                transmitPackets = 0;
                transmitBypes = 0;
                receivePackets = 0;
                receiveBytes = 0;
            }
            //заполняем параметры PreparedStatement
            stmt.setString(1, src);
            stmt.setString(2, connectionID);
            stmt.setTimestamp(3, setupTime);
            stmt.setString(4, srcPeerAddress);
            stmt.setString(5, srcPeerSubAddress);
            stmt.setString(6, disconnectCause);
            stmt.setTimestamp(7, connectTime);
            stmt.setTimestamp(8, disconnectTime);
            stmt.setString(9, callOrigin);
            stmt.setString(10, chargedUnits);
            stmt.setString(11, infoType);
            stmt.setLong(12, transmitPackets);
            stmt.setLong(13, transmitBypes);
            stmt.setLong(14, receivePackets);
            stmt.setLong(15, receiveBytes);
            //заносим в базу
            stmt.execute();

        } catch (Exception exp) {
            exp.printStackTrace();
        }
    }
    //метод который добавляет значение dstPeerAddress и dstPeerSubAddress в существующий кортеж таблицы в базе данных
    //находя его по полю connectionID

    private void insertToMysql(String connectionID, String dstPeerAddress, String dstSubPeerAddress, String setupTime, String srcAddr) {
        try {
            //берем текущее время + про запас одну секунду
            long currentMilliseconds = timeFormat.parse(setupTime).getTime() + 1000;
            //у cisco ConnectioID не является уникальным на все времена, через определенное время он повторяется, поэтому берем timestamp (-30 минут)
            //чтобы искать запись по значению connectionID внесенную в течении последних 30 минут
            Timestamp startTimestamp = new Timestamp(currentMilliseconds - 1800000);
            Timestamp endTimestamp = new Timestamp(currentMilliseconds);
            updStmt.setString(1, dstPeerAddress);
            updStmt.setString(2, dstSubPeerAddress);
            updStmt.setString(3, connectionID);
            updStmt.setString(4, srcAddr);
            updStmt.setTimestamp(5, startTimestamp);
            updStmt.setTimestamp(6, endTimestamp);

            updStmt.execute();
        } catch (ParseException parseExp) {
            parseExp.printStackTrace();
        } catch (Exception exp) {
            exp.printStackTrace();
        }

    }

    public static void main(String[] args) {

        try {

            Hashtable<String, String> params = new Hashtable<String, String>();
            //создаем hashtable из полученных параметров
            for (int i = 0; i < args.length; i++) {
                if ((i % 2) == 0) {
                    params.put(args[i], args[i + 1]);
                }
            }
            //проверяем наличие необходимых параметров и если они присутсвуют запускаем программу
            if ((params.containsKey("-dbs")) && (params.containsKey("-dbn")) && (params.containsKey("-dbu")) && (params.containsKey("-pass"))) {
                CiscoVoipStat voipStat = new CiscoVoipStat(params);
                voipStat.listen();
            } else {
                System.out.println("Use [-i listening IP address -p listening port] -dbs database IP address"
                        + " -dbn database name -dbu database user -pass database password. By default server listening on 0.0.0.0:514 udp port.");
            }
        } catch (Exception exp) {
            exp.printStackTrace();
        }

    }
}
