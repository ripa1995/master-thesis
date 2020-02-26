import org.hsqldb.jdbc.JDBCStatement;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.stream.Stream;

public class MainMasterThesis{


    private static JDBCStatement statement;

    public static void main(String args[]){
        try {
            Class.forName("org.hsqldb.jdbc.JDBCDriver" );
        } catch (Exception e) {
            System.err.println("ERROR: failed to load HSQLDB JDBC driver.");
            e.printStackTrace();
            return;
        }
        try {
            Connection c = DriverManager.getConnection("jdbc:hsqldb:file:data\\test", "SA", "");
            System.out.println("Connected");
            statement = (JDBCStatement) c.createStatement();
            importAdultDataset();

            String query = "select maritalStatus, sex, AVG(age) as AVG_AGE, count(sex) as num from adult group by maritalStatus, sex";
            String fileName = "avg_age_count_sex_gb_maritalstatus_sex";
            printResultToFile(statement.executeQuery("explain json minimal for "+query),fileName,".json");
            printResultToFile(statement.executeQuery(query),fileName,".csv");

            c.close();
        } catch (SQLException | IOException e) {
            e.printStackTrace();
        }

    }

    private static void printResultToFile(ResultSet resultSet, String fileName,String extension) throws SQLException, IOException {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        int columnCount = resultSetMetaData.getColumnCount();
        ArrayList<String> arrayList = new ArrayList<String>();
        StringBuilder stringBuilder = new StringBuilder();
        if(extension.equals(".csv")) {
            for (int i = 1; i <= columnCount; i++) {
                stringBuilder.append(resultSetMetaData.getColumnName(i)).append(",");
            }
            arrayList.add(stringBuilder.substring(0, stringBuilder.length() - 1) + "\n");
        }
        while (resultSet.next()){
            stringBuilder = new StringBuilder();
            for(int i=1;i<=columnCount;i++){
                stringBuilder.append(resultSet.getObject(i)).append(",");
            }
            arrayList.add(stringBuilder.substring(0,stringBuilder.length()-1)+"\n");
        }
        File dir = new File("./results");
        dir.mkdirs();
        File file = new File(dir,fileName+extension);
        FileOutputStream fileOutputStream = new FileOutputStream(file,true);
        for (String s1:arrayList) {
            fileOutputStream.write(s1.getBytes());
        }
    }

    private static void importAdultDataset() throws SQLException, IOException {
        statement.execute("create table if not exists adult(age int, "
                + "workclass VARCHAR(50), " + "fnlwgt int, "
                + "education VARCHAR(50), " + "educationNum int, "
                + "maritalStatus VARCHAR(50), " + "occupation VARCHAR(50), "
                + "relationship VARCHAR(50), " + "race VARCHAR(50), "
                + "sex VARCHAR(50), " + "capitalGain int, "
                + "capitalLoss int, " + "hoursPerWeek int, "
                + "nativeCountry VARCHAR(50), " + "income VARCHAR(50))");
        ResultSet resultSet = statement.executeQuery("SELECT COUNT(*) FROM adult");
        resultSet.next();
        if (resultSet.getInt(1)==0) {
            Stream st = Files.lines(Paths.get("./data/adult.csv"));
            st.forEach(MainMasterThesis::addAdult);
        }
    }

    private static void addAdult(Object o){
        String s = (String) o;
        s = s.replaceAll("\"","'");
        String[] values = s.split(",");
        try {
            statement.execute("insert into adult values("+
                    values[0]+","+values[1]+","+
                    values[2]+","+values[3]+","+
                    values[4]+","+values[5]+","+
                    values[6]+","+values[7]+","+
                    values[8]+","+values[9]+","+
                    values[10]+","+values[11]+","+
                    values[12]+","+values[13]+","+
                    values[14]+")");
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }
}