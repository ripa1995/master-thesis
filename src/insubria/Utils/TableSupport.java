package insubria.Utils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;

public class TableSupport {
    StringBuilder stringBuilder;
    String tableName;
    long denom;

    public TableSupport(String value) {
        stringBuilder = new StringBuilder();
        tableName = value;
    }

    public void addRows(ResultSet resultSet, String fileName, boolean calculateInformationLoss, boolean baseForInformationLoss) throws SQLException {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        int columnCount = resultSetMetaData.getColumnCount();
        if(stringBuilder.length()==0) {
            for (int i = 1; i <= columnCount; i++) {
                stringBuilder.append(resultSetMetaData.getColumnName(i)).append(",");
            }
            if (calculateInformationLoss) {
                stringBuilder.append("INFORMATIONLOSS");
            } else {
                stringBuilder = new StringBuilder(stringBuilder.substring(0, stringBuilder.length() - 1));
            }
            stringBuilder.append("\n");
        }
        while (resultSet.next()){
            int i;
            for(i=1;i<=columnCount;i++){
                stringBuilder.append(resultSet.getObject(i)).append(",");
            }
            if(calculateInformationLoss) {
                if (baseForInformationLoss) {
                    denom = (long) resultSet.getObject(i - 1);
                }
                long num = (long) resultSet.getObject(i - 1);
                stringBuilder.append(1.0-(1.0*num/denom));
            } else {
                stringBuilder = new StringBuilder(stringBuilder.substring(0, stringBuilder.length() - 1));
            }
            stringBuilder.append("\n");
        }
    }

    public void addRows(ResultSet resultSet, String fileName, boolean calculateInformationLoss) throws SQLException {
        addRows(resultSet,fileName,calculateInformationLoss,false);
    }

    public void printToFile() throws IOException {
        File dir = new File("./results");
        dir.mkdirs();
        File file = new File(dir,tableName+".csv");
        FileOutputStream fileOutputStream = new FileOutputStream(file,true);
        fileOutputStream.write(stringBuilder.toString().getBytes());
    }
}
