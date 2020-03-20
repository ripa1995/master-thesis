
import org.deidentifier.arx.*;
import org.deidentifier.arx.criteria.KAnonymity;
import org.hsqldb.jdbc.JDBCStatement;
import org.hsqldb.util.DatabaseManagerSwing;


import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.*;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

public class MainMasterThesis{


    private static JDBCStatement statement;

    public static void main(String args[]) throws IOException, SQLException {
        try {
            Class.forName("org.hsqldb.jdbc.JDBCDriver" );
        } catch (Exception e) {
            System.err.println("ERROR: failed to load HSQLDB JDBC driver.");
            e.printStackTrace();
            return;
        }

        Connection c = DriverManager.getConnection("jdbc:hsqldb:file:data\\test", "SA", "");
        System.out.println("Connected");
        statement = (JDBCStatement) c.createStatement();
        /*
        importAdultDataset();

        String query = "select maritalStatus, sex, AVG(age) as AVG_AGE, count(sex) as num from adult group by maritalStatus, sex";
        String fileName = "avg_age_count_sex_gb_maritalstatus_sex";
        printResultToFile(statement.executeQuery("explain json minimal for "+query),fileName,".json");
        printResultToFile(statement.executeQuery(query),fileName,".csv");

        c.close();
        mondrian();
        int[] k_array = new int[]{5,10,15,20};
        double[] s_array = new double[]{0.0,0.04,0.08,0.12,0.16,0.20};
        for(int k:k_array) {
            for (double s : s_array) {
                flashAdult(k, s);
            }
        }*/
        int[] k_array = new int[]{5,10,15,20};
        int[] s_array = new int[]{0,4,8,12,16,20};
        for(int k:k_array) {
            importAnonAdultDataset("adult_mondrian_anon_"+k+".csv","adult_mondrian_"+k);
            for (int s : s_array) {
                importAnonAdultDataset("adult_flash_anon_"+k+"_"+s+"%.csv","adult_flash_"+k+"_"+s);
                //System.out.println("drop table "+"adult_flash_"+k+"_"+s);
            }
        }

    }

    private static void importAnonAdultDataset(String filename, String tableName) throws SQLException, IOException {
        statement.execute("create table if not exists "+tableName+"(age VARCHAR(50), age_min int, age_max int,"
                + "workclass VARCHAR(50), " + "fnlwgt int, "
                + "education VARCHAR(50), " + "educationNum int, "
                + "maritalStatus VARCHAR(50), " + "occupation VARCHAR(50), "
                + "relationship VARCHAR(50), " + "race VARCHAR(50), "
                + "sex VARCHAR(50), " + "capitalGain int, "
                + "capitalLoss int, " + "hoursPerWeek int, "
                + "nativeCountry VARCHAR(50), " + "income VARCHAR(50))");
        ResultSet resultSet = statement.executeQuery("SELECT COUNT(*) FROM "+tableName);
        resultSet.next();
        //System.out.println(resultSet.getInt(1));
        if (resultSet.getInt(1)==0) {

            Stream st = Files.lines(Paths.get("./data/anonymized/"+filename)).skip(1);
            Object[] strings = st.toArray();
            for(Object o:strings){
                addAnonAdult(o,tableName);
            }
        }
    }

    private static void addAnonAdult(Object o,String tableName){
        String s = (String) o;
        s = s.replaceAll("\"","'");
        String[] values = s.split(";");
        //check if row is suppressed
        if (Arrays.stream(values).filter(x->x.equals("*")).count()==9){
            System.out.println("Suppressed");
            return;
        }
        values = Arrays.stream(values).map(x->x.equals("*")?"null":x).toArray(String[]::new);
        values = Arrays.stream(values).map(x->isIntegerOrNull(x)?x:"'"+x+"'").toArray(String[]::new);
        String[] age = (values[0].replace("[","").replace("]","").replace("'","")).trim().split(",");
        String min, max, sql = null;
        if (age.length>1){
            min = age[0];
            max = age[1];
        }else {
            min=age[0];
            max=age[0];
        }
        try {
            sql ="insert into "+tableName+" values("+
                    values[0]+","+min+","+max+","+values[1]+","+
                    values[2]+","+values[3]+","+
                    values[4]+","+values[5]+","+
                    values[6]+","+values[7]+","+
                    values[8]+","+values[9]+","+
                    values[10]+","+values[11]+","+
                    values[12]+","+values[13]+","+
                    values[14]+")";
            statement.execute(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    private static boolean isIntegerOrNull(String x) {
        try{
            Integer.valueOf(x);
        } catch (NumberFormatException e) {
            if (x.equals("null")){
                return true;
            }
            return false;
        }
        return true;
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
            Stream st = Files.lines(Paths.get("./data/dataset/adult.csv"));
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
/*
    private static void mondrian() throws IOException {
        DataReader datareader = new DataReader("./data/dataset/adult.csv");
        EquivalenceClass data = new EquivalenceClass();
        //Numero di righe
        int tupleNumber=32561;
        for(int i=0;i<tupleNumber;i++)
            data.add(datareader.getNextTuple());
        //Valore di K
        int k = 3;
        //Vettore contenente i quasi identifier
        String[] qid = ("0 2").split(" ");

        Mondrian algo = new Mondrian();
        algo.setQID(qid);
        algo.setData(data);
        algo.setK(k);
        algo.setStrictPartitioning();
        algo.run();
        System.out.println(algo.getResults().toString());
        //System.out.println(algo.getResults().get(0).get(0).getValue(2) +" "+algo.getResults().get(0).get(1).getValue(2));
    }
*/
    private  static void flashAdult(int k, double suppression) throws IOException {
        Data data = Data.create("./data/dataset/adult_clear.csv", StandardCharsets.UTF_8, ',');

        // Define input files
        data.getDefinition().setAttributeType("age", AttributeType.Hierarchy.create("./data/hierarchies/adult_age.csv", StandardCharsets.UTF_8, ';'));
        data.getDefinition().setAttributeType("education", AttributeType.Hierarchy.create("./data/hierarchies/adult_education.csv", StandardCharsets.UTF_8, ';'));
        data.getDefinition().setAttributeType("marital-status", AttributeType.Hierarchy.create("./data/hierarchies/adult_marital-status.csv", StandardCharsets.UTF_8, ';'));
        data.getDefinition().setAttributeType("native-country", AttributeType.Hierarchy.create("./data/hierarchies/adult_native-country.csv", StandardCharsets.UTF_8, ';'));
        data.getDefinition().setAttributeType("occupation", AttributeType.Hierarchy.create("./data/hierarchies/adult_occupation.csv", StandardCharsets.UTF_8, ';'));
        data.getDefinition().setAttributeType("race", AttributeType.Hierarchy.create("./data/hierarchies/adult_race.csv", StandardCharsets.UTF_8, ';'));
        data.getDefinition().setAttributeType("salary-class", AttributeType.Hierarchy.create("./data/hierarchies/adult_salary-class.csv", StandardCharsets.UTF_8, ';'));
        data.getDefinition().setAttributeType("sex", AttributeType.Hierarchy.create("./data/hierarchies/adult_sex.csv", StandardCharsets.UTF_8, ';'));
        data.getDefinition().setAttributeType("workclass", AttributeType.Hierarchy.create("./data/hierarchies/adult_workclass.csv", StandardCharsets.UTF_8, ';'));
        data.getDefinition().setAttributeType("fnlwgt",AttributeType.INSENSITIVE_ATTRIBUTE);
        data.getDefinition().setAttributeType("education-num",AttributeType.INSENSITIVE_ATTRIBUTE);
        data.getDefinition().setAttributeType("relationship",AttributeType.INSENSITIVE_ATTRIBUTE);
        data.getDefinition().setAttributeType("capital-gain",AttributeType.INSENSITIVE_ATTRIBUTE);
        data.getDefinition().setAttributeType("capital-loss",AttributeType.INSENSITIVE_ATTRIBUTE);
        data.getDefinition().setAttributeType("hours-per-week",AttributeType.INSENSITIVE_ATTRIBUTE);

        // set the minimal/maximal generalization height
        //data.getDefinition().setMaximumGeneralization("age", 3);
        //data.getDefinition().setMaximumGeneralization("education", 2);

        // Create an instance of the anonymizer
        ARXAnonymizer anonymizer = new ARXAnonymizer();

        // Execute the algorithm
        ARXConfiguration config = ARXConfiguration.create();
        config.addPrivacyModel(new KAnonymity(k));
        config.setSuppressionLimit(suppression);
        ARXResult result = anonymizer.anonymize(data, config);

        String filename = "adult_flash_anon_"+k+"_"+(int)(suppression*100)+"%";

        // Print info
        printResult(result, data, filename);

        // Write results
        System.out.print(" - Writing data...");
        result.getOutput(false).save("data/anonymized/"+filename+".csv", ';');
        System.out.println("Done!");
    }

    protected static void printResult(final ARXResult result, final Data data, String fileName) throws IOException {

        StringBuilder sb = new StringBuilder();

        // Print time
        final DecimalFormat df1 = new DecimalFormat("#####0.00");
        final String sTotal = df1.format(result.getTime() / 1000d) + "s";
        sb.append(" - Time needed: " + sTotal);
        sb.append("\n");

        // Extract
        final ARXLattice.ARXNode optimum = result.getGlobalOptimum();
        final List<String> qis = new ArrayList<String>(data.getDefinition().getQuasiIdentifyingAttributes());

        if (optimum == null) {
            return ;
        }

        // Initialize
        final StringBuffer[] identifiers = new StringBuffer[qis.size()];
        final StringBuffer[] generalizations = new StringBuffer[qis.size()];
        int lengthI = 0;
        int lengthG = 0;
        for (int i = 0; i < qis.size(); i++) {
            identifiers[i] = new StringBuffer();
            generalizations[i] = new StringBuffer();
            identifiers[i].append(qis.get(i));
            generalizations[i].append(optimum.getGeneralization(qis.get(i)));
            if (data.getDefinition().isHierarchyAvailable(qis.get(i)))
                generalizations[i].append("/").append(data.getDefinition().getHierarchy(qis.get(i))[0].length - 1);
            lengthI = Math.max(lengthI, identifiers[i].length());
            lengthG = Math.max(lengthG, generalizations[i].length());
        }

        // Padding
        for (int i = 0; i < qis.size(); i++) {
            while (identifiers[i].length() < lengthI) {
                identifiers[i].append(" ");
            }
            while (generalizations[i].length() < lengthG) {
                generalizations[i].insert(0, " ");
            }
        }

        // Print
        sb.append(" - Information loss: " + result.getGlobalOptimum().getLowestScore() + " / " + result.getGlobalOptimum().getHighestScore());
        sb.append("\n");
        sb.append(" - Optimal generalization");
        sb.append("\n");
        for (int i = 0; i < qis.size(); i++) {
            sb.append("   * " + identifiers[i] + ": " + generalizations[i]);
            sb.append("\n");
        }
        sb.append(" - Statistics");
        sb.append("\n");
        sb.append(result.getOutput(result.getGlobalOptimum(), false).getStatistics().getEquivalenceClassStatistics());

        File dir = new File("./data/anonymized");
        dir.mkdirs();
        File file = new File(dir,fileName+"_statistics.txt");
        FileOutputStream fileOutputStream = new FileOutputStream(file,true);

        fileOutputStream.write(sb.toString().getBytes());

    }


}