
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;


/**
 * Created by dell on 2018/10/29.
 */
public class ReadMysql {
    public static void main(String[] args) {
        try {
            ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
            JDBCInputFormat inputFormat = JDBCInputFormat.buildJDBCInputFormat()
                    .setDrivername("com.mysql.jdbc.Driver")
                    .setDBUrl("jdbc:mysql://singlenode:3306/test_db")
                    .setUsername("hive")
                    .setPassword("hive")
                    .setQuery("select name from hive")
                   // .setRowTypeInfo(new RowTypeInfo(TypeInformation.of(Integer.class), TypeInformation.of(String.class)))
                    .setRowTypeInfo(new RowTypeInfo(TypeInformation.of(String.class)))
                    .finish();

           // DataSource source = env.createInput(inputFormat);
            DataSource<Row> source = env.createInput(inputFormat);
            source.print();
            //env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}