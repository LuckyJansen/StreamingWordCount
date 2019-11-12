
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;

/**
 * Created by master on 8/17/17.
 */
public class ReadFile {
    public static void main(String[] args) throws Exception {
        Path pa = new Path("/usr/local/flink/test/input.txt");

        TextInputFormat format = new TextInputFormat(pa);

        BasicTypeInfo typeInfo = BasicTypeInfo.STRING_TYPE_INFO;

        format.setCharsetName("UTF-8");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> st = env.readFile(format, "/usr/local/flink/test/input.txt", FileProcessingMode.PROCESS_CONTINUOUSLY, 1L, (TypeInformation) typeInfo);

        st.print();

        env.execute();

    }

}
