import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

public class ReportBolt extends BaseRichBolt {

    private FileWriter fw;
    private BufferedWriter bw1;
    private long time;
    SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");


    public void prepare(Map config, TopologyContext context, OutputCollector collector) {

        try {
            fw = new FileWriter( "/s/chopin/k/grad/rakeshb/Top100.txt", true);
            bw1 = new BufferedWriter(fw);

        } catch (Exception e) {
            System.out.println("UNABLE TO WRITE FILE :: 1 ");
            e.printStackTrace();
        }

    }

    public void execute(Tuple tuple) {

        String list = tuple.getStringByField("list");
        time = tuple.getLongByField("time");
        Date resultdate = new Date(time);
        try {
            bw1.write("<" + df.format(resultdate) + ">" + list + "\n");
            bw1.flush();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

    public void cleanup() {
        try {
            bw1.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}