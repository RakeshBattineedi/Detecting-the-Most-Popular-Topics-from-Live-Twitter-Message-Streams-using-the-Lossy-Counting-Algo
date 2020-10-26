import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;

public class Nonparallel {
    public static double e = 0.2;
    public static Double th = 0.21;

    public static void main(String[] args) throws Exception {

        Config conf = new Config();

        conf.setDebug(true);


        TopologyBuilder builder = new TopologyBuilder();
        e = 0.2;

        builder.setSpout("TwitterStreamSpout", new TwitterStreamSpout());
        LossyCounting lc = new LossyCounting(e, th);
        builder.setBolt("LossyCountingBolt", lc).shuffleGrouping("TwitterStreamSpout");
        builder.setBolt("ReportBolt", new ReportBolt()).globalGrouping("LossyCountingBolt");

       StormSubmitter.submitTopology("Nparallel", conf, builder.createTopology());

//         LocalCluster cluster = new LocalCluster();
//
//         cluster.submitTopology("test", conf, builder.createTopology());

         Utils.sleep(10000);

    }
}
