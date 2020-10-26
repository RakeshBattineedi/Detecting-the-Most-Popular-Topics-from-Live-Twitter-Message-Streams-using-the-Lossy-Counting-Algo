import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;
import org.apache.storm.LocalCluster;

public class Parallel {
    public static double e;
    public static double th = 0.21;

    public static void main(String[] args) throws Exception {

        Config conf = new Config();

        conf.setDebug(true);
        conf.setNumWorkers(5);

        TopologyBuilder builder = new TopologyBuilder();

        e = 0.2;
        builder.setSpout("TwitterSpoutParallel", new TwitterSpoutParallel());
        LossyCountingParallel lc = new LossyCountingParallel(e, th);
        builder.setBolt("LossyCountingParallelBolt", lc,3).fieldsGrouping("TwitterSpoutParallel",new Fields("hashTag"));
        builder.setBolt("ReportBoltParallel", new ReportBoltParallel()).globalGrouping("LossyCountingParallelBolt");

      StormSubmitter.submitTopology("Parallel", conf, builder.createTopology());

//        LocalCluster cluster = new LocalCluster();
//
//        cluster.submitTopology("test", conf, builder.createTopology());

        Utils.sleep(10000);
    }
}
