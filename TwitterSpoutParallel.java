import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import twitter4j.*;
import twitter4j.auth.AccessToken;
import twitter4j.conf.ConfigurationBuilder;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

public class TwitterSpoutParallel extends BaseRichSpout {

    SpoutOutputCollector _collector;
    LinkedBlockingQueue<String> queue = null;
    TwitterStream twitterStream;
    private FileWriter fw;
    private BufferedWriter bw;
    PrintWriter _log;



    String consumerKey = "j8QR099FWCfppBwsP4zjDkRns";
    String consumerSecret = "6rmpkJDW3Wqh9uQiuxaMqA3HbayVWhND72bYUXQ0JVWcyLwCnj";
    String accessToken = "1235019141800550401-hZ4R88rVHj6RcYUNDHF7kTB9whr4wh";
    String accessTokenSecret = "PYOtv8qUhZSvs3DdgoOVz2ImTDZWLshUwc7JU1U18DqLE";


    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {

        queue = new LinkedBlockingQueue<String>(1000);
        _collector = collector;

        try {
            _log = new PrintWriter(new File("/s/chopin/k/grad/rakeshb/Tags.txt"));
        } catch (Exception e) {
            System.out.println("Error in writing to file");
            e.printStackTrace();
        }

        StatusListener listener = new StatusListener() {

            public void onStatus(Status status) {
                try {

                            for (HashtagEntity ht : status.getHashtagEntities()) {

                                String hashTag = ht.getText().toLowerCase();

                                if (!hashTag.isEmpty()) {
                                    try {

                                        queue.put(hashTag);
                                        _log.write(hashTag + "\n");
                                        _log.flush();

                                    } catch (Exception e) {
                                        e.printStackTrace();
                                    }
                                } else {
                                    Utils.sleep(50);
                                }
                            }

                } catch (Exception e) {
                    e.printStackTrace();
                }

            }

            public void onDeletionNotice(StatusDeletionNotice sdn) {
            }

            public void onTrackLimitationNotice(int i) {
            }

            public void onScrubGeo(long l, long l1) {
            }

            public void onException(Exception ex) {
            }

            public void onStallWarning(StallWarning arg0) {

            }
        };

        ConfigurationBuilder cb = new ConfigurationBuilder();

        cb.setDebugEnabled(true).setOAuthConsumerKey(consumerKey).setOAuthConsumerSecret(consumerSecret)
                .setOAuthAccessToken(accessToken).setOAuthAccessTokenSecret(accessTokenSecret);

        twitterStream = new TwitterStreamFactory(new ConfigurationBuilder().setJSONStoreEnabled(true).build())
                .getInstance();

        twitterStream.addListener(listener);
        twitterStream.setOAuthConsumer(consumerKey, consumerSecret);
        AccessToken token = new AccessToken(accessToken, accessTokenSecret);
        twitterStream.setOAuthAccessToken(token);
        twitterStream.sample("en");

    }

    public void nextTuple() {
        String ret = queue.poll();
        if (ret == null) {
            Utils.sleep(50);
        } else {
            _collector.emit(new Values(ret));
        }
    }

    public void close() {
        twitterStream.shutdown();
    }

    public void ack(Object id) {
    }

    public void fail(Object id) {
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("hashTag"));
    }

}