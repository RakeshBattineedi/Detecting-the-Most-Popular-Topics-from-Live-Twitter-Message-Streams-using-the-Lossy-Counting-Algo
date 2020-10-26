import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class LossyCountingParallel extends BaseRichBolt {

    private OutputCollector collector;
    private int numEle = 0;
    private Map<String, Objects> tags = new ConcurrentHashMap<String, Objects>();
    private double ep;
    private double th;

    private int currBuc = 1;
    private int buckSize = (int) Math.ceil(1 / ep);
    private static long startTime;

    public LossyCountingParallel(double e, double threshold) {
        ep = e;
        th = threshold;
    }

    public void prepare(Map config, TopologyContext context, OutputCollector collector) {

        this.collector = collector;
        startTime = System.currentTimeMillis();
    }

    public void execute(Tuple tuple) {

        String word;
        word = tuple.getStringByField("hashTag");
        collector.ack(tuple);
        lossyCounting(word);
    }

    public void lossyCounting(String word) {
        if (numEle < buckSize) {
            if (!tags.containsKey(word)) {
                Objects d = new Objects();
                d.delta = currBuc - 1;
                d.freq = 1;
                d.element = word;
                tags.put(word, d);
            } else {
                Objects d = tags.get(word);
                d.freq += 1;
                tags.put(word, d);
            }
            numEle += 1;
        }

        long currentTime = System.currentTimeMillis();
        if (currentTime >= startTime + 10000) {
            if (!tags.isEmpty()) {
                HashMap<String, Integer> emitList = new HashMap<String, Integer>();
                for (String str_loss : tags.keySet()) {
                    boolean b = check(str_loss);
                    if (b) {
                        Objects d = tags.get(str_loss);
                        emitList.put(str_loss, d.freq);
                    }
                }
                if (!emitList.isEmpty()) {

                    LinkedHashMap<String, Integer> lhm = sortHashMap(emitList);
                    Collection<String> str;

                    if (lhm.size() > 100) {
                        str=Collections.list(Collections.enumeration(lhm.keySet())).subList(0, 100);
                    }else{
                        str= lhm.keySet();
                    }

                    LinkedHashMap<String, Integer> finalEmit = new LinkedHashMap<String, Integer>();
                    for (String s : str) {
                        finalEmit.put("<" + s + ":" + tags.get(s).freq + ">", tags.get(s).freq);
                    }
                    for (String s : finalEmit.keySet()) {
                        collector.emit(new Values(s, finalEmit.get(s)));
                    }

                }
            }
            startTime = currentTime;

        }
        if (buckSize == numEle) {
            Delete();
            numEle = 0;
            currBuc += 1;
        }
    }

    public LinkedHashMap<String, Integer> sortHashMap(HashMap<String, Integer> pm) {
        List<String> mapKeys = new ArrayList<String>(pm.keySet());
        List<Integer> mapValues = new ArrayList<Integer>(pm.values());
        Collections.sort(mapValues, Collections.reverseOrder());
        Collections.sort(mapKeys, Collections.reverseOrder());

        LinkedHashMap<String, Integer> sortedMap = new LinkedHashMap<String, Integer>();

        Iterator<Integer> valueIt = mapValues.iterator();
        while (valueIt.hasNext()) {
            Integer val = valueIt.next();
            Iterator<String> keyIt = mapKeys.iterator();

            while (keyIt.hasNext()) {
                String key = keyIt.next();
                Integer comp1 = pm.get(key);
                Integer comp2 = val;

                if (comp1.equals(comp2)) {
                    keyIt.remove();
                    sortedMap.put(key, val);
                    break;
                }
            }
        }
        return sortedMap;
    }

    public boolean check(String lossyWord) {
        if (th == -1.0) {
            return true;
        } else {
            Objects d = tags.get(lossyWord);
            double a = (th - ep) * numEle;
            if (d.freq >= a)
                return true;
            else
                return false;
        }
    }

    public void Delete() {
        for (String word : tags.keySet()) {
            Objects d = tags.get(word);
            double sum = d.freq + d.delta;
            if (sum <= currBuc) {
                tags.remove(word);
            }
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("list", "freq"));
    }

}
