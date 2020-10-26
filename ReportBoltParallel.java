import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.text.SimpleDateFormat;
import java.util.*;

public class ReportBoltParallel extends BaseRichBolt {

    private FileWriter fw;
    private BufferedWriter bw1;
    private int freq;
    long startTime = System.currentTimeMillis();
    HashMap<String, Integer> hm = new HashMap<String, Integer>();
    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");


    public void prepare(Map config, TopologyContext context, OutputCollector collector) {

        try {
            fw = new FileWriter("/s/chopin/k/grad/rakeshb/Top100.txt", true);
            bw1 = new BufferedWriter(fw);

        } catch (Exception e) {
            System.out.println("UNABLE TO WRITE FILE :: 1 ");
            e.printStackTrace();
        }
        startTime = System.currentTimeMillis();

    }

    public void execute(Tuple tuple) {

        String list = tuple.getStringByField("list");
        freq = tuple.getIntegerByField("freq");
        displayOutput(list, freq);

    }

    public void displayOutput(String list, int freq) {
        long currentTime = System.currentTimeMillis();
        if (currentTime >= startTime + 10000 && !hm.isEmpty()) {
            LinkedHashMap<String, Integer> lhm = sortHashMap(hm);
            Collection<String> str;
            if (lhm.size() > 100) {
                str=Collections.list(Collections.enumeration(lhm.keySet())).subList(0, 100);
            }else{
                str = lhm.keySet();
            }
            Date resultdate = new Date(startTime);
            try {
                bw1.write("<" + dateFormat.format(resultdate) + ">" + str.toString() + "\n");
                bw1.flush();
            } catch (Exception e) {
                e.printStackTrace();
            }
            hm.clear();
            startTime = currentTime;
        } else {
            if (!hm.containsKey(list))
                hm.put(list, freq);
        }
    }


    public LinkedHashMap<String, Integer> sortHashMap(HashMap<String, Integer> passedMap) {
        List<String> mapKeys = new ArrayList<String>(passedMap.keySet());
        List<Integer> mapValues = new ArrayList<Integer>(passedMap.values());
        Collections.sort(mapValues, Collections.reverseOrder());
        Collections.sort(mapKeys, Collections.reverseOrder());

        LinkedHashMap<String, Integer> sortedMap = new LinkedHashMap<String, Integer>();

        Iterator<Integer> valueIt = mapValues.iterator();
        while (valueIt.hasNext()) {
            Integer val = valueIt.next();
            Iterator<String> keyIt = mapKeys.iterator();

            while (keyIt.hasNext()) {
                String key = keyIt.next();
                Integer comp1 = passedMap.get(key);
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