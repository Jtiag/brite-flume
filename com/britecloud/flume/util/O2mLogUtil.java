package flume.util;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Iterator;

import net.sf.json.JSONObject;

/**
 * @author Administrator
 */
public class O2mLogUtil {
    public O2mLogUtil() {
    }

    public static void main(String[] args) throws IOException {
        FileReader fr = new FileReader("d:/log/o2m-oc-srv-data.log");
        BufferedReader br = new BufferedReader(fr);
        String s = br.readLine();
        JSONObject jsonObj = JSONObject.fromObject(s);
        Iterator var5 = jsonObj.keySet().iterator();

        while (var5.hasNext()) {
            Object key = var5.next();
            Object value = jsonObj.get(key);
            System.out.println("key:" + key + "\tvalue:" + value);
        }

    }
}
