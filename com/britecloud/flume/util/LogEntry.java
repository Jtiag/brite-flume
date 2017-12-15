package flume.util;

import java.util.HashMap;
import java.util.Map;

import net.sf.json.JSONObject;

/**
 * @author Administrator
 */
public class LogEntry {
    private Map<String, Object> map = new HashMap();

    public LogEntry() {
    }

    public final <VT> void put(String key, VT value) {
        this.map.put(key, value);
    }

    public final <VT> VT get(String key) {
        return (VT) this.map.get(key);
    }

    public final <VT> boolean contains(String key) {
        return this.map.containsKey(key);
    }

    @Override
    public String toString() {
        JSONObject object = JSONObject.fromObject(this.map);
        return object.toString();
    }
}
