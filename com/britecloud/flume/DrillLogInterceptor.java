//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

package flume;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DrillLogInterceptor implements Interceptor {
    private static final Logger logger = LoggerFactory.getLogger(DrillLogInterceptor.class);
    static final String TB_NAME = "tbName";
    static final String DB_NAME = "dbName";
    static final String DT_PATTERN = "dtPattern";
    static final String DT_KEY = "dtKey";
    String dbName;
    String tbName;
    Pattern dtPattern;
    String dtKey;

    private DrillLogInterceptor(String dbName, String tbName, Pattern dtPattern, String dtKey) {
        this.dbName = dbName;
        this.tbName = tbName;
        this.dtPattern = dtPattern;
        this.dtKey = dtKey;
    }

    public void close() {
        logger.debug("close...");
    }

    public void initialize() {
        logger.debug("initialize....");
        logger.debug("initialized");
    }

    public Event intercept(Event event) {
        try {
            Map<String, String> header = event.getHeaders();
            String fileName = (String)header.get("basename");
            logger.debug("fileName:{}", fileName);
            Matcher matcher = this.dtPattern.matcher(fileName);
            if (matcher.find()) {
                String dt = matcher.group(1);
                if (StringUtils.isNotBlank(dt) && StringUtils.length(dt) == 8) {
                    dt = StringUtils.join(new String[]{dt.substring(0, 4), dt.substring(4, 6), dt.substring(6)}, "-");
                    header.put(this.dtKey, dt);
                    header.put("tbName", this.tbName);
                    header.put("dbName", this.dbName);
                    String fName = fileName.replaceAll("(\\.\\w*)?$", "");
                    header.put("basename", fName);
                    String body = new String(event.getBody());
                    body = StringUtils.replace(body, "\r", "");
                    event.setBody(body.getBytes());
                    return event;
                }
            }

            return null;
        } catch (Exception var8) {
            throw new RuntimeException("");
        }
    }

    public List<Event> intercept(List<Event> eventList) {
        logger.debug("eventList start:" + eventList.size());
        List<Event> result = new ArrayList();
        Iterator var3 = eventList.iterator();

        while(var3.hasNext()) {
            Event event = (Event)var3.next();
            Event res = this.intercept(event);
            if (res != null) {
                result.add(res);
            }
        }

        eventList.clear();
        logger.debug("eventList finished:" + result.size());
        return result;
    }

    public static class Builder implements org.apache.flume.interceptor.Interceptor.Builder {
        Pattern dtPattern;
        String dtPatterStr;
        String tbName;
        String dtKey;
        String dbName;
        private static final String DEFAULT_DT_KEY = "dt";
        private static final String DEFAULT_DB_NAME = "test";

        public Builder() {
        }

        public void configure(Context context) {
            this.dbName = context.getString("dbName");
            this.dbName = StringUtils.defaultIfEmpty(this.dbName, "test");
            this.tbName = context.getString("tbName");
            this.dtKey = context.getString("dtKey");
            this.dtKey = StringUtils.defaultIfEmpty(this.dtKey, "dt");
            this.dtPatterStr = context.getString("dtPattern");
            this.dtPattern = Pattern.compile(this.dtPatterStr);
            DrillLogInterceptor.logger.info("pos log interceptor param,dbName:{},tbName:{},dtKey:{},dtPatter:{}", new Object[]{this.dbName, this.tbName, this.dtKey, this.dtPatterStr});
        }

        public Interceptor build() {
            Preconditions.checkArgument(this.dtPattern != null, "dtPattern was misconfigured");
            Preconditions.checkArgument(StringUtils.isNotBlank(this.tbName), "dtPattern was misconfigured");
            return new DrillLogInterceptor(this.dbName, this.tbName, this.dtPattern, this.dtKey);
        }
    }
}
