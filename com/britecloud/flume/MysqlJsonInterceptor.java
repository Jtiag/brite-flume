package flume;

import flume.util.LogEntry;
import flume.util.MysqlLogParser;
import com.google.common.base.Preconditions;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MysqlJsonInterceptor implements Interceptor {
    private static final Logger logger = LoggerFactory.getLogger(MysqlJsonInterceptor.class);
    static final String DATE_PATTERN = "datePattern";
    static final String NAME = "name";
    static final String IP = "ip";
    static final String CHARSET = "charset";
    MysqlLogParser logParser;
    String name;
    String ip;
    String charset;

    private MysqlJsonInterceptor(MysqlLogParser logParser, String name, String ip, String charset) {
        this.logParser = logParser;
        this.name = name;
        this.ip = ip;
        this.charset = charset;
    }

    public void close() {
        logger.debug("close...");
    }

    public void initialize() {
        logger.debug("initialize....");
        if (this.ip == null) {
            try {
                InetAddress addr = InetAddress.getLocalHost();
                this.ip = addr.getHostAddress();
                logger.debug("get ip:" + this.ip);
            } catch (UnknownHostException var3) {
                logger.warn("Could not get local host address. Exception follows.", var3);
            }
        }

        if (this.charset == null) {
            this.charset = "UTF-8";
        }

        logger.debug("initialized");
    }

    public Event intercept(Event event) {
        String body;
        try {
            body = new String(event.getBody(), this.charset);
            logger.debug("event:" + body);
        } catch (UnsupportedEncodingException var5) {
            throw new RuntimeException("unspport charset:" + this.charset);
        }

        if (StringUtils.isNotBlank(body)) {
            LogEntry entry = this.logParser.parseLog(body);
            entry.put("serviceName", this.name);
            entry.put("IP", this.ip);
            String result = entry.toString();
            event.setBody(result.getBytes());
            return event;
        } else {
            return null;
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
        MysqlLogParser logParser;
        String name;
        String ip;
        String charset;

        public Builder() {
        }

        public void configure(Context context) {
            String patternString = context.getString("datePattern");
            Preconditions.checkArgument(!StringUtils.isEmpty(patternString), "Must supply a valid pattern string");
            this.name = context.getString("name");
            Preconditions.checkArgument(!StringUtils.isEmpty(this.name), "Must supply a valid name");
            this.ip = context.getString("ip");
            MysqlJsonInterceptor.logger.debug("configure   name:" + this.name + "\tdatePattern:" + patternString + "\tip:" + this.ip);
            this.charset = context.getString("charset");
            if (StringUtils.isNotBlank(patternString)) {
                this.logParser = new MysqlLogParser(patternString);
            } else {
                this.logParser = new MysqlLogParser();
            }

        }

        public Interceptor build() {
            Preconditions.checkArgument(this.logParser != null, "ConversionPattern was misconfigured");
            return new MysqlJsonInterceptor(this.logParser, this.name, this.ip, this.charset);
        }
    }
}
