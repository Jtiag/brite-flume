package flume;

import flume.util.LogEntry;
import flume.util.LogParser;
import com.google.common.base.Preconditions;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Iterator;
import java.util.List;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DruidJsonInterceptor implements Interceptor {
    private static final Logger logger = LoggerFactory.getLogger(DruidJsonInterceptor.class);
    static final String PATTERN = "pattern";
    static final String NAME = "name";
    static final String IP = "ip";
    static final String CHARSET = "charset";
    LogParser logParser;
    String name;
    String ip;
    String charset;

    private DruidJsonInterceptor(LogParser logParser, String name, String ip, String charset) {
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
        logger.debug("event");

        String body;
        try {
            body = new String(event.getBody(), this.charset);
            logger.debug("event...1");
        } catch (UnsupportedEncodingException var5) {
            logger.debug("event...exception");
            throw new RuntimeException("unspport charset:" + this.charset);
        }

        logger.debug("event...2");
        if (StringUtils.isNotBlank(body)) {
            logger.debug("event...3");
            LogEntry entry = this.logParser.parseLine(body);
            logger.debug("event...4");
            entry.put("serviceName", this.name);
            logger.debug("event...5");
            entry.put("IP", this.ip);
            logger.debug("event...6");
            String result = entry.toString();
            logger.debug("event...7");
            event.setBody(result.getBytes());
            logger.debug("event...8");
            logger.debug(result);
        }

        logger.debug("event end");
        return event;
    }

    public List<Event> intercept(List<Event> eventList) {
        logger.debug("event list");
        Iterator var2 = eventList.iterator();

        while(var2.hasNext()) {
            Event event = (Event)var2.next();
            this.intercept(event);
        }

        logger.debug("event list end");
        return eventList;
    }

    public static class Builder implements org.apache.flume.interceptor.Interceptor.Builder {
        LogParser logParser;
        String name;
        String ip;
        String charset;

        public Builder() {
        }

        public void configure(Context context) {
            String patternString = context.getString("pattern");
            Preconditions.checkArgument(!StringUtils.isEmpty(patternString), "Must supply a valid pattern string");
            this.name = context.getString("name");
            Preconditions.checkArgument(!StringUtils.isEmpty(this.name), "Must supply a valid name");
            this.ip = context.getString("ip");
            DruidJsonInterceptor.logger.debug("configure   name:" + this.name + "\tpattern:" + patternString + "\tip:" + this.ip);
            this.charset = context.getString("charset");
            this.logParser = new LogParser(patternString);
        }

        public Interceptor build() {
            Preconditions.checkArgument(this.logParser != null, "ConversionPattern was misconfigured");
            return new DruidJsonInterceptor(this.logParser, this.name, this.ip, this.charset);
        }
    }
}
