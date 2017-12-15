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

/**
 * @author Administrator
 */
public class Log4jJsonInterceptor implements Interceptor {
    private static final Logger logger = LoggerFactory.getLogger(Log4jJsonInterceptor.class);
    static final String PATTERN = "pattern";
    static final String NAME = "name";
    static final String IP = "ip";
    static final String CHARSET = "charset";
    static final String MSG_MAX_LENGTH = "msg-max-length";
    LogParser logParser;
    String name;
    String ip;
    String charset;

    private Log4jJsonInterceptor(LogParser logParser, String name, String ip, String charset) {
        this.logParser = logParser;
        this.name = name;
        this.ip = ip;
        this.charset = charset;
    }

    @Override
    public void close() {
        logger.debug("close...");
    }

    @Override
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

    @Override
    public Event intercept(Event event) {
        String body;
        try {
            body = new String(event.getBody(), this.charset);
        } catch (UnsupportedEncodingException var5) {
            throw new RuntimeException("unspport charset:" + this.charset);
        }

        if (StringUtils.isNotBlank(body)) {
            LogEntry entry = this.logParser.parseLine(body);
            entry.put("serviceName", this.name);
            entry.put("IP", this.ip);
            /**
             * 转化为json串
             */
            String result = entry.toString();
            event.setBody(result.getBytes());
        }

        return event;
    }

    @Override
    public List<Event> intercept(List<Event> eventList) {
        Iterator var2 = eventList.iterator();

        while(var2.hasNext()) {
            Event event = (Event)var2.next();
            this.intercept(event);
        }

        return eventList;
    }

    public static class Builder implements org.apache.flume.interceptor.Interceptor.Builder {
        LogParser logParser;
        String name;
        String ip;
        String charset;
        int msgMaxLength = 5000;

        public Builder() {
        }

        @Override
        public void configure(Context context) {
            String patternString = context.getString("pattern");
            Preconditions.checkArgument(!StringUtils.isEmpty(patternString), "Must supply a valid pattern string");
            this.name = context.getString("name");
            Preconditions.checkArgument(!StringUtils.isEmpty(this.name), "Must supply a valid name");
            this.ip = context.getString("ip");
            Integer maxLength = context.getInteger("msg-max-length", this.msgMaxLength);
            Log4jJsonInterceptor.logger.debug("configure   name:" + this.name + "\tpattern:" + patternString + "\tip:" + this.ip + "\tmsgMaxLength:" + maxLength);
            this.charset = context.getString("charset");
            this.logParser = new LogParser(patternString);
            this.logParser.setMsgMaxLength(maxLength.intValue());
        }

        @Override
        public Interceptor build() {
            Preconditions.checkArgument(this.logParser != null, "ConversionPattern was misconfigured");
            return new Log4jJsonInterceptor(this.logParser, this.name, this.ip, this.charset);
        }
    }
}
