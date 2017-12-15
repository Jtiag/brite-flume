package flume;

import com.google.common.base.Preconditions;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import net.sf.json.JSONObject;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.mortbay.log.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class O2mJsonInterceptor implements Interceptor {
    private static final Logger logger = LoggerFactory.getLogger(Log4jJsonInterceptor.class);
    static final String NAME = "name";
    static final String IP = "ip";
    static final String CHARSET = "charset";
    static final String DATE_PATTERN = "datePattern";
    static final String DEFAULT_DATE_PATTERN = "yyyy-MM-dd HH:mm:ss.SSS";
    String name;
    String ip;
    String charset;
    String dateFormatStr;
    SimpleDateFormat dateFormat;
    DateTimeFormatter utcFormatter;

    private O2mJsonInterceptor(String name, String ip, String charset, String dateFormatStr) {
        this.utcFormatter = ISODateTimeFormat.dateTime().withZoneUTC();
        this.name = name;
        this.ip = ip;
        this.charset = charset;
        this.dateFormatStr = dateFormatStr;
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

        if (StringUtils.isEmpty(this.dateFormatStr)) {
            this.dateFormatStr = "yyyy-MM-dd HH:mm:ss.SSS";
        }

        this.dateFormat = new SimpleDateFormat(this.dateFormatStr);
        if (this.charset == null) {
            this.charset = "UTF-8";
        }

        logger.debug("initialized");
    }

    public Event intercept(Event event) {
        String body;
        try {
            body = new String(event.getBody(), this.charset);
        } catch (UnsupportedEncodingException var11) {
            throw new RuntimeException("unspport charset:" + this.charset);
        }

        Log.debug("intercept event:" + body);
        Date beginTime = null;
        Date endtime = null;
        Long invokeTime = null;
        JSONObject jsonObj = JSONObject.fromObject(body);
        String beginTimeStr = jsonObj.getString("beginTime");
        String endTimeStr = jsonObj.getString("endTime");

        try {
            if (StringUtils.isNotEmpty(beginTimeStr)) {
                beginTime = this.dateFormat.parse(beginTimeStr);
                jsonObj.put("beginTime", this.utcFormatter.print(beginTime.getTime()));
            }

            if (StringUtils.isNotEmpty(endTimeStr)) {
                endtime = this.dateFormat.parse(endTimeStr);
                jsonObj.put("endtime", this.utcFormatter.print(endtime.getTime()));
            }

            if (beginTime != null & endtime != null) {
                invokeTime = endtime.getTime() - beginTime.getTime();
                jsonObj.put("invokeTime", invokeTime);
            }
        } catch (ParseException var10) {
            Log.warn("can't parse begin/end time:" + beginTimeStr + "\t" + endTimeStr);
        }

        jsonObj.put("moduleName", this.name);
        jsonObj.put("IP", this.ip);
        event.setBody(jsonObj.toString().getBytes());
        return event;
    }

    public List<Event> intercept(List<Event> events) {
        Iterator var2 = events.iterator();

        while(var2.hasNext()) {
            Event event = (Event)var2.next();
            this.intercept(event);
        }

        logger.debug("eventList finished:" + events.size());
        return events;
    }

    public void close() {
        logger.debug("close...");
    }

    public static class Builder implements org.apache.flume.interceptor.Interceptor.Builder {
        String name;
        String ip;
        String charset;
        String patternString;

        public Builder() {
        }

        public void configure(Context context) {
            this.patternString = context.getString("datePattern");
            this.name = context.getString("name");
            Preconditions.checkArgument(!StringUtils.isEmpty(this.name), "Must supply a valid name");
            this.ip = context.getString("ip");
            this.charset = context.getString("charset");
            O2mJsonInterceptor.logger.debug("configure   name:" + this.name + "\tdatePattern:" + this.patternString + "\tip:" + this.ip + "\tcharset:" + this.charset);
        }

        public Interceptor build() {
            Preconditions.checkArgument(this.name != null, "name was misconfigured");
            return new O2mJsonInterceptor(this.name, this.ip, this.charset, this.patternString);
        }
    }
}
