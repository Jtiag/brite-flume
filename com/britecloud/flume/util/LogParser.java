package flume.util;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.apache.log4j.Logger;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import java.io.File;
import java.io.FileInputStream;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.io.File;
import java.io.FileInputStream;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.apache.log4j.Logger;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

/**
 * @author Administrator
 */
public class LogParser {
    public static final String FIELD_LOGGER = "logger";
    public static final String FIELD_TIMESTAMP = "timestamp";
    public static final String FIELD_LEVEL = "level";
    public static final String FIELD_THREAD = "thread";
    public static final String FIELD_MESSAGE = "message";
    public static final String FIELD_CURRENT_LINE = "currLine";
    public static final String FIELD_IS_HIT = "isHit";
    public static final String FIELD_NDC = "ndc";
    public static final String FIELD_THROWABLE = "throwable";
    public static final String FIELD_LOC_FILENAME = "locFilename";
    public static final String FIELD_LOC_CLASS = "locClass";
    public static final String FIELD_LOC_METHOD = "locMethod";
    public static final String FIELD_LOC_LINE = "locLine";
    private static Logger logger = Logger.getLogger(LogParser.class);
    private ConversionRuleParser conversionRuleParser = new ConversionRuleParser();
    private List<ConversionRule> extractRules;
    private Pattern pattern;
    private long lineNo = 1L;
    private int msgMaxLength = 1024;
    private DateTimeFormatter dtFormatter = ISODateTimeFormat.dateTime().withZoneUTC();

    public int getMsgMaxLength() {
        return this.msgMaxLength;
    }

    public void setMsgMaxLength(int msgMaxLength) {
        this.msgMaxLength = msgMaxLength;
    }

    public LogParser(String conversionPattern) {
        try {
            this.extractRules = this.conversionRuleParser.extractRules(conversionPattern);
            this.pattern = this.conversionRuleParser.getInternalPattern(conversionPattern);
        } catch (Exception var3) {
            throw new RuntimeException("Error on extract rules:" + conversionPattern, var3);
        }
    }

    public List<LogEntry> parse(String fileName) throws Exception {
        FileInputStream fis = new FileInputStream(new File(fileName));
        List<LogEntry> logList = new ArrayList();
        LineIterator iter = IOUtils.lineIterator(fis, "GBK");

        ArrayList var10;
        try {
            while (iter.hasNext()) {
                String line = iter.next();
                LogEntry currentEntry = this.parseLine(line);
                if (currentEntry != null) {
                    logList.add(currentEntry);
                }
            }

            var10 = (ArrayList) logList;
        } finally {
            LineIterator.closeQuietly(iter);
        }

        return var10;
    }

    public LogEntry parseLine(String line) {
        LogEntry entry = new LogEntry();
        Matcher m = this.pattern.matcher(line);
        if (!m.find()) {
            String msg = this.sustring(line);
            entry.put("message", msg);
            return entry;
        } else {
            for (int i = 0; i < m.groupCount(); ++i) {
                try {
                    this.extractField(entry, this.extractRules.get(i), m.group(i + 1));
                } catch (Exception var6) {
                    logger.warn(var6);
                }
            }

            entry.put("currLine", this.lineNo++);
            return entry;
        }
    }

    private String sustring(String str) {
        if (str == null) {
            return "";
        } else {
            String result = str.trim();
            return result.length() > this.msgMaxLength ? result.substring(0, this.msgMaxLength) + " ..." : result;
        }
    }

    private void extractField(LogEntry entry, ConversionRule rule, String val) throws Exception {
        if (rule.getPlaceholderName().equals("d")) {
            DateFormat df = rule.getProperty("dateFormat", DateFormat.class);
            Date d = df.parse(val.trim());
            entry.put("timestamp", this.dtFormatter.print(d.getTime()));
        } else if (rule.getPlaceholderName().equals("p")) {
            entry.put("level", val.trim());
        } else if (rule.getPlaceholderName().equals("c")) {
            entry.put("logger", val.trim());
        } else if (rule.getPlaceholderName().equals("t")) {
            entry.put("thread", val.trim());
        } else if (rule.getPlaceholderName().equals("m")) {
            String msg = this.sustring(val);
            entry.put("message", msg);
        } else if (rule.getPlaceholderName().equals("F")) {
            entry.put("locFilename", val.trim());
        } else if (rule.getPlaceholderName().equals("C")) {
            entry.put("locClass", val.trim());
        } else if (rule.getPlaceholderName().equals("M")) {
            entry.put("locMethod", val.trim());
        } else if (rule.getPlaceholderName().equals("L")) {
            entry.put("locLine", val.trim());
        } else {
            if (!rule.getPlaceholderName().equals("x")) {
                throw new Exception("异常消息暂未设置");
            }

            entry.put("ndc", val.trim());
        }

    }

    public static void main(String[] args) throws Exception {
        System.out.println(ISODateTimeFormat.dateTime().print(System.currentTimeMillis()));
        String pattern = "%d{ISO8601} %p %c{2}: %m%n";
        LogParser p = new LogParser(pattern);
        String line = "2015-11-16 14:14:37,458 INFO org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.FsDatasetAsyncDiskService: Deleted BP-154267003-10.128.11.36-1441951366325 blk_1073748051_7230 file /home/hadoop/storage/dfs/data/current/BP-154267003-10.128.11.36-1441951366325/current/finalized/subdir0/subdir24/blk_1073748051";
        LogEntry log = p.parseLine(line);
        System.out.println(log.toString());
        String logFilePath = "d:/realtime.log";
    }
}
