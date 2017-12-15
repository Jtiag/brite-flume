package flume.util;

import org.apache.commons.lang.StringUtils;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class MysqlLogParser {
    public static final String FIELD_TIMESTAMP = "timestamp";
    public static final String FIELD_LEVEL = "level";
    public static final String FIELD_MESSAGE = "message";
    private String dateParten = "yyyy-MM-dd hh:mm:ss,yyMMdd hh:mm:ss,yyMMdd h:m:s";
    private SimpleDateFormat[] formaters;
    private DateTimeFormatter dtFormatter = ISODateTimeFormat.dateTime().withZoneUTC();

    public MysqlLogParser() {
        this.init();
    }

    public MysqlLogParser(String dateParten) {
        this.dateParten = dateParten;
        this.init();
    }

    protected void init() {
        String[] partenArr = this.dateParten.split(",");
        this.formaters = new SimpleDateFormat[partenArr.length];

        for (int i = 0; i < partenArr.length; ++i) {
            this.formaters[i] = new SimpleDateFormat(partenArr[i]);
        }

    }

    public LogEntry parseLog(String line) {
        if (StringUtils.isEmpty(line)) {
            return null;
        } else {
            LogEntry entry = new LogEntry();
            int startBrackets = line.indexOf(91);
            int endBracksets = line.indexOf(93);
            String tsStr;
            if (startBrackets >= 0 && endBracksets >= 0) {
                String level = line.substring(startBrackets + 1, endBracksets).trim();
                entry.put("level", level);
                tsStr = line.substring(endBracksets + 1).trim();
                entry.put("message", tsStr);
            }

            String[] tempArr = line.split(" ");
            if (tempArr.length > 1) {
                tsStr = tempArr[0] + " " + tempArr[1];
                Date d = this.parseTimeStamp(tsStr);
                if (d != null) {
                    entry.put("timestamp", this.dtFormatter.print(d.getTime()));
                    if (entry.get("message") == null) {
                        StringBuffer sb = new StringBuffer();

                        for (int i = 2; i < tempArr.length; ++i) {
                            sb.append(tempArr[i]).append(" ");
                        }

                        entry.put("message", sb.toString());
                    }
                } else {
                    entry.put("message", line);
                }
            } else {
                entry.put("message", line);
            }

            return entry;
        }
    }

    private Date parseTimeStamp(String tsStr) {
        int i = 0;

        while (i < this.formaters.length) {
            try {
                Date d = this.formaters[i].parse(tsStr);
                return d;
            } catch (ParseException var4) {
                ++i;
            }
        }

        return null;
    }

    public static void main(String[] args) throws ParseException {
        String ln1 = "2015-11-06 11:44:21 2330 [Note] Server socket created on IP: '::'.";
        String ln2 = "2015-11-06 11:44:21 2330 [Note] Event Scheduler: Loaded 0 events";
        String ln3 = "151027 15:19:44 mysqld_safe mysqld from pid file /home/mysql/run/mysqld.pid ended";
        String ln4 = "Version: '5.6.21-log'  socket: '/home/mysql/run/mysql.sock'  port: 3306  MySQL Community Server (GPL)";
        String ln5 = "151104 08:54:30 mysqld_safe Starting mysqld daemon with databases from /home/mariadb/data";
        String ln6 = "151104  8:54:30 [Note] /home/mariadb/product/5.5/mariadb-1/bin/mysqld (mysqld 5.5.45-MariaDB) starting as process 22934 ...";
        MysqlLogParser p = new MysqlLogParser("yyyy-MM-dd HH:mm:ss,yymmdd HH:mm:ss");
        System.out.println(p.parseLog(ln1));
        System.out.println(p.parseLog(ln2));
        System.out.println(p.parseLog(ln3));
        System.out.println(p.parseLog(ln4));
        System.out.println(p.parseLog(ln5));
        System.out.println(p.parseLog(ln6));
    }
}
