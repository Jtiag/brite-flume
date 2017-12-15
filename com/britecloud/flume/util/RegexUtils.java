package flume.util;

import java.text.SimpleDateFormat;
import java.util.BitSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

/**
 * @author Administrator
 */
public class RegexUtils {
    private static transient Logger logger = Logger.getLogger(RegexUtils.class);

    public RegexUtils() {
    }

    public static String getLazySuffix(ConversionRule rule) {
        return rule.isFollowedByQuotedString() ? "?" : "";
    }

    public static String getLengthHint(ConversionRule rule) {
        if (rule.getMaxWidth() > 0 && rule.getMaxWidth() == rule.getMinWidth()) {
            return "{" + rule.getMaxWidth() + "}";
        } else if (rule.getMaxWidth() > 0) {
            return "{" + Math.max(0, rule.getMinWidth()) + "," + rule.getMaxWidth() + "}";
        } else {
            return rule.getMinWidth() > 0 ? "{" + rule.getMinWidth() + ",}" : "";
        }
    }

    public static String getRegexForSimpleDateFormat(String format) throws Exception {
        RegexUtils utils = new RegexUtils();
        return utils.doGetRegexForSimpleDateFormat(format);
    }

    private String doGetRegexForSimpleDateFormat(String format) throws Exception {
        try {
            new SimpleDateFormat(format);
        } catch (Exception var3) {
            throw new Exception(var3);
        }

        RegexUtils.ReplacementContext ctx = new RegexUtils.ReplacementContext();
        ctx.setBits(new BitSet(format.length()));
        ctx.setBuffer(new StringBuffer(format));
        this.unquote(ctx);
        this.replace(ctx, "G+", "[ADBC]{2}");
        this.replace(ctx, "[y]{3,}", "\\d{4}");
        this.replace(ctx, "[y]{2}", "\\d{2}");
        this.replace(ctx, "y", "\\d{4}");
        this.replace(ctx, "[M]{3,}", "[a-zA-Z]*");
        this.replace(ctx, "[M]{2}", "\\d{2}");
        this.replace(ctx, "M", "\\d{1,2}");
        this.replace(ctx, "w+", "\\d{1,2}");
        this.replace(ctx, "W+", "\\d");
        this.replace(ctx, "D+", "\\d{1,3}");
        this.replace(ctx, "d+", "\\d{1,2}");
        this.replace(ctx, "F+", "\\d");
        this.replace(ctx, "E+", "[a-zA-Z]*");
        this.replace(ctx, "a+", "[AMPM]{2}");
        this.replace(ctx, "H+", "\\d{1,2}");
        this.replace(ctx, "k+", "\\d{1,2}");
        this.replace(ctx, "K+", "\\d{1,2}");
        this.replace(ctx, "h+", "\\d{1,2}");
        this.replace(ctx, "m+", "\\d{1,2}");
        this.replace(ctx, "s+", "\\d{1,2}");
        this.replace(ctx, "S+", "\\d{1,3}");
        this.replace(ctx, "z+", "[a-zA-Z-+:0-9]*");
        this.replace(ctx, "Z+", "[-+]\\d{4}");
        return ctx.getBuffer().toString();
    }

    private void unquote(RegexUtils.ReplacementContext ctx) {
        Pattern p = Pattern.compile("'[^']+'");
        Matcher m = p.matcher(ctx.getBuffer().toString());

        byte offset;
        int i;
        while (m.find()) {
            logger.trace(ctx.toString());
            offset = -2;

            for (i = m.end(); i < ctx.getBuffer().length(); ++i) {
                ctx.getBits().set(i + offset, ctx.getBits().get(i));
            }

            for (i = m.start(); i < m.end() + offset; ++i) {
                ctx.getBits().set(i);
            }

            ctx.getBuffer().replace(m.start(), m.start() + 1, "");
            ctx.getBuffer().replace(m.end() - 2, m.end() - 1, "");
            logger.trace(ctx.toString());
        }

        p = Pattern.compile("''");
        m = p.matcher(ctx.getBuffer().toString());

        while (m.find()) {
            logger.trace(ctx.toString());
            offset = -1;

            for (i = m.end(); i < ctx.getBuffer().length(); ++i) {
                ctx.getBits().set(i + offset, ctx.getBits().get(i));
            }

            for (i = m.start(); i < m.end() + offset; ++i) {
                ctx.getBits().set(i);
            }

            ctx.getBuffer().replace(m.start(), m.start() + 1, "");
            logger.trace(ctx.toString());
        }

    }

    private void replace(RegexUtils.ReplacementContext ctx, String regex, String replacement) {
        Pattern p = Pattern.compile(regex);
        Matcher m = p.matcher(ctx.getBuffer().toString());

        while (true) {
            int idx;
            do {
                if (!m.find()) {
                    return;
                }

                logger.trace(regex);
                logger.trace(ctx.toString());
                idx = ctx.getBits().nextSetBit(m.start());
            } while (idx != -1 && idx <= m.end() - 1);

            int len = m.end() - m.start();
            int offset = replacement.length() - len;
            int i;
            if (offset > 0) {
                for (i = ctx.getBuffer().length() - 1; i > m.end(); --i) {
                    ctx.getBits().set(i + offset, ctx.getBits().get(i));
                }
            } else if (offset < 0) {
                for (i = m.end(); i < ctx.getBuffer().length(); ++i) {
                    ctx.getBits().set(i + offset, ctx.getBits().get(i));
                }
            }

            for (i = m.start(); i < m.end() + offset; ++i) {
                ctx.getBits().set(i);
            }

            ctx.getBuffer().replace(m.start(), m.end(), replacement);
            logger.trace(ctx.toString());
        }
    }

    private class ReplacementContext {
        private BitSet bits;
        private StringBuffer buffer;

        private ReplacementContext() {
        }

        public BitSet getBits() {
            return this.bits;
        }

        public void setBits(BitSet bits) {
            this.bits = bits;
        }

        public StringBuffer getBuffer() {
            return this.buffer;
        }

        public void setBuffer(StringBuffer buffer) {
            this.buffer = buffer;
        }

        public String toString() {
            StringBuffer sb = new StringBuffer();
            sb.append("ReplacementContext [bits=");

            for (int i = 0; i < this.buffer.length(); ++i) {
                sb.append((char) (this.bits.get(i) ? '1' : '0'));
            }

            sb.append(", buffer=");
            sb.append(this.buffer);
            sb.append(']');
            return sb.toString();
        }
    }
}
