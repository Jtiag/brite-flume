package flume.util;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author Administrator
 */
public class ConversionRuleParser {
    private static final Pattern EXTRACTION_PATTERN = Pattern.compile("%(-?(\\d+))?(\\.(\\d+))?([a-zA-Z])(\\{([^\\}]+)\\})?");
    public static final String PROP_DATEFORMAT = "dateFormat";

    public ConversionRuleParser() {
    }

    protected Pattern getInternalPattern(String externalPattern) throws Exception {
        List<ConversionRule> rules = this.extractRules(externalPattern);
        return Pattern.compile(this.toRegexPattern(this.prepare(externalPattern), rules));
    }

    protected List<ConversionRule> extractRules(String externalPattern) throws Exception {
        externalPattern = this.prepare(externalPattern);
        Matcher m = EXTRACTION_PATTERN.matcher(externalPattern);
        ArrayList ret = new ArrayList();

        while (m.find()) {
            String minWidthModifier = m.group(2);
            String maxWidthModifier = m.group(4);
            String conversionName = m.group(5);
            String conversionModifier = m.group(7);
            int minWidth = -1;
            if (minWidthModifier != null && minWidthModifier.length() > 0) {
                minWidth = Integer.parseInt(minWidthModifier);
            }

            int maxWidth = -1;
            if (maxWidthModifier != null && maxWidthModifier.length() > 0) {
                maxWidth = Integer.parseInt(maxWidthModifier);
            }

            ConversionRule rule = new ConversionRule();
            rule.setBeginIndex(m.start());
            rule.setLength(m.end() - m.start());
            rule.setMaxWidth(maxWidth);
            rule.setMinWidth(minWidth);
            rule.setPlaceholderName(conversionName);
            rule.setModifier(conversionModifier);
            this.rewrite(rule);
            ret.add(rule);
        }

        return ret;
    }

    public String prepare(String externalPattern) throws Exception {
        if (!externalPattern.endsWith("%n")) {
            return externalPattern;
        } else {
            externalPattern = externalPattern.substring(0, externalPattern.length() - 2);
            if (externalPattern.contains("%n")) {
                throw new Exception("ConversionPattern不合法!");
            } else {
                return externalPattern;
            }
        }
    }

    private void rewrite(ConversionRule rule) throws Exception {
        if (rule.getPlaceholderName().equals("d")) {
            this.applyDefaults(rule);
            if (rule.getModifier().equals("ABSOLUTE")) {
                rule.setModifier("HH:mm:ss,SSS");
            } else if (rule.getModifier().equals("DATE")) {
                rule.setModifier("dd MMM yyyy HH:mm:ss,SSS");
            } else if (rule.getModifier().equals("ISO8601")) {
                rule.setModifier("yyyy-MM-dd HH:mm:ss,SSS");
            }

            try {
                rule.putProperty("dateFormat", new SimpleDateFormat(rule.getModifier()));
            } catch (IllegalArgumentException var3) {
                throw new Exception(var3);
            }
        }

    }

    private void applyDefaults(ConversionRule rule) throws Exception {
        if (rule.getModifier() == null) {
            rule.setModifier("ISO8601");
        }

    }

    private String getRegexPatternForRule(ConversionRule rule) throws Exception {
        if (rule.getPlaceholderName().equals("d")) {
            return "(" + RegexUtils.getRegexForSimpleDateFormat(rule.getModifier()) + ")";
        } else if (rule.getPlaceholderName().equals("p")) {
            String lnHint = RegexUtils.getLengthHint(rule);
            return lnHint.length() > 0 ? "([ A-Z]" + lnHint + ")" : "([A-Z]{4,5})";
        } else if (rule.getPlaceholderName().equals("c")) {
            return "(.*" + RegexUtils.getLengthHint(rule) + RegexUtils.getLazySuffix(rule) + ")";
        } else if (rule.getPlaceholderName().equals("t")) {
            return "(.*" + RegexUtils.getLengthHint(rule) + RegexUtils.getLazySuffix(rule) + ")";
        } else if (rule.getPlaceholderName().equals("m")) {
            return "(.*" + RegexUtils.getLengthHint(rule) + RegexUtils.getLazySuffix(rule) + ")";
        } else if (rule.getPlaceholderName().equals("F")) {
            return "(.*" + RegexUtils.getLengthHint(rule) + RegexUtils.getLazySuffix(rule) + ")";
        } else if (rule.getPlaceholderName().equals("C")) {
            return "(.*" + RegexUtils.getLengthHint(rule) + RegexUtils.getLazySuffix(rule) + ")";
        } else if (rule.getPlaceholderName().equals("M")) {
            return "(.*" + RegexUtils.getLengthHint(rule) + RegexUtils.getLazySuffix(rule) + ")";
        } else if (rule.getPlaceholderName().equals("L")) {
            return "([0-9]*" + RegexUtils.getLengthHint(rule) + ")";
        } else if (rule.getPlaceholderName().equals("x")) {
            return "(.*" + RegexUtils.getLengthHint(rule) + RegexUtils.getLazySuffix(rule) + ")";
        } else {
            throw new Exception("无法找到对应的表达式描述!");
        }
    }

    protected String toRegexPattern(String externalPattern, List<ConversionRule> rules) throws Exception {
        int idx = 0;
        ConversionRule prevRule = null;

        ConversionRule rule;
        for (Iterator var5 = rules.iterator(); var5.hasNext(); prevRule = rule) {
            rule = (ConversionRule) var5.next();
            if (rule.getBeginIndex() > idx && prevRule != null) {
                prevRule.setFollowedByQuotedString(true);
            }

            idx = rule.getBeginIndex();
            idx += rule.getLength();
        }

        if (externalPattern.length() > idx && prevRule != null) {
            prevRule.setFollowedByQuotedString(true);
        }

        StringBuilder sb = new StringBuilder();
        idx = 0;
        for (Iterator var10 = rules.iterator(); var10.hasNext(); idx += rule.getLength()) {
            rule = (ConversionRule) var10.next();
            if (rule.getBeginIndex() > idx) {
                sb.append(Pattern.quote(externalPattern.substring(idx, rule.getBeginIndex())));
            }

            idx = rule.getBeginIndex();
            String regex = this.getRegexPatternForRule(rule);
            sb.append(regex);
        }

        if (externalPattern.length() > idx) {
            sb.append(Pattern.quote(externalPattern.substring(idx)));
        }

        return sb.toString();
    }
}
