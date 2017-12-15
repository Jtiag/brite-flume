package flume.util;

import java.util.HashMap;
import java.util.Map;

public final class ConversionRule {
    private boolean followedByQuotedString;
    private int beginIndex;
    private int length;
    private int minWidth = -1;
    private int maxWidth = -1;
    /**
     * 匹配的表达式 如 c,d
     */
    private String placeholderName;
    /**
     * 指定层数的命名空间等
     */
    private String modifier;
    private Map<String, Object> properties = new HashMap();

    public ConversionRule() {
    }

    public boolean isFollowedByQuotedString() {
        return this.followedByQuotedString;
    }

    public void setFollowedByQuotedString(boolean followedByQuotedString) {
        this.followedByQuotedString = followedByQuotedString;
    }

    public int getBeginIndex() {
        return this.beginIndex;
    }

    public void setBeginIndex(int beginIndex) {
        this.beginIndex = beginIndex;
    }

    public int getLength() {
        return this.length;
    }

    public void setLength(int length) {
        this.length = length;
    }

    public int getMinWidth() {
        return this.minWidth;
    }

    public void setMinWidth(int minWidth) {
        this.minWidth = minWidth;
    }

    public int getMaxWidth() {
        return this.maxWidth;
    }

    public void setMaxWidth(int maxWidth) {
        this.maxWidth = maxWidth;
    }

    public String getPlaceholderName() {
        return this.placeholderName;
    }

    public void setPlaceholderName(String placeholderName) {
        this.placeholderName = placeholderName;
    }

    public String getModifier() {
        return this.modifier;
    }

    public void setModifier(String modifier) {
        this.modifier = modifier;
    }

    public void putProperty(String key, Object value) {
        this.properties.put(key, value);
    }

    public <T> T getProperty(String key, Class<T> clazz) {
        return (T) this.properties.get(key);
    }

    @Override
    public String toString() {
        return "ConversionRule [modifier=" + this.modifier + ", placeholderName=" + this.placeholderName + "]";
    }
}
