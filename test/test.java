import java.util.regex.Matcher;
import java.util.regex.Pattern;

class RegexMatches {

    public static void main(String args[]) {
        String str = "";
        String pattern = "%d{ISO8601} %T %c{2} %5p - %m%n";

        Pattern r = Pattern.compile(pattern);
        Matcher m = r.matcher(str);
        System.out.println(m.matches());
    }

}
