package kz.greetgo.libase.util;

public class StrUtil {

  public static String nns(String str) {
    if (str == null) return "";
    return str;
  }

  public static boolean def(String str) {
    if (str == null) return false;
    return str.trim().length() > 0;
  }

  public static String sizeToStr(int size, int scale) {
    if (size <= 0) {
      return "";
    }
    if (scale <= 0) {
      return "(" + size + ")";
    }
    return "(" + size + ", " + scale + ")";
  }

  public static String killSemicolonInEnd(String str) {
    if (str == null) return null;
    str = str.trim();
    if (str.endsWith(";")) return str.substring(0, str.length() - 1).trim();
    return str;
  }

}
