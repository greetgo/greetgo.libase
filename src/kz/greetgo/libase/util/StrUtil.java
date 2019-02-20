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
  
}
