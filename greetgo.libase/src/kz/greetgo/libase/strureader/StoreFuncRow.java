package kz.greetgo.libase.strureader;

import java.util.ArrayList;
import java.util.List;

public class StoreFuncRow {
  public String name;
  public final List<String> argTypes = new ArrayList<>();
  public final List<String> argNames = new ArrayList<>();
  public String returns;
  public String source, language;
  
  public String __argTypesStr;
  public String __argNamesStr;
  public String __returns;
  public String __langId;
  
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(name).append(' ').append(__argTypesStr).append(' ').append(__argNamesStr);
    sb.append("\n    returns ").append(__returns);
    sb.append("\n    langId ").append(__langId);
    sb.append("\n    ::: ").append(argTypes).append(" ").append(argNames).append("");
    sb.append("\n    ::: returns ").append(returns);
    sb.append("\n    ::: language ").append(language);
    sb.append("\n    source {").append(source.replaceAll("\n", " ")).append('}');
    
    return sb.toString();
  }
}
