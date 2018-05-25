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
    return name + ' ' + __argTypesStr + ' ' + __argNamesStr +
      "\n    returns " + __returns +
      "\n    langId " + __langId +
      "\n    ::: " + argTypes + " " + argNames + "" +
      "\n    ::: returns " + returns +
      "\n    ::: language " + language +
      "\n    source {" + source.replaceAll("\n", " ") + '}';
  }
}
