package kz.greetgo.libase.changes;

import kz.greetgo.libase.model.Relation;
import kz.greetgo.libase.model.Table;
import kz.greetgo.libase.model.View;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ChangeComparator implements Comparator<Change>, Serializable {
  @Override
  public int compare(Change o1, Change o2) {

    {
      int c1 = typeCompareFactor(o1);
      int c2 = typeCompareFactor(o2);
      if (c1 != c2) return c1 - c2;
    }

    {
      View v1 = takeView(o1);
      View v2 = takeView(o2);
      if (v1 != null && v2 != null) {

        Set<String> v1deps = getViewDepends(v1);
        Set<String> v2deps = getViewDepends(v2);

        boolean v1dep2 = v1deps.contains(v2.name);
        boolean v2dep1 = v2deps.contains(v1.name);

        if (v1dep2 && !v2dep1) return +1;
        if (v2dep1 && !v1dep2) return -1;

        return depsName(v1).compareTo(depsName(v2));
      }
    }

    return 0;
  }

  final Map<String, String> depsNameCache = new HashMap<>();

  private String depsName(View v) {
    {
      String ret = depsNameCache.get(v.name);
      if (ret != null) return ret;
    }
    {
      List<String> list = new ArrayList<>(getViewDepends(v));
      Collections.sort(list);
      StringBuilder sb = new StringBuilder();
      for (String name : list) {
        sb.append(name).append('_');
      }
      sb.append(v.name);
      String ret = sb.toString();
      depsNameCache.put(v.name, ret);
      return ret;
    }
  }

  final Map<String, Set<String>> viewDependsCache = new HashMap<>();

  private Set<String> getViewDepends(View view) {

    {
      Set<String> ret = viewDependsCache.get(view.name);
      if (ret != null) return ret;
    }

    {
      Set<String> ret = new HashSet<>();
      addAllViewDepends(ret, view);
      ret.remove(view.name);
      viewDependsCache.put(view.name, ret);
      return ret;
    }

  }

  private static void addAllViewDepends(Set<String> depends, View view) {
    if (depends.contains(view.name)) return;
    depends.add(view.name);
    for (Relation r : view.dependences) {
      if (r instanceof View) addAllViewDepends(depends, (View) r);
    }
  }

  private static View takeView(Change o) {
    if (!(o instanceof CreateRelation)) return null;
    CreateRelation cr = (CreateRelation) o;
    if (cr.relation instanceof View) return (View) cr.relation;
    return null;
  }

  private static int typeCompareFactor(Change o) {
    if (o instanceof AddTableField) return 1;
    if (o instanceof AlterField) return 2;
    if (o instanceof CreateRelation) {
      CreateRelation cr = (CreateRelation) o;
      if (cr.relation instanceof Table) return 3;
      if (cr.relation instanceof View) return 4;
      return 5;
    }
    if (o instanceof TableComment) return 6;
    if (o instanceof FieldComment) return 7;
    return 8;
  }
}
