package kz.greetgo.libase.changes;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import kz.greetgo.libase.model.DbStru;
import kz.greetgo.libase.model.Field;
import kz.greetgo.libase.model.ForeignKey;
import kz.greetgo.libase.model.Relation;
import kz.greetgo.libase.model.Sequence;
import kz.greetgo.libase.model.StoreFunc;
import kz.greetgo.libase.model.Table;
import kz.greetgo.libase.model.Trigger;
import kz.greetgo.libase.model.View;

public class Comparer {
  public static List<Change> compare(DbStru to, DbStru from) {
    List<Change> changeList = new ArrayList<>();
    
    for (Relation relationFrom : from.relations.values()) {
      Relation relationTo = to.relations.get(relationFrom.name);
      if (relationTo == null) {
        changeList.add(new CreateRelation(relationFrom));
        continue;
      }
      
      addRelationModify(changeList, relationFrom, relationTo);
    }
    
    for (ForeignKey fk : from.foreignKeys) {
      if (to.foreignKeys.contains(fk)) continue;
      changeList.add(new CreateForeignKey(fk));
    }
    
    for (Sequence sequence : from.sequences) {
      if (to.sequences.contains(sequence)) continue;
      changeList.add(new CreateSequence(sequence));
    }
    
    for (StoreFunc fromFunc : from.funcs.values()) {
      StoreFunc toFunc = to.funcs.get(fromFunc);
      if (!fromFunc.fullEquals(toFunc)) {
        changeList.add(new CreateOrReplaceFunc(fromFunc));
      }
    }
    
    for (Trigger fromTrigger : from.triggers.values()) {
      Trigger toTrigger = to.triggers.get(fromTrigger);
      if (toTrigger == null) {
        changeList.add(new CreateTrigger(fromTrigger));
      } else if (!fromTrigger.fullEquals(toTrigger)) {
        changeList.add(new DropCreateTrigger(fromTrigger));
      }
    }
    
    for (Relation relationFrom : from.relations.values()) {
      if (!(relationFrom instanceof Table)) continue;
      Table tableFrom = (Table)relationFrom;
      Table tableTo = to.table(tableFrom.name);
      if (tableTo == null) {
        if (tableFrom.trimComment().length() > 0) {
          changeList.add(new TableComment(tableFrom));
        }
        for (Field fieldFrom : tableFrom.allFields) {
          if (fieldFrom.comment != null) {
            changeList.add(new FieldComment(tableFrom, fieldFrom));
          }
        }
        continue;
      }
      if (tableFrom.trimComment().length() > 0
          && !tableTo.trimComment().equals(tableFrom.trimComment())) {
        changeList.add(new TableComment(tableFrom));
      }
      for (Field fieldFrom : tableFrom.allFields) {
        Field fieldTo = tableTo.field(fieldFrom.name);
        if (fieldTo == null || fieldFrom.trimComment().length() > 0
            && !fieldTo.trimComment().equals(fieldFrom.trimComment())) {
          changeList.add(new FieldComment(tableFrom, fieldFrom));
        }
      }
    }
    
    return changeList;
  }
  
  private static void addRelationModify(List<Change> changeList, //
      Relation relationFrom, Relation relationTo) {
    
    if (relationFrom instanceof Table && relationTo instanceof Table) {
      Table tableFrom = (Table)relationFrom;
      Table tableTo = (Table)relationTo;
      addTableModify(changeList, tableTo, tableFrom);
      return;
    }
    
    if (relationFrom instanceof View && relationTo instanceof View) {
      View viewFrom = (View)relationFrom;
      View viewTo = (View)relationTo;
      if (!Objects.equals(viewFrom.content, viewTo.content)) {
        changeList.add(new CreateOrReplaceView(viewFrom));
      }
      return;
    }
    
    throw new IllegalArgumentException("Cannot change from " + relationFrom + " to " + relationTo);
  }
  
  private static void addTableModify(List<Change> changeList, Table tableTo, Table tableFrom) {
    for (Field fieldFrom : tableFrom.allFields) {
      Field fieldTo = tableTo.field(fieldFrom.name);
      if (fieldTo == null) {
        changeList.add(new AddTableField(fieldFrom));
        continue;
      }
      addFieldModify(changeList, fieldTo, fieldFrom);
    }
  }
  
  private static void addFieldModify(List<Change> changeList, Field fieldTo, Field fieldFrom) {
    Set<AlterPartPart> alters = new HashSet<>();
    
    if (fieldTo.nullable != fieldFrom.nullable) {
      alters.add(AlterPartPart.NOT_NULL);
    }
    if (!Objects.equals(fieldTo.type, fieldFrom.type)) {
      alters.add(AlterPartPart.TYPE);
    }
    if (!Objects.equals(fieldTo.defaultValue, fieldFrom.defaultValue)) {
      alters.add(AlterPartPart.DEFAULT);
    }
    
    if (alters.size() == 0) return;
    
    changeList.add(new AlterField(fieldFrom, alters));
  }
  
  private static class CMP implements Comparator<Change>, Serializable {
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
        List<String> list = new ArrayList<>();
        list.addAll(getViewDepends(v));
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
    
    private void addAllViewDepends(Set<String> depends, View view) {
      if (depends.contains(view.name)) return;
      depends.add(view.name);
      for (Relation r : view.dependences) {
        if (r instanceof View) addAllViewDepends(depends, (View)r);
      }
    }
    
    private View takeView(Change o) {
      if (!(o instanceof CreateRelation)) return null;
      CreateRelation cr = (CreateRelation)o;
      if (cr.relation instanceof View) return (View)cr.relation;
      return null;
    }
    
    private int typeCompareFactor(Change o) {
      if (o instanceof AddTableField) return 1;
      if (o instanceof AlterField) return 2;
      if (o instanceof CreateRelation) {
        CreateRelation cr = (CreateRelation)o;
        if (cr.relation instanceof Table) return 3;
        if (cr.relation instanceof View) return 4;
        return 5;
      }
      if (o instanceof TableComment) return 6;
      if (o instanceof FieldComment) return 7;
      return 8;
    }
  }
  
  public static void sort(List<Change> changes) {
    Collections.sort(changes, new CMP());
  }
  
}
