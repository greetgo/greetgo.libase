package kz.greetgo.libase.strureader;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.logging.Logger;

import kz.greetgo.libase.model.DbStru;
import kz.greetgo.libase.model.Field;
import kz.greetgo.libase.model.FieldVector;
import kz.greetgo.libase.model.ForeignKey;
import kz.greetgo.libase.model.Relation;
import kz.greetgo.libase.model.Sequence;
import kz.greetgo.libase.model.StoreFunc;
import kz.greetgo.libase.model.Table;
import kz.greetgo.libase.model.Trigger;
import kz.greetgo.libase.model.View;

public class StruReader {
  private static final Logger log = Logger.getLogger(StruReader.class.getName());
  
  public static DbStru read(RowReader rowReader) throws Exception {
    DbStru ret = new DbStru();
    
    long t1 = System.currentTimeMillis();
    List<ColumnRow> allTableColumns = rowReader.readAllTableColumns();
    long t2 = System.currentTimeMillis();
    
    log.info("readAllTableColumns выполнено за " + (t2 - t1) + " мс");
    
    for (ColumnRow colRow : allTableColumns) {
      Table table = (Table)ret.relations.get(colRow.tableName);
      if (table == null) ret.relations.put(colRow.tableName, table = new Table(colRow.tableName));
      table.allFields.add(new Field(table, colRow.name, colRow.type, colRow.nullable,
          colRow.defaultValue));
      
    }
    
    long t3 = System.currentTimeMillis();
    Map<String, PrimaryKeyRow> allTablePrimaryKeys = rowReader.readAllTablePrimaryKeys();
    long t4 = System.currentTimeMillis();
    
    log.info("readAllTablePrimaryKeys выполнено за " + (t4 - t3) + " мс");
    
    for (PrimaryKeyRow pkr : allTablePrimaryKeys.values()) {
      Table table = (Table)ret.relations.get(pkr.tableName);
      if (table == null) throw new NullPointerException("No table " + pkr.tableName);
      for (String fieldName : pkr.keyFieldNames) {
        table.keyFields.add(table.field(fieldName));
      }
    }
    
    long t5 = System.currentTimeMillis();
    Map<String, ForeignKeyRow> allForeignKeys = rowReader.readAllForeignKeys();
    long t6 = System.currentTimeMillis();
    
    log.info("readAllForeignKeys выполнено за " + (t6 - t5) + " мс");
    
    for (ForeignKeyRow fkr : allForeignKeys.values()) {
      Table from = (Table)ret.relations.get(fkr.fromTable);
      if (from == null) throw new NullPointerException("No table " + fkr.fromTable);
      Table to = (Table)ret.relations.get(fkr.toTable);
      if (to == null) throw new NullPointerException("No table " + fkr.toTable);
      
      if (fkr.fromColumns.size() != fkr.toColumns.size()) throw new IllegalArgumentException(
          "Different number of columns " + fkr);
      
      if (fkr.fromColumns.size() <= 0) throw new IllegalArgumentException("No columns " + fkr);
      
      ForeignKey fk = new ForeignKey(from, to);
      
      for (int i = 0, C = fkr.fromColumns.size(); i < C; i++) {
        fk.vectors.add(new FieldVector( //
            from.field(fkr.fromColumns.get(i)), //
            to.field(fkr.toColumns.get(i))));
      }
      
      from.foreignKeys.add(fk);
      ret.foreignKeys.add(fk);
    }
    
    long t7 = System.currentTimeMillis();
    Map<String, SequenceRow> allSequences = rowReader.readAllSequences();
    long t8 = System.currentTimeMillis();
    
    log.info("readAllSequences выполнено за " + (t8 - t7) + " мс");
    
    for (SequenceRow s : allSequences.values()) {
      ret.sequences.add(new Sequence(s.name, s.startFrom));
    }
    
    {
      long t9 = System.currentTimeMillis();
      Map<String, ViewRow> viewRows = rowReader.readAllViews();
      long t10 = System.currentTimeMillis();
      
      log.info("readAllViews выполнено за " + (t10 - t9) + " мс");
      
      for (ViewRow vr : viewRows.values()) {
        if (ret.relations.keySet().contains(vr.name)) {
          throw new IllegalArgumentException("Cannot add view " + vr.name);
        }
        ret.relations.put(vr.name, new View(vr.name, vr.content));
      }
      
      for (ViewRow vr : viewRows.values()) {
        View view = (View)ret.relations.get(vr.name);
        if (view == null) throw new NullPointerException("No view " + vr.name);
        
        for (String depName : vr.dependenses) {
          Relation dep = ret.relations.get(depName);
          if (dep == null) throw new NullPointerException("No relation " + depName);
          view.dependences.add(dep);
        }
      }
    }
    
    {
      long t11 = System.currentTimeMillis();
      List<StoreFuncRow> allFuncs = rowReader.readAllFuncs();
      long t12 = System.currentTimeMillis();
      
      log.info("readAllFuncs выполнено за " + (t12 - t11) + " мс");
      
      for (StoreFuncRow sfr : allFuncs) {
        StoreFunc f = new StoreFunc(sfr.name, sfr.argTypes);
        f.argNames.addAll(sfr.argNames);
        f.returns = sfr.returns;
        f.source = sfr.source;
        f.language = sfr.language;
        ret.funcs.put(f, f);
      }
    }
    
    {
      long t13 = System.currentTimeMillis();
      Map<String, TriggerRow> allTriggers = rowReader.readAllTriggers();
      long t14 = System.currentTimeMillis();
      
      log.info("readAllTriggers выполнено за " + (t14 - t13) + " мс");
      
      for (TriggerRow tr : allTriggers.values()) {
        Trigger t = new Trigger(tr.name, tr.tableName);
        t.eventManipulation = tr.eventManipulation;
        t.actionOrientation = tr.actionOrientation;
        t.actionTiming = tr.actionTiming;
        t.actionStatement = tr.actionStatement;
        ret.triggers.put(t, t);
      }
    }
    
    {
      long t15 = System.currentTimeMillis();
      Map<String, String> tableComments = rowReader.readTableComments();
      long t16 = System.currentTimeMillis();
      
      log.info("readTableComments выполнено за " + (t16 - t15) + " мс");
      
      for (Entry<String, String> e : tableComments.entrySet()) {
        Table table = ret.table(e.getKey());
        if (table != null) {
          table.comment = e.getValue();
        }
      }
    }
    {
      long t17 = System.currentTimeMillis();
      Map<String, String> columnComments = rowReader.readColumnComments();
      long t18 = System.currentTimeMillis();
      
      log.info("readColumnComments выполнено за " + (t18 - t17) + " мс");
      
      for (Entry<String, String> e : columnComments.entrySet()) {
        String[] split = e.getKey().split("\\.");
        Table table = ret.table(split[0]);
        if (table != null) {
          Field field = table.field(split[1]);
          if (field != null) {
            field.comment = e.getValue();
          }
        }
      }
    }
    
    long t19 = System.currentTimeMillis();
    log.info("Чтение всей структуры выполнено за " + (t19 - t1) + " мс");
    
    return ret;
  }
}
