package kz.greetgo.libase.changesql;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import kz.greetgo.libase.changes.AddTableField;
import kz.greetgo.libase.changes.AlterField;
import kz.greetgo.libase.changes.AlterPartPart;
import kz.greetgo.libase.changes.Change;
import kz.greetgo.libase.changes.CreateForeignKey;
import kz.greetgo.libase.changes.CreateOrReplaceFunc;
import kz.greetgo.libase.changes.CreateOrReplaceView;
import kz.greetgo.libase.changes.CreateRelation;
import kz.greetgo.libase.changes.CreateSequence;
import kz.greetgo.libase.changes.CreateTrigger;
import kz.greetgo.libase.changes.DropCreateTrigger;
import kz.greetgo.libase.changes.FieldComment;
import kz.greetgo.libase.changes.TableComment;
import kz.greetgo.libase.model.Field;
import kz.greetgo.libase.model.FieldVector;
import kz.greetgo.libase.model.ForeignKey;
import kz.greetgo.libase.model.Relation;
import kz.greetgo.libase.model.Sequence;
import kz.greetgo.libase.model.StoreFunc;
import kz.greetgo.libase.model.Table;
import kz.greetgo.libase.model.Trigger;
import kz.greetgo.libase.model.View;

public class SqlGeneratorOracle implements SqlGenerator {
  
  @Override
  public void generate(List<String> sqlResult, List<Change> changes) {
    for (Change change : changes) {
      
      if (change instanceof DropCreateTrigger) {
        generateDropCreateTrigger(sqlResult, ((DropCreateTrigger)change).trigger);
        continue;
      }
      
      addGeneratedChanges(sqlResult, change);
    }
  }
  
  private void addGeneratedChanges(List<String> res, Change change) {
    if (change instanceof CreateRelation) {
      res.add(generateCreateRelation(((CreateRelation)change).relation));
      return;
    }
    
    if (change instanceof AddTableField) {
      res.add(generateAddTableField(((AddTableField)change).field));
      return;
    }
    if (change instanceof AlterField) {
      addGeneratedAlterField(res, (AlterField)change);
      return;
    }
    if (change instanceof CreateOrReplaceView) {
      res.add(generateCreateOrReplaceView(((CreateOrReplaceView)change).view));
      return;
    }
    if (change instanceof CreateForeignKey) {
      res.add(generateCreateForeignKey(((CreateForeignKey)change).foreignKey));
      return;
    }
    if (change instanceof CreateSequence) {
      res.add(generateCreateSequence(((CreateSequence)change).sequence));
      return;
    }
    if (change instanceof CreateOrReplaceFunc) {
      res.add(generateCreateOrReplaceFunc(((CreateOrReplaceFunc)change).storeFunc));
      return;
    }
    if (change instanceof CreateTrigger) {
      res.add(generateCreateTrigger(((CreateTrigger)change).trigger));
      return;
    }
    if (change instanceof TableComment) {
      TableComment x = (TableComment)change;
      if (x.table.comment != null) {
        res.add(GeneratorAddon.generateTableComment(x.table));
      }
      return;
    }
    if (change instanceof FieldComment) {
      FieldComment x = (FieldComment)change;
      if (x.field.comment != null) {
        res.add(GeneratorAddon.generateFieldComment(x.table, x.field));
      }
      return;
    }
    
    throw new IllegalArgumentException("Unknown change " + change);
  }
  
  private String generateCreateRelation(Relation relation) {
    if (relation instanceof Table) {
      return generateCreateTable((Table)relation);
    }
    
    if (relation instanceof View) {
      return generateCreateView((View)relation);
    }
    
    throw new IllegalArgumentException("Unknown relation " + relation);
  }
  
  private String generateCreateTable(Table table) {
    StringBuilder sb = new StringBuilder();
    sb.append("create table ").append(table.name).append("(");
    for (Field field : table.allFields) {
      sb.append(field.name).append(' ').append(field.type);
      if (field.defaultValue != null) sb.append(" default ").append(field.defaultValue);
      if (!field.nullable) sb.append(" not null");
      sb.append(", ");
    }
    if (table.keyFields.size() == 0) {
      sb.setLength(sb.length() - 2);
    } else {
      sb.append(" primary key (");
      for (Field key : table.keyFields) {
        sb.append(key.name).append(", ");
      }
      sb.setLength(sb.length() - 2);
      sb.append(')');
    }
    sb.append(')');
    return sb.toString();
  }
  
  private String generateCreateView(View view) {
    return "create view " + view.name + " as " + view.content;
  }
  
  private String generateAddTableField(Field field) {
    StringBuilder sb = new StringBuilder();
    sb.append("alter table " + field.owner.name + " add (" + field.name + ' ' + field.type);
    
    if (field.defaultValue != null) sb.append(" default ").append(field.defaultValue);
    if (!field.nullable) sb.append(" not null");
    sb.append(')');
    return sb.toString();
  }
  
  private String addGeneratedAlterField(List<String> res, AlterField alter) {
    StringBuilder sb = new StringBuilder();
    sb.append("alter table ").append(alter.field.owner.name);
    sb.append(" alter column ").append(alter.field.name);
    
    String pre = "alter table " + alter.field.owner.name + " modify (" + alter.field.name;
    
    FOR: for (AlterPartPart alterPart : sort(alter.alters)) {
      switch (alterPart) {
      case DEFAULT:
        if (alter.field.defaultValue == null) {
          res.add(pre + " default null)");
        } else {
          res.add(pre + " default " + alter.field.defaultValue + ')');
        }
        continue FOR;
        
      case NOT_NULL:
        if (alter.field.nullable) {
          res.add(pre + " null)");
        } else {
          res.add(pre + " not null)");
        }
        continue FOR;
        
      case TYPE:
        res.add(pre + ' ' + alter.field.type + ')');
        continue FOR;
        
      default:
        throw new IllegalArgumentException("Unknown " + alterPart);
      }
    }//FOR: for (AlterPartPart alterPart : sort(alter.alters))
    
    sb.setLength(sb.length() - 1);
    return sb.toString();
  }
  
  private static List<AlterPartPart> sort(Set<AlterPartPart> alters) {
    List<AlterPartPart> ret = new ArrayList<>();
    ret.addAll(alters);
    Collections.sort(ret);
    return ret;
  }
  
  private String generateCreateOrReplaceView(View view) {
    return "create or replace view " + view.name + " as " + view.content;
  }
  
  private String generateCreateForeignKey(ForeignKey fk) {
    StringBuilder sb = new StringBuilder("alter table " + fk.from.name);
    sb.append(" add foreign key (");
    for (FieldVector v : fk.vectors) {
      sb.append(v.from.name).append(", ");
    }
    sb.setLength(sb.length() - 2);
    sb.append(") references ").append(fk.to.name).append('(');
    for (FieldVector v : fk.vectors) {
      sb.append(v.to.name).append(", ");
    }
    sb.setLength(sb.length() - 2);
    sb.append(')');
    return sb.toString();
  }
  
  private String generateCreateSequence(Sequence sequence) {
    StringBuilder sb = new StringBuilder("create sequence " + sequence.name);
    if (sequence.startFrom > 1) {
      sb.append(" start with " + sequence.startFrom);
    }
    return sb.toString();
  }
  
  private String generateCreateOrReplaceFunc(StoreFunc func) {
    StringBuilder sb = new StringBuilder();
    sb.append("create or replace ").append(func.source).append(' ');
    return sb.toString();
  }
  
  private void generateDropCreateTrigger(List<String> sqlResult, Trigger trigger) {
    sqlResult.add(generateCreateTrigger(trigger));
  }
  
  private String generateCreateTrigger(Trigger trigger) {
    return "create or replace trigger " + trigger.name + " " + trigger.eventManipulation + " "
        + trigger.actionStatement;
  }
  
}
