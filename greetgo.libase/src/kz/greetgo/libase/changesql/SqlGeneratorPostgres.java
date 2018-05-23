package kz.greetgo.libase.changesql;

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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public class SqlGeneratorPostgres implements SqlGenerator {

  @Override
  public void generate(List<String> sqlResult, List<Change> changes) {
    for (Change change : changes) {

      if (change instanceof DropCreateTrigger) {
        generateDropCreateTrigger(sqlResult, ((DropCreateTrigger) change).trigger);
        continue;
      }

      sqlResult.add(generateChange(change));
    }
  }

  private String generateChange(Change change) {
    if (change instanceof CreateRelation) {
      return generateCreateRelation(((CreateRelation) change).relation);
    }

    if (change instanceof AddTableField) {
      return generateAddTableField(((AddTableField) change).field);
    }
    if (change instanceof AlterField) {
      return generateAlterField((AlterField) change);
    }
    if (change instanceof CreateOrReplaceView) {
      return generateCreateOrReplaceView(((CreateOrReplaceView) change).view);
    }
    if (change instanceof CreateForeignKey) {
      return generateCreateForeignKey(((CreateForeignKey) change).foreignKey);
    }
    if (change instanceof CreateSequence) {
      return generateCreateSequence(((CreateSequence) change).sequence);
    }
    if (change instanceof CreateOrReplaceFunc) {
      return generateCreateOrReplaceFunc(((CreateOrReplaceFunc) change).storeFunc);
    }
    if (change instanceof CreateTrigger) {
      return generateCreateTrigger(((CreateTrigger) change).trigger);
    }
    if (change instanceof TableComment) {
      return GeneratorAddon.generateTableComment(((TableComment) change).table);
    }
    if (change instanceof FieldComment) {
      FieldComment x = (FieldComment) change;
      return GeneratorAddon.generateFieldComment(x.table, x.field);
    }

    throw new IllegalArgumentException("Unknown change " + change);
  }

  private String generateCreateRelation(Relation relation) {
    if (relation instanceof Table) {
      return generateCreateTable((Table) relation);
    }

    if (relation instanceof View) {
      return generateCreateView((View) relation);
    }

    throw new IllegalArgumentException("Unknown relation " + relation);
  }

  private String generateCreateTable(Table table) {
    StringBuilder sb = new StringBuilder();
    sb.append("create table ").append(table.name).append("(");
    for (Field field : table.allFields) {
      sb.append(field.name).append(' ').append(correctType(field.type));
      if (!field.nullable) sb.append(" not null");
      if (field.defaultValue != null) sb.append(" default ").append(field.defaultValue);
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

  private String correctType(String type) {
    if (type == null) return "";
    if (type.toLowerCase().startsWith("smallint")) {
      return "smallint";
    }
    return type;
  }

  private String generateCreateView(View view) {
    return "create view " + view.name + " as " + view.content;
  }

  private String generateAddTableField(Field field) {
    StringBuilder sb = new StringBuilder();
    sb.append("alter table " + field.owner.name + " add column " + field.name + ' ' + correctType(field.type));
    if (!field.nullable) sb.append(" not null");
    if (field.defaultValue != null) sb.append(" default ").append(field.defaultValue);
    return sb.toString();
  }

  private String generateAlterField(AlterField alter) {
    StringBuilder sb = new StringBuilder();
    sb.append("alter table ").append(alter.field.owner.name);
    sb.append(" alter column ").append(alter.field.name);

    FOR:
    for (AlterPartPart alterPart : sort(alter.alters)) {
      switch (alterPart) {
        case DEFAULT:
          if (alter.field.defaultValue == null) {
            sb.append(" drop default,");
          } else {
            sb.append(" set default " + alter.field.defaultValue + ',');
          }
          continue FOR;

        case NOT_NULL:
          if (alter.field.nullable) {
            sb.append(" drop not null,");
          } else {
            sb.append(" set not null,");
          }
          continue FOR;

        case TYPE:
          sb.append(" type " + correctType(alter.field.type) + ",");
          continue FOR;

        default:
          throw new IllegalArgumentException("Unknown " + alterPart);
      }
    }//FOR

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
    sb.append("create or replace function ").append(func.name).append('(');
    for (int i = 0, C = func.argTypes.size(); i < C; i++) {
      if (i > 0) sb.append(", ");
      sb.append(func.argNames.get(i)).append(' ');
      sb.append(func.argTypes.get(i));
    }
    sb.append(") returns ").append(func.returns);
    sb.append(" as $LIBASE$").append(func.source).append("$LIBASE$ language " + func.language);
    return sb.toString();
  }

  private void generateDropCreateTrigger(List<String> sqlResult, Trigger trigger) {
    sqlResult.add(generateDropTrigger(trigger));
    sqlResult.add(generateCreateTrigger(trigger));
  }

  private String generateCreateTrigger(Trigger trigger) {
    return "create trigger " + trigger.name + " " + trigger.actionTiming + " "
      + trigger.eventManipulation + " on " + trigger.tableName + " for each "
      + trigger.actionOrientation + " " + trigger.actionStatement;
  }

  private String generateDropTrigger(Trigger trigger) {
    return "drop trigger " + trigger.name + " on " + trigger.tableName;
  }
}
