package kz.greetgo.libase.changes;

import kz.greetgo.libase.model.DbStru;
import kz.greetgo.libase.model.Field;
import kz.greetgo.libase.model.ForeignKey;
import kz.greetgo.libase.model.Relation;
import kz.greetgo.libase.model.Sequence;
import kz.greetgo.libase.model.StoreFunc;
import kz.greetgo.libase.model.Table;
import kz.greetgo.libase.model.Trigger;
import kz.greetgo.libase.model.View;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public class Comparer {

  public Comparer() {}

  private final List<Change> changeList = new ArrayList<>();

  public List<Change> changeList() {
    changeList.sort(new ChangeComparator());
    return changeList;
  }

  public static List<Change> compare(DbStru to, DbStru from) {
    Comparer comparer = new Comparer();
    comparer.addCompare(to, from);
    return comparer.changeList();
  }

  private final Set<String> createTables = new HashSet<>();

  private void saveCreateRelation(CreateRelation createRelation) {
    changeList.add(createRelation);
    if (createRelation.relation instanceof Table) {
      createTables.add(((Table) createRelation.relation).name);
    }
  }

  private void saveCreateForeignKey(CreateForeignKey createForeignKey) {
    changeList.add(createForeignKey);
  }

  private void saveCreateSequence(CreateSequence createSequence) {
    changeList.add(createSequence);
  }

  private void saveCreateOrReplaceFunc(CreateOrReplaceFunc createOrReplaceFunc) {
    changeList.add(createOrReplaceFunc);
  }

  private void saveCreateTrigger(CreateTrigger createTrigger) {
    changeList.add(createTrigger);
  }

  private void saveDropCreateTrigger(DropCreateTrigger dropCreateTrigger) {
    changeList.add(dropCreateTrigger);
  }

  private void saveCreateOrReplaceView(CreateOrReplaceView createOrReplaceView) {
    changeList.add(createOrReplaceView);
  }

  private final Set<String> addingTableFields = new HashSet<>();

  private void saveAddTableField(AddTableField addTableField) {
    changeList.add(addTableField);
    addingTableFields.add(addTableField.field.fullName());
  }

  private void saveAlterField(AlterField alterField) {
    changeList.add(alterField);
  }

  private void saveTableComment(TableComment tableComment) {
    if (isNullOrEmpty(tableComment.table.comment)
      && createTables.contains(tableComment.table.name)) {
      return;
    }

    changeList.add(tableComment);
  }

  private static boolean isNullOrEmpty(String str) {
    if (str == null) return true;
    return str.trim().length() == 0;
  }

  private void saveFieldComment(FieldComment fieldComment) {
    if (isNullOrEmpty(fieldComment.field.comment) && (

      createTables.contains(fieldComment.table.name)
        || addingTableFields.contains(fieldComment.field.fullName())

    )) return;

    changeList.add(fieldComment);

  }

  public void addCompare(DbStru to, DbStru from) {
    for (Relation relationFrom : from.relations.values()) {
      Relation relationTo = to.relations.get(relationFrom.name);
      if (relationTo == null) {
        saveCreateRelation(new CreateRelation(relationFrom));
        continue;
      }

      addRelationModify(relationFrom, relationTo);
    }

    for (ForeignKey fk : from.foreignKeys) {
      if (to.foreignKeys.contains(fk)) continue;
      saveCreateForeignKey(new CreateForeignKey(fk));
    }

    for (Sequence sequence : from.sequences) {
      if (to.sequences.contains(sequence)) continue;
      saveCreateSequence(new CreateSequence(sequence));
    }

    for (StoreFunc fromFunc : from.funcs.values()) {
      StoreFunc toFunc = to.funcs.get(fromFunc);
      if (!fromFunc.fullEquals(toFunc)) {
        saveCreateOrReplaceFunc(new CreateOrReplaceFunc(fromFunc));
      }
    }

    for (Trigger fromTrigger : from.triggers.values()) {
      Trigger toTrigger = to.triggers.get(fromTrigger);
      if (toTrigger == null) {
        saveCreateTrigger(new CreateTrigger(fromTrigger));
      } else if (!fromTrigger.fullEquals(toTrigger)) {
        saveDropCreateTrigger(new DropCreateTrigger(fromTrigger));
      }
    }

    for (Relation relationFrom : from.relations.values()) {
      if (!(relationFrom instanceof Table)) continue;
      Table tableFrom = (Table) relationFrom;
      Table tableTo = to.table(tableFrom.name);
      if (tableTo == null) {
        if (tableFrom.trimComment().length() > 0) {
          saveTableComment(new TableComment(tableFrom));
        }
        for (Field fieldFrom : tableFrom.allFields) {
          if (fieldFrom.comment != null) {
            saveFieldComment(new FieldComment(tableFrom, fieldFrom));
          }
        }
        continue;
      }
      if (tableFrom.trimComment().length() > 0
        && !tableTo.trimComment().equals(tableFrom.trimComment())) {
        saveTableComment(new TableComment(tableFrom));
      }
      for (Field fieldFrom : tableFrom.allFields) {
        Field fieldTo = tableTo.field(fieldFrom.name);
        if (fieldTo == null || fieldFrom.trimComment().length() > 0
          && !fieldTo.trimComment().equals(fieldFrom.trimComment())) {
          saveFieldComment(new FieldComment(tableFrom, fieldFrom));
        }
      }
    }
  }

  private void addRelationModify(Relation relationFrom,
                                 Relation relationTo) {

    if (relationFrom instanceof Table && relationTo instanceof Table) {
      Table tableFrom = (Table) relationFrom;
      Table tableTo = (Table) relationTo;
      addTableModify(tableTo, tableFrom);
      return;
    }

    if (relationFrom instanceof View && relationTo instanceof View) {
      View viewFrom = (View) relationFrom;
      View viewTo = (View) relationTo;
      if (!Objects.equals(viewFrom.content, viewTo.content)) {
        saveCreateOrReplaceView(new CreateOrReplaceView(viewFrom));
      }
      return;
    }

    throw new IllegalArgumentException("Cannot change from " + relationFrom + " to " + relationTo);
  }

  private void addTableModify(Table tableTo, Table tableFrom) {
    for (Field fieldFrom : tableFrom.allFields) {
      Field fieldTo = tableTo.field(fieldFrom.name);
      if (fieldTo == null) {
        saveAddTableField(new AddTableField(fieldFrom));
        continue;
      }
      addFieldModify(fieldTo, fieldFrom);
    }
  }

  private void addFieldModify(Field fieldTo, Field fieldFrom) {
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

    saveAlterField(new AlterField(fieldTo, fieldFrom, alters));
  }
}
