package kz.greetgo.libase.changes;

import static org.fest.assertions.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;

import kz.greetgo.libase.model.Creator;
import kz.greetgo.libase.model.DbStru;
import kz.greetgo.libase.model.Table;

import org.testng.annotations.Test;

public class ComparerTest {
  @Test
  public void compare_CreateRelation() throws Exception {
    DbStru from = new DbStru();
    DbStru to = new DbStru();
    
    Creator.addTable(from, "asd *id int, name varchar(100)");
    
    List<Change> list = noComments(Comparer.compare(to, from));
    
    assertThat(list).hasSize(1);
    assertThat(list.get(0)).isInstanceOf(CreateRelation.class);
    
    CreateRelation cr = (CreateRelation)list.get(0);
    
    assertThat(cr.relation).isInstanceOf(Table.class);
    
    Table table = (Table)cr.relation;
    
    assertThat(table.name).isEqualTo("asd");
  }
  
  @Test
  public void compare_AddTableField() throws Exception {
    DbStru from = new DbStru();
    DbStru to = new DbStru();
    
    Creator.addTable(from, "asd *id int, name varchar(100), wow int defASDxxDD");
    Creator.addTable(to, "asd *id int, name varchar(100)");
    
    List<Change> list = noComments(Comparer.compare(to, from));
    
    assertThat(list).hasSize(1);
    assertThat(list.get(0)).isInstanceOf(AddTableField.class);
    
    AddTableField atf = (AddTableField)list.get(0);
    
    assertThat(atf.field.owner.name).isEqualTo("asd");
    assertThat(atf.field.name).isEqualTo("wow");
    assertThat(atf.field.type).isEqualTo("int");
    assertThat(atf.field.defaultValue).isEqualTo("ASDxxDD");
  }
  
  @Test
  public void compare_noChange() throws Exception {
    DbStru from = new DbStru();
    DbStru _to_ = new DbStru();
    
    Creator.addTable(from, "asd *id int, name varchar(100), wow int defASDxxDD notnull");
    Creator.addTable(_to_, "asd *id int, name varchar(100), wow int defASDxxDD notnull");
    
    List<Change> list = Comparer.compare(_to_, from);
    
    assertThat(list).hasSize(0);
  }
  
  @Test
  public void compare_fieldChange1() throws Exception {
    DbStru from = new DbStru();
    DbStru _to_ = new DbStru();
    
    Creator.addTable(from, "asd *id int, name varchar(100), wow int defASDxxAA notnull");
    Creator.addTable(_to_, "asd *id int, name varchar(100), wow int defASDxxDD notnull");
    
    List<Change> list = Comparer.compare(_to_, from);
    
    assertThat(list).hasSize(1);
    assertThat(list.get(0)).isInstanceOf(AlterField.class);
    
    AlterField alt = (AlterField)list.get(0);
    assertThat(alt.alters).hasSize(1);
    assertThat(alt.alters).contains(AlterPartPart.DEFAULT);
    assertThat(alt.field.defaultValue).isEqualTo("ASDxxAA");
  }
  
  @Test
  public void compare_fieldChange2() throws Exception {
    DbStru from = new DbStru();
    DbStru _to_ = new DbStru();
    
    Creator.addTable(from, "asd *id int, name varchar(100), wow int defASDxxAA notnull");
    Creator.addTable(_to_, "asd *id int, name varchar(100), wow int1");
    
    List<Change> list = Comparer.compare(_to_, from);
    
    assertThat(list).hasSize(1);
    assertThat(list.get(0)).isInstanceOf(AlterField.class);
    
    AlterField alt = (AlterField)list.get(0);
    assertThat(alt.alters).hasSize(3);
    assertThat(alt.alters).contains(AlterPartPart.DEFAULT);
    assertThat(alt.alters).contains(AlterPartPart.NOT_NULL);
    assertThat(alt.alters).contains(AlterPartPart.TYPE);
    assertThat(alt.field.defaultValue).isEqualTo("ASDxxAA");
    assertThat(alt.field.nullable).isEqualTo(false);
    assertThat(alt.field.type).isEqualTo("int");
  }
  
  @Test
  public void compare_ChangeView() throws Exception {
    DbStru from = new DbStru();
    DbStru to = new DbStru();
    
    Creator.addView(from, "asd|hello_world");
    Creator.addView(to, "asd|hello_world_001");
    
    List<Change> list = Comparer.compare(to, from);
    
    assertThat(list).hasSize(1);
    assertThat(list.get(0)).isInstanceOf(CreateOrReplaceView.class);
    
    CreateOrReplaceView atf = (CreateOrReplaceView)list.get(0);
    
    assertThat(atf.view.name).isEqualTo("asd");
    assertThat(atf.view.content).isEqualTo("hello_world");
  }
  
  @Test
  public void compare_ChangeView_noChanges() throws Exception {
    DbStru from = new DbStru();
    DbStru to = new DbStru();
    
    Creator.addView(from, "asd|hello_world");
    Creator.addView(to, "asd|hello_world");
    
    List<Change> list = Comparer.compare(to, from);
    
    assertThat(list).hasSize(0);
  }
  
  @Test
  public void compare_comments_noChanges() throws Exception {
    DbStru from = new DbStru();
    DbStru _to_ = new DbStru();
    
    Creator.addTable(from, "asd *id int, name varchar(100)");
    Creator.addTable(_to_, "asd *id int, name varchar(100)");
    
    from.table("asd").comment = "asd";
    _to_.table("asd").comment = "asd";
    
    from.table("asd").field("id").comment = "dsa";
    _to_.table("asd").field("id").comment = "dsa";
    
    from.table("asd").field("name").comment = "wow";
    _to_.table("asd").field("name").comment = "wow";
    
    List<Change> list = Comparer.compare(_to_, from);
    
    assertThat(list).hasSize(0);
  }
  
  @Test
  public void compare_comments_tableChanged() throws Exception {
    DbStru from = new DbStru();
    DbStru _to_ = new DbStru();
    
    Creator.addTable(from, "asd *id int, name varchar(100)");
    Creator.addTable(_to_, "asd *id int, name varchar(100)");
    
    from.table("asd").comment = "asd1";
    _to_.table("asd").comment = "asd";
    
    from.table("asd").field("id").comment = "dsa";
    _to_.table("asd").field("id").comment = "dsa";
    
    from.table("asd").field("name").comment = "wow";
    _to_.table("asd").field("name").comment = "wow";
    
    List<Change> list = Comparer.compare(_to_, from);
    
    assertThat(list).hasSize(1);
    assertThat(list.get(0)).isInstanceOf(TableComment.class);
    TableComment x = (TableComment)list.get(0);
    assertThat(x.table.comment).isEqualTo("asd1");
  }
  
  private static List<Comment> onlyComments(List<Change> in) {
    List<Comment> ret = new ArrayList<>();
    for (Change change : in) {
      if (change instanceof Comment) {
        ret.add((Comment)change);
      }
    }
    return ret;
  }
  
  private List<Change> noComments(List<Change> in) {
    List<Change> ret = new ArrayList<>();
    for (Change change : in) {
      if (!(change instanceof Comment)) {
        ret.add(change);
      }
    }
    return ret;
  }
  
  @Test
  public void compare_comments_fieldChanged() throws Exception {
    DbStru from = new DbStru();
    DbStru _to_ = new DbStru();
    
    Creator.addTable(from, "asd *id int, name varchar(100)");
    Creator.addTable(_to_, "asd *id int, name varchar(100)");
    
    from.table("asd").comment = "asd";
    _to_.table("asd").comment = "asd";
    
    from.table("asd").field("id").comment = "dsa1";
    _to_.table("asd").field("id").comment = "dsa";
    
    from.table("asd").field("name").comment = "wow";
    _to_.table("asd").field("name").comment = "wow";
    
    List<Change> list = Comparer.compare(_to_, from);
    
    assertThat(list).hasSize(1);
    assertThat(list.get(0)).isInstanceOf(FieldComment.class);
    FieldComment x = (FieldComment)list.get(0);
    assertThat(x.field.comment).isEqualTo("dsa1");
  }
  
  @Test
  public void compare_comments_fieldChanged_NoField() throws Exception {
    DbStru from = new DbStru();
    DbStru _to_ = new DbStru();
    
    Creator.addTable(from, "asd *id int, name varchar(100)");
    Creator.addTable(_to_, "asd *id int");
    
    from.table("asd").field("name").comment = "wow";
    
    List<Comment> list = onlyComments(Comparer.compare(_to_, from));
    
    assertThat(list).hasSize(1);
    assertThat(list.get(0)).isInstanceOf(FieldComment.class);
    FieldComment x = (FieldComment)list.get(0);
    assertThat(x.field.comment).isEqualTo("wow");
  }
  
  @Test
  public void compare_comments_fieldChanged_NoTable() throws Exception {
    DbStru from = new DbStru();
    DbStru _to_ = new DbStru();
    
    Creator.addTable(from, "asd *id int, name varchar(100)");
    
    from.table("asd").comment = "wow0";
    from.table("asd").field("id").comment = "wow1";
    from.table("asd").field("name").comment = "wow2";
    
    List<Comment> list = onlyComments(Comparer.compare(_to_, from));
    
    assertThat(list).hasSize(3);
  }
  
}
