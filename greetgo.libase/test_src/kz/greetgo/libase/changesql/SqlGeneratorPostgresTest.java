package kz.greetgo.libase.changesql;

import static org.fest.assertions.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;

import kz.greetgo.libase.changes.AddTableField;
import kz.greetgo.libase.changes.AlterField;
import kz.greetgo.libase.changes.AlterPartPart;
import kz.greetgo.libase.changes.Change;
import kz.greetgo.libase.changes.CreateOrReplaceView;
import kz.greetgo.libase.changes.CreateRelation;
import kz.greetgo.libase.changes.FieldComment;
import kz.greetgo.libase.changes.TableComment;
import kz.greetgo.libase.model.Creator;
import kz.greetgo.libase.model.DbStru;
import kz.greetgo.libase.model.Field;

import org.testng.annotations.Test;

public class SqlGeneratorPostgresTest {
  @Test
  public void generate_createTable() throws Exception {
    DbStru stru = new DbStru();
    Creator.addTable(stru, "asd *id int notnull, name varchar(100) def'asd'");
    
    SqlGeneratorPostgres g = new SqlGeneratorPostgres();
    List<Change> changes = new ArrayList<>();
    changes.add(new CreateRelation(stru.relations.get("asd")));
    List<String> sqlResult = new ArrayList<>();
    g.generate(sqlResult, changes);
    
    assertThat(sqlResult).hasSize(1);
    assertThat(sqlResult.get(0)).isEqualTo(
        "create table asd(id int not null," + " name varchar(100) default 'asd',"
            + "  primary key (id))");
  }
  
  @Test
  public void generate_createView() throws Exception {
    DbStru stru = new DbStru();
    Creator.addTable(stru, "t1 id int");
    Creator.addTable(stru, "t2 id int");
    Creator.addView(stru, "asd t1 t2|select 'hello world' hello_world");
    
    SqlGeneratorPostgres g = new SqlGeneratorPostgres();
    List<Change> changes = new ArrayList<>();
    changes.add(new CreateRelation(stru.relations.get("asd")));
    List<String> sqlResult = new ArrayList<>();
    g.generate(sqlResult, changes);
    
    assertThat(sqlResult).hasSize(1);
    assertThat(sqlResult.get(0)).isEqualTo("create view asd as select 'hello world' hello_world");
  }
  
  @Test
  public void generate_addTableField() throws Exception {
    DbStru stru = new DbStru();
    Creator.addTable(stru, "asd *id int notnull, name varchar(100) notnull def'asd'");
    Field field = stru.table("asd").field("name");
    List<Change> changes = new ArrayList<>();
    changes.add(new AddTableField(field));
    List<String> sqlResult = new ArrayList<>();
    SqlGeneratorPostgres g = new SqlGeneratorPostgres();
    g.generate(sqlResult, changes);
    
    assertThat(sqlResult).hasSize(1);
    assertThat(sqlResult.get(0)).isEqualTo(
        "alter table asd add column name varchar(100) not null default 'asd'");
  }
  
  @Test
  public void generate_alterField1() throws Exception {
    DbStru stru = new DbStru();
    Creator.addTable(stru, "asd *id int notnull, name varchar(100) notnull def'asd'");
    Field field = stru.table("asd").field("name");
    List<Change> changes = new ArrayList<>();
    changes.add(new AlterField(field,//
        AlterPartPart.DEFAULT, AlterPartPart.NOT_NULL, AlterPartPart.TYPE));
    List<String> sqlResult = new ArrayList<>();
    SqlGeneratorPostgres g = new SqlGeneratorPostgres();
    g.generate(sqlResult, changes);
    
    System.out.println(sqlResult);
    
    assertThat(sqlResult).hasSize(1);
    assertThat(sqlResult.get(0)).isEqualTo(
        "alter table asd alter column name set default 'asd', set not null, type varchar(100)");
  }
  
  @Test
  public void generate_alterField2() throws Exception {
    DbStru stru = new DbStru();
    Creator.addTable(stru, "asd *id int notnull, name varchar(100)");
    Field field = stru.table("asd").field("name");
    List<Change> changes = new ArrayList<>();
    changes.add(new AlterField(field,//
        AlterPartPart.DEFAULT, AlterPartPart.NOT_NULL));
    List<String> sqlResult = new ArrayList<>();
    SqlGeneratorPostgres g = new SqlGeneratorPostgres();
    g.generate(sqlResult, changes);
    
    System.out.println(sqlResult);
    
    assertThat(sqlResult).hasSize(1);
    assertThat(sqlResult.get(0)).isEqualTo(
        "alter table asd alter column name drop default, drop not null");
  }
  
  @Test
  public void generate_createOrReplaceView() throws Exception {
    DbStru stru = new DbStru();
    Creator.addView(stru, "asd|select 'hello world' hello_world");
    
    SqlGeneratorPostgres g = new SqlGeneratorPostgres();
    List<Change> changes = new ArrayList<>();
    changes.add(new CreateOrReplaceView(stru.view("asd")));
    List<String> sqlResult = new ArrayList<>();
    g.generate(sqlResult, changes);
    
    assertThat(sqlResult).hasSize(1);
    assertThat(sqlResult.get(0)).isEqualTo(
        "create or replace view asd as select 'hello world' hello_world");
  }
  
  @Test
  public void generate_TableComment() throws Exception {
    DbStru stru = new DbStru();
    Creator.addTable(stru, "asd *id int notnull, name varchar(100)");
    stru.table("asd").comment = "wow";
    
    TableComment x = new TableComment(stru.table("asd"));
    
    List<Change> changes = new ArrayList<>();
    changes.add(x);
    
    List<String> sqlResult = new ArrayList<>();
    SqlGeneratorPostgres g = new SqlGeneratorPostgres();
    g.generate(sqlResult, changes);
    
    System.out.println(sqlResult);
    
    assertThat(sqlResult).hasSize(1);
    assertThat(sqlResult.get(0)).isEqualTo("COMMENT ON TABLE asd IS 'wow'");
  }
  
  @Test
  public void generate_FieldComment() throws Exception {
    DbStru stru = new DbStru();
    Creator.addTable(stru, "asd *id int notnull, name varchar(100)");
    stru.table("asd").field("name").comment = "asd123";
    
    FieldComment x = new FieldComment(stru.table("asd"), stru.table("asd").field("name"));
    
    List<Change> changes = new ArrayList<>();
    changes.add(x);
    
    List<String> sqlResult = new ArrayList<>();
    SqlGeneratorPostgres g = new SqlGeneratorPostgres();
    g.generate(sqlResult, changes);
    
    System.out.println(sqlResult);
    
    assertThat(sqlResult).hasSize(1);
    assertThat(sqlResult.get(0)).isEqualTo("comment on column asd.name is 'asd123'");
  }
}
