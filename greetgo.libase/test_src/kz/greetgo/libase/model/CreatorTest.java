package kz.greetgo.libase.model;

import static org.fest.assertions.api.Assertions.assertThat;

import java.util.HashSet;
import java.util.Set;

import org.testng.annotations.Test;

public class CreatorTest {
  @Test
  public void createTable() throws Exception {
    DbStru stru = new DbStru();
    Creator.addTable(stru, "client_phone *client_id int, type varchar(100) notnull,"
        + " *value varchar(100), actual int def1__1");
    
    assertThat(stru.relations.keySet()).contains("client_phone");
    
    Relation relation = stru.relations.get("client_phone");
    assertThat(relation).isInstanceOf(Table.class);
    
    Table table = (Table)relation;
    assertThat(table.name).isEqualTo("client_phone");
    assertThat(table.allFields).hasSize(4);
    assertThat(table.keyFields).hasSize(2);
    
    assertThat(table.allFields.get(0).name).isEqualTo("client_id");
    assertThat(table.allFields.get(1).name).isEqualTo("type");
    assertThat(table.allFields.get(2).name).isEqualTo("value");
    assertThat(table.allFields.get(3).name).isEqualTo("actual");
    
    assertThat(table.allFields.get(0).type).isEqualTo("int");
    assertThat(table.allFields.get(1).type).isEqualTo("varchar(100)");
    assertThat(table.allFields.get(2).type).isEqualTo("varchar(100)");
    assertThat(table.allFields.get(3).type).isEqualTo("int");
    
    assertThat(table.allFields.get(0).nullable).isEqualTo(true);
    assertThat(table.allFields.get(1).nullable).isEqualTo(false);
    assertThat(table.allFields.get(2).nullable).isEqualTo(true);
    assertThat(table.allFields.get(3).nullable).isEqualTo(true);
    
    assertThat(table.allFields.get(0).defaultValue).isNull();
    assertThat(table.allFields.get(1).defaultValue).isNull();
    assertThat(table.allFields.get(2).defaultValue).isNull();
    assertThat(table.allFields.get(3).defaultValue).isEqualTo("1__1");
  }
  
  @Test
  public void createView() throws Exception {
    DbStru stru = new DbStru();
    Creator.addTable(stru, "client *id int");
    Creator.addTable(stru, "asd *id int");
    
    Creator.addView(stru, "a_view client asd|abra kadabra");
    
    assertThat(stru.relations.keySet()).contains("a_view");
    Relation relation = stru.relations.get("a_view");
    assertThat(relation).isInstanceOf(View.class);
    
    View view = (View)relation;
    
    assertThat(view.name).isEqualTo("a_view");
    assertThat(view.content).isEqualTo("abra kadabra");
    Set<String> deps = new HashSet<>();
    for (Relation rel : view.dependences) {
      deps.add(rel.name);
    }
    assertThat(deps).hasSize(2);
    assertThat(deps).contains("asd", "client");
  }
}
