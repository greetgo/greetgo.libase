package kz.greetgo.libase.strureader;

import static org.fest.assertions.api.Assertions.assertThat;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class PostgresRowReaderTest {
  
  private Connection connection;
  
  @BeforeClass
  @SuppressFBWarnings("DMI_EMPTY_DB_PASSWORD")
  private void beforeClass() throws Exception {
    Class.forName("org.postgresql.Driver");
    
    connection = DriverManager.getConnection("jdbc:postgresql:scoring_diff", "scoring_diff", "");
  }
  
  @AfterClass
  private void afterClass() throws Exception {
    connection.close();
    connection = null;
  }
  
  @Test
  public void readAllTableColumns() throws Exception {
    RowReaderPostgres reader = new RowReaderPostgres(connection);
    List<ColumnRow> list = reader.readAllTableColumns();
    
    for (ColumnRow columnRow : list) {
      System.out.println(columnRow);
    }
  }
  
  @Test
  public void readAllTablePrimaryKey() throws Exception {
    RowReaderPostgres reader = new RowReaderPostgres(connection);
    Map<String, PrimaryKeyRow> map = reader.readAllTablePrimaryKeys();
    
    for (PrimaryKeyRow primaryKey : map.values()) {
      System.out.println(primaryKey);
    }
  }
  
  @Test
  public void readAllForeignKeys() throws Exception {
    RowReaderPostgres reader = new RowReaderPostgres(connection);
    for (ForeignKeyRow r : reader.readAllForeignKeys().values()) {
      System.out.println(r);
    }
  }
  
  @Test
  public void readAllSequences() throws Exception {
    RowReaderPostgres reader = new RowReaderPostgres(connection);
    for (SequenceRow x : reader.readAllSequences().values()) {
      System.out.println(x);
    }
    
  }
  
  @Test
  public void readAllViews() throws Exception {
    RowReaderPostgres reader = new RowReaderPostgres(connection);
    for (ViewRow vr : reader.readAllViews().values()) {
      System.out.println(vr);
    }
  }
  
  @Test
  public void readAllFuncs() throws Exception {
    RowReaderPostgres reader = new RowReaderPostgres(connection);
    for (StoreFuncRow f : reader.readAllFuncs()) {
      System.out.println(f);
    }
  }
  
  @Test
  public void readAllTriggers() throws Exception {
    RowReaderPostgres reader = new RowReaderPostgres(connection);
    for (TriggerRow x : reader.readAllTriggers().values()) {
      System.out.println(x);
    }
  }
  
  @Test
  public void readTableComments() throws Exception {
    RowReaderPostgres reader = new RowReaderPostgres(connection);
    Map<String, String> map = reader.readTableComments();
    for (Entry<String, String> e : map.entrySet()) {
      System.out.println(e);
    }
  }
  
  @Test
  public void readColumnComments() throws Exception {
    RowReaderPostgres reader = new RowReaderPostgres(connection);
    Map<String, String> map = reader.readColumnComments();
    for (Entry<String, String> e : map.entrySet()) {
      System.out.println(e);
    }
    assertThat(1);
  }
}
