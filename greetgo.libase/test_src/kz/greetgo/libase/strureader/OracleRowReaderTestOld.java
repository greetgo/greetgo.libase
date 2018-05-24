package kz.greetgo.libase.strureader;

import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static org.fest.assertions.api.Assertions.assertThat;

public class OracleRowReaderTestOld {

  private Connection connection;

  //  @BeforeClass
  private void openConnectionToDb() throws Exception {
    Class.forName("oracle.jdbc.OracleDriver");

    connection = DriverManager.getConnection(//
      "jdbc:oracle:thin:@192.168.0.101:1521:orcl", //
      "aaa1", "aaa1");
  }

  //  @AfterClass
  private void afterClass() throws Exception {
    connection.close();
    connection = null;
  }

  @Test(enabled = false)
  public void readAllTableColumns() throws Exception {
    RowReaderOracle reader = new RowReaderOracle(connection);
    List<ColumnRow> list = reader.readAllTableColumns();

    for (ColumnRow columnRow : list) {
      System.out.println(columnRow);
    }
  }

  @Test(enabled = false)
  public void readAllTablePrimaryKey() throws Exception {
    RowReaderOracle reader = new RowReaderOracle(connection);
    Map<String, PrimaryKeyRow> map = reader.readAllTablePrimaryKeys();

    for (PrimaryKeyRow primaryKey : map.values()) {
      System.out.println(primaryKey);
    }
  }

  @Test(enabled = false)
  public void readAllForeignKeys() throws Exception {
    RowReaderOracle reader = new RowReaderOracle(connection);
    for (ForeignKeyRow r : reader.readAllForeignKeys().values()) {
      System.out.println(r);
    }
  }

  @Test(enabled = false)
  public void readAllSequences() throws Exception {
    RowReaderOracle reader = new RowReaderOracle(connection);
    for (SequenceRow x : reader.readAllSequences().values()) {
      System.out.println(x);
    }

  }

  @Test(enabled = false)
  public void readAllViews() throws Exception {
    RowReaderOracle reader = new RowReaderOracle(connection);
    for (ViewRow vr : reader.readAllViews().values()) {
      System.out.println(vr);
    }
  }

  @Test(enabled = false)
  public void readAllFuncs() throws Exception {
    RowReaderOracle reader = new RowReaderOracle(connection);
    for (StoreFuncRow f : reader.readAllFuncs()) {
      System.out.println(f);
    }
  }

  @Test(enabled = false)
  public void readAllTriggers() throws Exception {
    RowReaderOracle reader = new RowReaderOracle(connection);
    for (TriggerRow x : reader.readAllTriggers().values()) {
      System.out.println(x);
    }
  }

  @Test(enabled = false)
  public void readTableComments() throws Exception {
    RowReaderOracle reader = new RowReaderOracle(connection);
    Map<String, String> map = reader.readTableComments();
    for (Entry<String, String> e : map.entrySet()) {
      System.out.println(e);
    }
  }

  @Test(enabled = false)
  public void readColumnComments() throws Exception {
    RowReaderOracle reader = new RowReaderOracle(connection);
    Map<String, String> map = reader.readColumnComments();
    for (Entry<String, String> e : map.entrySet()) {
      System.out.println(e);
    }
    assertThat(1);
  }
}
