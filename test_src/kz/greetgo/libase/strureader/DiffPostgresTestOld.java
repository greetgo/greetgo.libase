package kz.greetgo.libase.strureader;

import kz.greetgo.libase.changes.Change;
import kz.greetgo.libase.changes.Comparer;
import kz.greetgo.libase.changesql.SqlGeneratorPostgres;
import kz.greetgo.libase.model.DbStru;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.List;

public class DiffPostgresTestOld {
  private Connection connTo, connFrom;

  //  @BeforeClass
  private void openConnectionToDb() throws Exception {
    Class.forName("org.postgresql.Driver");

    connFrom = DriverManager.getConnection("jdbc:postgresql:scoring_diff", "scoring_diff", "");
    connTo = DriverManager.getConnection("jdbc:postgresql:scoring", "scoring", "");
  }

  //  @AfterClass
  private void afterClass() throws Exception {
    connTo.close();
    connTo = null;
    connFrom.close();
    connFrom = null;
  }

  @Test(enabled = false)
  public void diff() throws Exception {
    DbStru to = StruReader.read(new RowReaderPostgres(connTo));
    DbStru from = StruReader.read(new RowReaderPostgres(connFrom));

    List<Change> changes = Comparer.compare(to, from);

    SqlGeneratorPostgres g = new SqlGeneratorPostgres();
    List<String> sqlResult = new ArrayList<>();
    g.generate(sqlResult, changes);

    for (String sql : sqlResult) {
      System.out.println(sql + ";;");
    }
  }
}
