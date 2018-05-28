package kz.greetgo.libase.strureader;

import kz.greetgo.libase.changes.Change;
import kz.greetgo.libase.changes.Comparer;
import kz.greetgo.libase.changes.CreateRelation;
import kz.greetgo.libase.changesql.SqlGeneratorOracle;
import kz.greetgo.libase.model.DbStru;
import kz.greetgo.libase.model.Relation;
import kz.greetgo.libase.model.Table;
import kz.greetgo.libase.model.View;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.List;

public class DiffOracleTestOld {
  private Connection connTo, connFrom;

  //  @BeforeClass
  private void openConnectionToDb() throws Exception {
    Class.forName("org.postgresql.Driver");

    connFrom = DriverManager.getConnection("jdbc:oracle:thin:@192.168.11.103:1521:orcl",
      "POMPEI_KASPIPTP_DIFF", "pompei_kaspiptp");
    connTo = DriverManager.getConnection("jdbc:oracle:thin:@192.168.11.103:1521:orcl",
      "POMPEI_KASPIPTP", "pompei_kaspiptp");
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
    System.out.println("Чтение TO...");
    DbStru to = StruReader.read(new RowReaderOracle(connTo));
    System.out.println("OK");
    System.out.println("Чтение FROM...");
    DbStru from = StruReader.read(new RowReaderOracle(connFrom));
    System.out.println("OK");

    List<Change> changes = Comparer.compare(to, from);

    for (Change change : changes) {
      if (change instanceof CreateRelation) {
        Relation rel = ((CreateRelation) change).relation;
        if (rel instanceof View) {
          System.out.println("---> VIEW " + rel.name);
        }
        if (rel instanceof Table) {
          System.out.println("---> TABLE " + rel.name);
        }
      }
    }

    if ("a".equals("a")) return;

    SqlGeneratorOracle g = new SqlGeneratorOracle();
    List<String> sqlResult = new ArrayList<>();
    g.generate(sqlResult, changes);

    System.out.println("-----------------------------------------");
    for (String sql : sqlResult) {
      System.out.println(sql + ";;");
    }
    System.out.println("-----------------------------------------");
  }
}
