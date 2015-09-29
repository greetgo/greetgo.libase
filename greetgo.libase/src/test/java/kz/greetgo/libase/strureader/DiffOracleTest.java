package kz.greetgo.libase.strureader;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.List;

import kz.greetgo.libase.changes.Change;
import kz.greetgo.libase.changes.Comparer;
import kz.greetgo.libase.changes.CreateRelation;
import kz.greetgo.libase.changesql.SqlGeneratorOracle;
import kz.greetgo.libase.model.DbStru;
import kz.greetgo.libase.model.Relation;
import kz.greetgo.libase.model.Table;
import kz.greetgo.libase.model.View;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class DiffOracleTest {
  private Connection connTo, connFrom;
  
  @BeforeClass
  @SuppressFBWarnings("DMI_CONSTANT_DB_PASSWORD")
  private void beforeClass() throws Exception {
    Class.forName("org.postgresql.Driver");
    
    connFrom = DriverManager.getConnection("jdbc:oracle:thin:@192.168.0.101:1521:orcl",
        "POMPEI_COLLECT_DIFF", "pompei_collect");
    connTo = DriverManager.getConnection("jdbc:oracle:thin:@192.168.0.101:1521:orcl",
        "POMPEI_COLLECT", "pompei_collect");
  }
  
  @AfterClass
  private void afterClass() throws Exception {
    connTo.close();
    connTo = null;
    connFrom.close();
    connFrom = null;
  }
  
  @Test
  public void diff() throws Exception {
    DbStru to = StruReader.read(new RowReaderOracle(connTo));
    DbStru from = StruReader.read(new RowReaderOracle(connFrom));
    
    List<Change> changes = Comparer.compare(to, from);
    
    Comparer.sort(changes);
    
    for (Change change : changes) {
      if (change instanceof CreateRelation) {
        Relation rel = ((CreateRelation)change).relation;
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
