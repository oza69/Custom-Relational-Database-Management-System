package test;

import com.csci5408.*;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;


class QueryTest {

  EventLog logtest;

  {
    try {
      logtest = new EventLog("logtest.txt");
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  // char length test
  @Test
  void insert_data_validation1() throws ClassNotFoundException, IOException, ParseException {
    Main.tableStatus = new HashMap<>();
    Main.GlobalDict = new HashMap<String, ArrayList<HashMap>>();
    Main.initialize_gdd(Main.GlobalDict,Main.tableStatus);
    QueryInAction currentQuery = new QueryInAction();
    Query q = new Query();

    String createString = "CREATE TABLE Persons (PersonID int,LastName varchar(50),FirstName varchar(50),Address varchar(50),City varchar(50));";
    HashMap createData = QueryParser.parseQuery(createString);
    Main.createQuery(currentQuery, q, createData);

    String insertString = "INSERT INTO Persons (LastName) VALUES ('AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA');";
    HashMap insertData = QueryParser.parseQuery(insertString);
    Main.insertQuery(currentQuery, q, insertData, logtest);
  }

  // positive int test
  @Test
  void insert_data_validation2() throws ClassNotFoundException, IOException, ParseException {
    Main.tableStatus = new HashMap<>();
    Main.GlobalDict = new HashMap<String, ArrayList<HashMap>>();
    Main.initialize_gdd(Main.GlobalDict,Main.tableStatus);
    QueryInAction currentQuery = new QueryInAction();
    Query q = new Query();

    String createString = "CREATE TABLE COUNT (CountID int,Total int);";
    HashMap createData = QueryParser.parseQuery(createString);
    Main.createQuery(currentQuery, q, createData);

    String insertString = "INSERT INTO COUNT (Total) VALUES ('2147483647999');";
    HashMap insertData = QueryParser.parseQuery(insertString);
    Main.insertQuery(currentQuery, q, insertData, logtest);
  }

}