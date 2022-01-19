package test;

import com.csci5408.*;
import org.junit.jupiter.api.Test;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

public class MainTest {

  static List<String> actions = Collections.synchronizedList(new ArrayList<String>()) ;

  public static class concurrentRun1 implements Runnable {
    EventLog logtest2;

    {
      try {
        logtest2 = new EventLog("database/logs/Event.txt");
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    @Override
    public void run() {
      actions.add("Thread 1 is inserting value");
      Main.tableStatus = new HashMap<>();
      Main.GlobalDict = new HashMap<>();
      try {
        Main.initialize_gdd(Main.GlobalDict, Main.tableStatus);
        QueryInAction currentQuery1 = new QueryInAction();
        Query q1 = new Query();
        String queryString = "INSERT INTO Persons (LastName, FirstName) VALUES ('Smith', 'Steve');";
        HashMap parsedData = QueryParser.parseQuery(queryString);
        Main.insertQuery(currentQuery1, q1,parsedData, logtest2);
        actions.add("Thread 1 is insertion complete");
      } catch (IOException e) {
        e.printStackTrace();
      } catch (ParseException e) {
        e.printStackTrace();
      } catch (ClassNotFoundException e) {
        e.printStackTrace();
      }
    }
  }

  public static class concurrentRun2 implements Runnable {

    EventLog logtest2;
    {
      try {
        logtest2 = new EventLog("database/logs/Event.txt");
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    @Override
    public void run() {
      actions.add("Thread 2 is inserting value");
      Main.tableStatus = new HashMap<>();
      Main.GlobalDict = new HashMap<>();
      try {
        Main.initialize_gdd(Main.GlobalDict, Main.tableStatus);
        QueryInAction currentQuery2 = new QueryInAction();
        Query q2 = new Query();
        String queryString = "INSERT INTO Persons (LastName, FirstName) VALUES ('Kohli', 'Virat');";
        HashMap parsedData = QueryParser.parseQuery(queryString);
        Main.insertQuery(currentQuery2, q2, parsedData, logtest2);
        actions.add("Thread 2 is insertion complete");
      } catch (IOException e) {
        e.printStackTrace();
      } catch (ClassNotFoundException | ParseException e) {
        e.printStackTrace();
      }
    }
  }

  @Test
  public void locking() throws InterruptedException {
    Thread thread1 = new Thread(new concurrentRun1());
    Thread thread2 = new Thread(new concurrentRun2());
    thread1.start();
    thread2.start();
    thread2.join();
    thread1.join();
    for(String action:actions)
    {
      System.out.println(action);
    }
  }
}
