package com.csci5408;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

public class Main {

  //Global Data Dict
  public static HashMap<String, ArrayList<HashMap>> GlobalDict = new HashMap<String, ArrayList<HashMap>>();
  public static HashMap<String, String> tableStatus = new HashMap<>();
  private static ERD create_erd = new ERD(GlobalDict);
  public static List<String> tName = new ArrayList<>();
  public static List<String> constraints = new ArrayList<>();
  private static boolean transactionFlag = false;
  private static List<HashMap> queryQueue = new ArrayList<>();

  public static void main(String[] args) throws Exception {

    String user, pass;
    Scanner s = new Scanner(System.in);
    System.out.println("Enter username:");
    user = s.nextLine();
    System.out.println("Enter password:");
    pass = s.nextLine();

    MessageDigest md1 = MessageDigest.getInstance("MD5");
    byte[] messageDigest = md1.digest(pass.getBytes());
    BigInteger no = new BigInteger(1, messageDigest);
    String hashtext = no.toString(16);
    while (hashtext.length() < 32) {
      hashtext = "0" + hashtext;
    }

    String configFile = user + ".txt";

    //to check whether configFile is null or empty
    if (!configFile.equals("") || !configFile.equals(null)) {
      try {
        Scanner scan;
        scan = new Scanner(new File(configFile));
        List<String> list = new ArrayList<>();
        while (scan.hasNextLine()) {
          String str = scan.nextLine();
          list.add(str);
        }
        if (list.size() != 4) {
          System.out.println("Invalid Content in configFile !!");
          throw new Exception();
        }

        //declaring username and password as string
        String username = "";
        String password = "";
        String question = "";
        String answer = "";
        for (String str_detail : list) {
          int index = str_detail.indexOf("=");

          if (str_detail.contains("username")) {
            username = str_detail.substring(index + 1);
          } else if (str_detail.contains("password")) {
            password = str_detail.substring(index + 1);
          } else if (str_detail.contains("question")) {
            question = str_detail.substring(index + 1);
          } else if (str_detail.contains("answer")) {
            answer = str_detail.substring(index + 1);
          } else {
            System.out.println("Content Format Incorrect !!");
            throw new Exception();
          }

          if (str_detail.contains("=")) {
            continue;
          } else {
            System.out.println("Content Format Incorrect !!");
            throw new Exception();
          }
        }

        //if username and password are not null concat them
        if (!username.equals(null) && !password.equals(null) && !username.equals(" ") && !password.equals(" ") &&
            !question.equals(null) && !answer.equals(null) && !question.equals(" ") && !answer.equals(" ")) {


          if (user.equals(username) && hashtext.equals(password)) {
            String response;
            Scanner scanner1 = new Scanner(System.in);
            System.out.println("Security Question: ");
            System.out.println(question);
            response = scanner1.nextLine();
            if (response.equals(answer)) {

              QueryInAction currentQuery = new QueryInAction();
              Query q = new Query();

              QueryLog queryLog = new QueryLog("database/logs/Query.txt");
              EventLog eventLog = new EventLog("database/logs/Event.txt");

              Scanner operation = new Scanner(System.in);
              initialize_gdd(GlobalDict, tableStatus);
              while (true) {

                System.out.println("Enter Query: ");
                String sessionQuery = operation.nextLine();
                HashMap parsedData = QueryParser.parseQuery(sessionQuery);
                String sessionType = (String) parsedData.get("queryType");

                if (sessionType.equalsIgnoreCase("quit")) {
                  break;
                } else if (sessionType.equals("create")) {
                  //Create Query
                  if (transactionFlag) {
                    queryQueue.add(parsedData);
                  } else {
                    createQuery(currentQuery, q, parsedData);
                  }
                } else if (sessionType.equals("insert")) {
                  //Insert Query
                  if (transactionFlag) {
                    queryQueue.add(parsedData);
                  } else {
                    insertQuery(currentQuery, q, parsedData, eventLog);
                  }
                } else if (sessionType.equals("select")) {
                  //Select Query
                  if (transactionFlag) {
                    queryQueue.add(parsedData);
                  } else {
                    selectQuery(currentQuery, q, parsedData);
                  }
                } else if (sessionType.equals("update")) {
                  //Update Query
                  if (transactionFlag) {
                    queryQueue.add(parsedData);
                  } else {
                    updateQuery(currentQuery, q, parsedData, eventLog);
                  }
                } else if (sessionType.equals("delete")) {
                  //Delete Query
                  if (transactionFlag) {
                    queryQueue.add(parsedData);
                  } else {
                    deleteQuery(currentQuery, q, parsedData, eventLog);
                  }
                } else if (sessionType.equals("get_sql_dump")) {
                  currentQuery.setAction(new GenerateDumpAction(q, GlobalDict));
                  currentQuery.triggerquery();
                } else if (sessionType.equals("erd")) {
                  Scanner erd = new Scanner(System.in);
                  System.out.println("Enter : ");
                  String name = erd.next();
                  List<String> tableNames = new ArrayList<>();
                  int index = 0;
                  while (index < 2) {
                    tableNames.add(name);
                    System.out.println("Enter : ");
                    name = erd.next();
                    index++;
                  }
                  updateConstraints(GlobalDict);
                  tName.add(tableNames.get(1));
                  constraints.add(tableNames.get(0) + "_FK");
                  create_erd.GenerateERD(tableNames, tName, constraints);
                  clearConstraints();
                } else if (sessionType.equals("begin_transaction")) {
                  System.out.println("Begin transaction");
                  transactionFlag = true;

                } else if (sessionType.equals("end_transaction")) {
                  System.out.println("End transaction");
                  for (int i = 0; i < queryQueue.size(); i++) {
                    String transactionType = (String) queryQueue.get(i).get("queryType");

                    if (transactionType.equals("create")) {
                      createQuery(currentQuery, q, queryQueue.get(i));
                    } else if (transactionType.equals("insert")) {
                      insertQuery(currentQuery, q, queryQueue.get(i), eventLog);
                    } else if (transactionType.equals("update")) {
                      updateQuery(currentQuery, q, queryQueue.get(i), eventLog);
                    } else if(transactionType.equals("delete")) {
                      deleteQuery(currentQuery, q, queryQueue.get(i), eventLog);
                    } else if(transactionType.equals("select")){
                      selectQuery(currentQuery, q, queryQueue.get(i));
                    }
                  }
                  transactionFlag = false;
                  queryQueue = new ArrayList<>(); // reset
                }
                queryLog.logger.info(sessionQuery);
              }
            } else {
              System.out.println("Wrong Answer !!");
            }
          } else {
            System.out.println("Authentication Failed !!");
          }
        } else {
          System.out.println("Null value found in file for username and password !!");
          throw new Exception();
        }
      } catch (FileNotFoundException e) {
        System.out.println(e);
      } catch (IOException e) {
        System.out.println(e);
      }
    } else {
      System.out.println("Invalid Config File name !!");
    }

  }

  private static void selectQuery(QueryInAction currentQuery, Query q, HashMap parsedQuerydata) throws IOException, ClassNotFoundException, ParseException {
    currentQuery.setAction(new SelectAction(q, GlobalDict, parsedQuerydata));
    currentQuery.triggerquery();
  }

  public static void deleteQuery(QueryInAction currentQuery, Query q, HashMap parsedQuerydata, EventLog eventLog) throws IOException, ClassNotFoundException, ParseException {
    if (tableStatus.size() > 0) {
      if (tableStatus.get(parsedQuerydata.get("tableName")).equals("open")) {
        eventLog.logger.info(parsedQuerydata.get("tableName") + " status is locked");
        tableStatus.replace(parsedQuerydata.get("tableName").toString(), "close");
        currentQuery.setAction(new DeleteAction(q, GlobalDict, parsedQuerydata));
        currentQuery.triggerquery();
        tableStatus.replace(parsedQuerydata.get("tableName").toString(), "open");
        eventLog.logger.info(parsedQuerydata.get("tableName") + " status is open");//status open
      } else {
        eventLog.logger.info("Table is locked by other process!");
        System.out.println("Table is locked by other process!"); //status open
      }
    } else {
      eventLog.logger.info(parsedQuerydata.get("tableName") + " status is locked");
      currentQuery.setAction(new DeleteAction(q, GlobalDict, parsedQuerydata));
      currentQuery.triggerquery();
      eventLog.logger.info(parsedQuerydata.get("tableName") + " status is open");//status open
    }
  }

  public static void updateQuery(QueryInAction currentQuery, Query q, HashMap parsedQuerydata, EventLog eventLog) throws IOException, ClassNotFoundException, ParseException {
    if (tableStatus.size() > 0) {
      if (tableStatus.get(parsedQuerydata.get("tableName")).toString().equals("open")) {
        eventLog.logger.info(tableStatus.get(parsedQuerydata.get("tableName")) + " status is locked");
        tableStatus.replace(parsedQuerydata.get("tableName").toString(), "close");
        currentQuery.setAction(new UpdateAction(q, GlobalDict, parsedQuerydata));
        currentQuery.triggerquery();
        tableStatus.replace(parsedQuerydata.get("tableName").toString(), "open");
        eventLog.logger.info(parsedQuerydata.get("tableName") + " status is open");//status open
      } else {
        eventLog.logger.info("Table is locked by other process!");
        System.out.println("Table is locked by other process!"); //status open
      }
    } else {
      eventLog.logger.info(parsedQuerydata.get("tableName") + " status is locked");
      currentQuery.setAction(new UpdateAction(q, GlobalDict, parsedQuerydata));
      currentQuery.triggerquery();
      eventLog.logger.info(parsedQuerydata.get("tableName") + " status is open");//status open
    }
  }

  public static void insertQuery(QueryInAction currentQuery, Query q, HashMap parsedQuerydata, EventLog eventLog) throws IOException, ClassNotFoundException, ParseException {
    if (tableStatus.size() > 0) {
      if (tableStatus.get(parsedQuerydata.get("tableName")).equals("open")) {
        eventLog.logger.info(parsedQuerydata.get("tableName") + " status is locked");//status locked
        tableStatus.replace(parsedQuerydata.get("tableName").toString(), "close");
        currentQuery.setAction(new InsertAction(q, GlobalDict, parsedQuerydata));
        currentQuery.triggerquery();
        tableStatus.replace(parsedQuerydata.get("tableName").toString(), "open");
        eventLog.logger.info(parsedQuerydata.get("tableName") + " status is open");//status open
      } else {
        eventLog.logger.info("Table is locked by other process!");
        System.out.println("Table is locked by other process!"); //status open
      }
    } else {
      eventLog.logger.info(parsedQuerydata.get("tableName") + " status is locked");
      currentQuery.setAction(new InsertAction(q, GlobalDict, parsedQuerydata));
      currentQuery.triggerquery();
      eventLog.logger.info(parsedQuerydata.get("tableName") + " status is open");//status open
    }
  }

  public static void createQuery(QueryInAction currentQuery, Query q, HashMap parsedQuerydata) throws IOException, ClassNotFoundException, ParseException {
    currentQuery.setAction(new CreateAction(q, GlobalDict, parsedQuerydata));
    currentQuery.triggerquery();
  }

  // Initialize GDD with an already created Tables
  public static void initialize_gdd(HashMap<String, ArrayList<HashMap>> gdd, HashMap<String, String> tableStatus) throws ClassNotFoundException {
    String table_name = "database/global.table";
    try {
      FileInputStream table_file = new FileInputStream(table_name);
      ObjectInputStream deserialized_table = new ObjectInputStream(table_file);
      List table_data = (List) deserialized_table.readObject();
      for (int i = 0; i < table_data.size(); i++) {
        HashMap temp = (HashMap) table_data.get(i);
        Iterator value_iterator = temp.entrySet().iterator();
        while (value_iterator.hasNext()) {
          Map.Entry mapElement = (Map.Entry) value_iterator.next();
          gdd.put(mapElement.getKey().toString(), (ArrayList<HashMap>) mapElement.getValue());
          tableStatus.put(mapElement.getKey().toString(), "open");
        }
      }
      Iterator gdd_iterator = gdd.entrySet().iterator();
      while (gdd_iterator.hasNext()) {
        Map.Entry mapElement = (Map.Entry) gdd_iterator.next();
        tName.add(mapElement.getKey().toString());
        constraints.add(mapElement.getKey().toString() + "_PK");
      }
    } catch (IOException e) {
      System.out.println("No existing tables found, Create one");
    }
  }

  public static void updateConstraints(HashMap<String, ArrayList<HashMap>> gdd) {
    Iterator gdd_iterator = gdd.entrySet().iterator();
    while (gdd_iterator.hasNext()) {
      Map.Entry mapElement = (Map.Entry) gdd_iterator.next();
      tName.add(mapElement.getKey().toString());
      constraints.add(mapElement.getKey().toString() + "_PK");
    }
  }

  public static void clearConstraints() {
    for (int i = 0; i < constraints.size(); i++) {
      if (!constraints.get(i).contains("_FK")) {
        tName.remove(i);
        constraints.remove(i);
      }
    }
  }
}


