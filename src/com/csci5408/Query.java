package com.csci5408;
import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class Query {

  static Object lock = new Object(); //
  final int INT_MAX = 2147483647;
  final int INT_MIN = -2147483648;
  SimpleDateFormat validFormat1 = new SimpleDateFormat("MM-dd-yyyy HH:mm:ss");
  GeneralLog generalLog;


  {
    try {
      generalLog = new GeneralLog("database/logs/General.txt");
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public void select(HashMap<String, ArrayList<HashMap>> globalDict, HashMap table_metadata) throws IOException, ClassNotFoundException {

    /* element Array will be null when query is Select * from table_name */
    String[] elementArray = (String[]) table_metadata.get("columns");

    long QueryStart = System.currentTimeMillis(); //Start time

    //Filtering values

    List<HashMap> column_data = this.deserialize_table((table_metadata.get("tableName").toString().replaceAll("[^a-zA-Z]", "")));

    // Validating Column exists or not
    int counter = 0;
    if (column_data != null) {// Check if column is present in Global Data Dict
      List<HashMap> global_data_column = globalDict.get(table_metadata.get("tableName").toString().replaceAll("[^a-zA-Z]", ""));
      int global_loop = global_data_column.size();
      if (elementArray != null) {
        for (int j = 0; j < global_loop; j++) {
          for (int i = 0; i < elementArray.length; i++) {
            HashMap global_name = global_data_column.get(j);
            String global_name_value = global_name.get("column_name").toString();
            if (global_name_value.equals(elementArray[i])) {
              counter++;
            }
          }
        }
        // if column names are valid then print it
        if (counter == elementArray.length) {
          System.out.println("==========================");
          for (int k = 0; k < column_data.size(); k++) {
            boolean whereConditionFullfillment = true;
            ArrayList<HashMap> conditions = (ArrayList) table_metadata.getOrDefault("condition", null);
            if (conditions != null) {
              for (HashMap condition : conditions) {
                if (!column_data.get(k).get(condition.get("columnName")).equals(condition.get("value"))) {
                  whereConditionFullfillment = false;
                  break;
                }
              }
            }
            if (whereConditionFullfillment) {
              for (int i = 0; i < elementArray.length; i++) {
                {
                  System.out.println("Column | " + elementArray[i] + " : " + column_data.get(k).get(elementArray[i]) + " : ");
                }
              }
              System.out.println("==========================");
            }
          }
        } else {
          System.out.println("Incorrect Column Name or Value provided or Column does not exist!");
        }
      } else {
        System.out.println("==========================");
        for (int i = 0; i < column_data.size(); i++) {
          boolean whereConditionFullfillment = true;
          ArrayList<HashMap> conditions = (ArrayList) table_metadata.getOrDefault("condition", null);
          if (conditions != null) {
            for (HashMap condition : conditions) {
              if (!column_data.get(i).get(condition.get("columnName")).equals(condition.get("value"))) {
                whereConditionFullfillment = false;
                break;
              }
            }
          }
          if (whereConditionFullfillment) {
            Iterator value_iterator = column_data.get(i).entrySet().iterator();
            while (value_iterator.hasNext()) {
              Map.Entry mapElement = (Map.Entry) value_iterator.next();
              System.out.println("Column | " + mapElement.getKey() + " : " + mapElement.getValue() + " : ");
            }
            System.out.println("==========================");
          }
        }
      }
    } else {
      System.out.println("Table Does not Exist!");
    }
    long QueryEnd = System.currentTimeMillis();  //End time
    System.out.println("Query Processed in " + (QueryEnd - QueryStart) + " ms");
    generalLog.logger.info("Select Query Processed in " + (QueryEnd - QueryStart) + " ms");
  }

  public void update(HashMap<String, ArrayList<HashMap>> globalDict, HashMap table_metadata) throws IOException, ClassNotFoundException, ParseException {

    /* element Array will be null */
    List elementArray = (List) table_metadata.get("columnValue");
    List<HashMap> newFile = new ArrayList<>();
    long QueryStart = System.currentTimeMillis(); //Start time

    //Filtering values
    synchronized (lock) {
      List<HashMap> column_data = this.deserialize_table((table_metadata.get("tableName").toString().replaceAll("[^a-zA-Z]", "")));

      List<HashMap> global_data_column = globalDict.get(table_metadata.get("tableName").toString().replaceAll("[^a-zA-Z]", ""));

      // Initialization
      int global_loop = global_data_column.size();
      int counter = 0;
      int validData = 0;

      // Column name valdiation
      if (elementArray != null) {
        for (int j = 0; j < global_loop; j++) {
          for (int i = 0; i < elementArray.size(); i++) {
            HashMap global_name = global_data_column.get(j);
            String global_name_value = global_name.get("column_name").toString();
            HashMap intermediate = (HashMap) elementArray.get(i);
            if (global_name_value.equals(intermediate.get("columnName"))) {
              counter++;
            }
          }
        }

        // Value insertation loop
        if (counter == elementArray.size()) {
          HashMap currentData = new HashMap();
          HashMap valuesToUpdate;
          List columnToUpdate = (List) table_metadata.get("condition");
          List temp = (List) table_metadata.get("columnValue");
          for (int k = 0; k < column_data.size(); k++) {
            HashMap valueToCompare = column_data.get(k);
            for (int j = 0; j < global_data_column.size(); j++) {
              HashMap intermediate = global_data_column.get(j);

              for (int i = 0; i < columnToUpdate.size(); i++) {
                valuesToUpdate = (HashMap) columnToUpdate.get(i);
                if (intermediate.get("column_name").toString().equals(valuesToUpdate.get("columnName"))) {
                  if (valueToCompare.get(valuesToUpdate.get("columnName")).toString().equals(valuesToUpdate.get("value"))) {
                    if (intermediate.get("data_type").equals("int") && (Integer.valueOf(valuesToUpdate.get("value").toString()) < INT_MAX) && ((Integer.valueOf(valuesToUpdate.get("value").toString()) >= INT_MIN))) {
                      validData++;
                      break;
                    } else if (intermediate.get("data_type").toString().contains("varchar") && ((valuesToUpdate.get("value").toString()).length() < Integer.valueOf(intermediate.get("data_length").toString()))) {
                      validData++;
                      break;
                    } else if (intermediate.get("data_type").toString().contains("datetime")) {
                      validFormat1.parse((valuesToUpdate.get("value").toString()));
                      validData++;
                      break;
                    } else {
                      System.out.println("Out of bounds! Please check the input length");
                      return;
                    }
                  }
                }
              }
            }
            // For continous flow checking
            if (validData != columnToUpdate.size()) {
              validData = 0;
            } else {
              break;
            }

          }
          if (validData == columnToUpdate.size()) // All data is valid
          {

            for (int i = 0; i < column_data.size(); i++) {
              HashMap local = (HashMap) column_data.get(i);
              //currentData = local;
              HashMap replace_val = new HashMap();
              int rowCounter = 0;
              for (int k = 0; k < columnToUpdate.size(); k++) {
                HashMap rowToUpdate = (HashMap) columnToUpdate.get(k);
                if (local.containsValue(rowToUpdate.get("value"))) {
                  rowCounter++;
                }
              }
              if (rowCounter == validData) {
                for (int l = 0; l < temp.size(); l++) {
                  replace_val = (HashMap) temp.get(l);
                  column_data.get(i).replace(replace_val.get("columnName"), replace_val.get("value"));
                  this.serialize_table(table_metadata.get("tableName").toString(), column_data);
                  break;
                }
                break;
              }
            }
          } else {
            System.out.println("Incorrect Column Name provided or Column does not exist!");
          }
        } else {
          System.out.println("Something is wrong! Check the elements");
        }
      }
      long QueryEnd = System.currentTimeMillis();  //End time
      System.out.println("Query Processed in " + (QueryEnd - QueryStart) + " ms");
      generalLog.logger.info("Update Query Processed in " + (QueryEnd - QueryStart) + " ms");
    }
  }

  public void delete(HashMap<String, ArrayList<HashMap>> globalDict, HashMap table_metadata) throws IOException, ParseException, ClassNotFoundException {
    /* element Array will be null when query is Select * from table_name */
    List elementArray = (List) table_metadata.get("condition");

    long QueryStart = System.currentTimeMillis(); //Start time

    //Filtering values
    synchronized (lock) {
      List<HashMap> column_data = this.deserialize_table((table_metadata.get("tableName").toString().replaceAll("[^a-zA-Z]", "")));

      List<HashMap> global_data_column = globalDict.get(table_metadata.get("tableName").toString().replaceAll("[^a-zA-Z]", ""));

      // Initialization
      int global_loop = global_data_column.size();
      int counter = 0;
      int validData = 0;

      if (elementArray != null) {
        for (int j = 0; j < global_loop; j++) {
          for (int i = 0; i < elementArray.size(); i++) {
            HashMap global_name = global_data_column.get(j);
            String global_name_value = global_name.get("column_name").toString();
            HashMap intermediate = (HashMap) elementArray.get(i);
            if (global_name_value.equals(intermediate.get("columnName"))) {
              counter++;
            }
          }
        }

        if (counter == elementArray.size()) {
          HashMap currentData = new HashMap();
          HashMap valuesToUpdate;
          List columnToUpdate = (List) table_metadata.get("condition");
          List temp = (List) table_metadata.get("columnValue");
          for (int k = 0; k < column_data.size(); k++) {
            HashMap valueToCompare = column_data.get(k);
            for (int j = 0; j < global_data_column.size(); j++) {
              HashMap intermediate = global_data_column.get(j);

              for (int i = 0; i < columnToUpdate.size(); i++) {
                valuesToUpdate = (HashMap) columnToUpdate.get(i);
                if (intermediate.get("column_name").toString().equals(valuesToUpdate.get("columnName"))) {
                  if (valueToCompare.get(valuesToUpdate.get("columnName")).toString().equals(valuesToUpdate.get("value"))) {
                    validData++;
                  }
                }
              }
            }
            // For continous flow checking
            if (validData != columnToUpdate.size()) {
              validData = 0;
            } else {
              break;
            }
          }

          if (validData == columnToUpdate.size()) // All data is valid
          {

            for (int i = 0; i < column_data.size(); i++) {
              HashMap local = (HashMap) column_data.get(i);
              HashMap replace_val = new HashMap();
              int rowCounter = 0;
              for (int k = 0; k < columnToUpdate.size(); k++) {
                HashMap rowToUpdate = (HashMap) columnToUpdate.get(k);
                if (local.containsValue(rowToUpdate.get("value"))) {
                  rowCounter++;
                }
              }
              if (rowCounter == validData) {
                for (int l = 0; l < elementArray.size(); l++) {
                  replace_val = (HashMap) elementArray.get(l);
                  column_data.remove(i);
                  this.serialize_table(table_metadata.get("tableName").toString(), column_data);
                  break;
                }
                break;
              }
            }
          } else {
            System.out.println("Incorrect Column Name or Value provided or Column does not exist!");
          }
        } else {
          System.out.println("Incorrect Column Name or Value provided or Column does not exist!");
        }
      } else {
        System.out.println("Dummy");
      }
    }
    long QueryEnd = System.currentTimeMillis();  //End time
    System.out.println("Query Processed in " + (QueryEnd - QueryStart) + " ms");
    generalLog.logger.info("Delete Query Processed in " + (QueryEnd - QueryStart) + " ms");

  }


  private List<HashMap> deserialize_table(String name) throws IOException, ClassNotFoundException {

    String table_name = "database/" + name + ".table";
    try {
      FileInputStream table_file = new FileInputStream(table_name);
      ObjectInputStream deserialized_table = new ObjectInputStream(table_file);
      List<HashMap> table_data = (List<HashMap>) deserialized_table.readObject();
      return table_data;
    } catch (IOException e) {
    } catch (ClassNotFoundException e) {
    }
    return null;
  }

  public void create(HashMap<String, ArrayList<HashMap>> globalDict, HashMap data) throws IOException, ClassNotFoundException {

    String table_name; //Initialization stuff
    List entire_table;
    long QueryStart = System.currentTimeMillis(); //Start time
    List<HashMap> init_table = new ArrayList<>();
    table_name = data.get("tableName").toString(); //fetch table name

    //Check table Name if it exists
      if(globalDict.containsKey((table_name)))
      {
        System.out.println("Table already Exists in the System");
        System.out.println("Trying creating with a unique name or use the existing ones");
        return;
      }

    globalDict.put(table_name, (ArrayList<HashMap>) data.get("columnList"));//set fields in gdd

    /* If GDD is not created then create and add data */
    /* Else add data to the existing GDD */
    entire_table = this.deserialize_table("global");
    if (entire_table == null) {
      entire_table = new ArrayList<HashMap>();
      entire_table.add(globalDict);
    } else {
      entire_table.add(globalDict);
    }
    this.serialize_table("global", entire_table);
    this.serialize_table(table_name, init_table);
    long QueryEnd = System.currentTimeMillis();  //End time
    System.out.println("Query Processed in " + (QueryEnd - QueryStart) + " ms");
    generalLog.logger.info("Create Query Processed in " + (QueryEnd - QueryStart) + " ms");
  }

  private void serialize_table(String name, Object column) throws IOException {
    String custom_name = "database/" + name + ".table";
    FileOutputStream table_file = new FileOutputStream(custom_name);
    ObjectOutputStream serialized_table = new ObjectOutputStream(table_file);
    serialized_table.writeObject(column);
    serialized_table.flush();
    serialized_table.close();
    table_file.close();
  }

  public void insert(HashMap<String, ArrayList<HashMap>> globalDict, HashMap table_metadata) throws IOException, ClassNotFoundException, ParseException {

    /* element Array will be null when query is Select * from table_name */
    List elementArray = (List) table_metadata.get("columnValue");

    long QueryStart = System.currentTimeMillis(); //Start time

    //Filtering values
    synchronized (lock) {
      List<HashMap> column_data = this.deserialize_table((table_metadata.get("tableName").toString().replaceAll("[^a-zA-Z]", "")));
      List<HashMap> global_data_column = globalDict.get(table_metadata.get("tableName").toString().replaceAll("[^a-zA-Z]", ""));

      // Initialization
      int global_loop = global_data_column.size();
      int counter = 0;
      int validData = 0;
      boolean pk = false;
      boolean has_key = false;
      String name = null;

      // Verification in case of primary key
      for (int j = 0; j < global_loop; j++) {
        HashMap global_name = global_data_column.get(j);
        if (global_name.containsKey("is_primary_key")) {
          has_key = true;
          name = (String) global_name.get("column_name");
          for (int o = 0; o < elementArray.size(); o++) {
            HashMap verifyColumn = (HashMap) elementArray.get(o);
            if (verifyColumn.get("columnName").equals(name)) {
              pk = true;
              for (int i = 0; i < column_data.size(); i++) {
                HashMap checkexist = column_data.get(i);
                if (checkexist.containsValue(verifyColumn.get("value"))) {
                  System.out.println("Duplicate Key Found!");
                  System.out.println("Enter a unique key!");
                  return;
                }
              }
            }
          }
        }
      }

      if (!pk & has_key) {
        System.out.println("Please provide the value for primary Key" + name);
        System.out.println("Currently it is not set to Auto-increment");
        return;
      }
      if (elementArray != null) {
        for (int j = 0; j < global_loop; j++) {
          for (int i = 0; i < elementArray.size(); i++) {
            HashMap global_name = global_data_column.get(j);
            String global_name_value = global_name.get("column_name").toString();
            HashMap intermediate = (HashMap) elementArray.get(i);
            if (global_name_value.equals(intermediate.get("columnName"))) {
              counter++;
            }
          }
        }

        if (counter == elementArray.size()) {
          HashMap currentData = new HashMap();
          HashMap valuesToInsert;
          for (int j = 0; j < global_data_column.size(); j++) {
            HashMap intermediate = global_data_column.get(j);
            for (int i = 0; i < elementArray.size(); i++) {
              valuesToInsert = (HashMap) elementArray.get(i);

              if (intermediate.get("column_name").toString().equals(valuesToInsert.get("columnName"))) {
                if (intermediate.get("data_type").equals("int") && (Integer.valueOf(valuesToInsert.get("value").toString()) < INT_MAX) && ((Integer.valueOf(valuesToInsert.get("value").toString()) >= INT_MIN))) {
                  currentData.put(valuesToInsert.get("columnName"), valuesToInsert.get("value"));
                  validData++;
                  break;
                } else if (intermediate.get("data_type").toString().contains("varchar") && ((valuesToInsert.get("value").toString()).length() < Integer.valueOf(intermediate.get("data_length").toString()))) {
                  currentData.put(valuesToInsert.get("columnName"), valuesToInsert.get("value"));
                  validData++;
                  break;
                } else if (intermediate.get("data_type").toString().contains("datetime")) {
                  validFormat1.parse((valuesToInsert.get("value").toString()));
                  currentData.put(valuesToInsert.get("columnName"), valuesToInsert.get("value"));
                  validData++;
                  break;
                } else {
                  System.out.println("Out of bounds! Please check the input length");
                  return;
                }
              } else {
                currentData.put(intermediate.get("column_name"), null);
              }

            }
          }
          if (validData == counter) // All data is valid
          {
            column_data.add(currentData);
            this.serialize_table(table_metadata.get("tableName").toString(), column_data);
          }
        } else {
          System.out.println("Incorrect Column Name or Value provided or Column does not exist!");
        }
      } else {
        System.out.println("Enter Valid Data");
      }
    }
    long QueryEnd = System.currentTimeMillis();  //End time
    System.out.println("Query Processed in " + (QueryEnd - QueryStart) + " ms");
    generalLog.logger.info("Insert Query Processed in " + (QueryEnd - QueryStart) + " ms");

  }

  public void generateDump(HashMap<String, ArrayList<HashMap>> gdd) throws ClassNotFoundException, IOException {
    DateTimeFormatter dtf = DateTimeFormatter.ofPattern("dd-MM-yyyy-HH:mm");
    LocalDateTime now = LocalDateTime.now();
    String FileName = "database/dump/sql_dump" + dtf.format(now) + ".sql";
    FileWriter dumpFile = new FileWriter(new File(FileName));
    BufferedWriter outputStream = new BufferedWriter(dumpFile);

    for (Map.Entry<String, ArrayList<HashMap>> table : gdd.entrySet()) {
      ArrayList<HashMap> columns = table.getValue();
      List<HashMap> rows = this.deserialize_table(table.getKey());

      outputStream.write("CREATE TABLE " + table.getKey() + " (\n");
      for (HashMap column : columns) {
        String columnDump = column.get("column_name") + " " + column.get("data_type");
        if (column.get("data_length") == null) {
          columnDump = columnDump + ",\n";
        } else {
          columnDump = columnDump + "(" + column.get("data_length") + "),\n";
        }
        outputStream.write(columnDump);
      }
      outputStream.write(");\n\n");

      for (HashMap<String, String> row : rows) {
        String insertRow = "INSERT INTO " + table.getKey();
        String keyString = "(", valueString = "(";
        for (Map.Entry<String, String> rowEntry : row.entrySet()) {
          keyString = keyString + rowEntry.getKey() + ",";
          if (rowEntry.getValue() != null) {
            if (isIntColumn(rowEntry.getKey(), columns)) {
              valueString = valueString + rowEntry.getValue() + ",";
            } else {
              valueString = valueString + "'" + rowEntry.getValue() + "',";
            }
          } else {
            valueString = valueString + ",";
          }
        }
        keyString = keyString.substring(0, keyString.length() - 1) + ")";
        valueString = valueString.substring(0, valueString.length() - 1) + ")";
        insertRow = insertRow + " " + keyString + " VALUES " + valueString + ";\n";
        outputStream.write(insertRow);
      }
    }
    outputStream.flush();
    outputStream.close();
    dumpFile.close();
  }

  private boolean isIntColumn(String key, ArrayList<HashMap> columns) {
    boolean isInt = false;
    for (HashMap column : columns) {
      if (column.get("data_type").equals("int") && column.get("column_name").equals(key)) {
        isInt = true;
        break;
      }
    }
    return isInt;
  }
}
