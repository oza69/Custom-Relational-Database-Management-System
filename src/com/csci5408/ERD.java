package com.csci5408;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class ERD {
  HashMap<String, ArrayList<HashMap>> GlobalDict;

  public ERD(HashMap<String, ArrayList<HashMap>> globalDict) {
    GlobalDict = globalDict;
  }

  public void GenerateERD(List Name, List table, List constraints) {
    try {
      if (linkTables(Name)) {
        File file = new File("database/ERD/EntityRelationshipDiagram.txt");
        BufferedWriter erdWriter = new BufferedWriter(new FileWriter(file));
        Iterator<Map.Entry<String, ArrayList<HashMap>>> gdd_iterator = GlobalDict.entrySet().iterator();
        List erd_constraints = constraints;
        List tableName = table;
        boolean pk = false;
        while (gdd_iterator.hasNext()) {
          Map.Entry<String, ArrayList<HashMap>> Listentry = gdd_iterator.next();
          String tname = Listentry.getKey();
          List allColumns = Listentry.getValue();
          erdWriter.write("Table: " + tname);
          erdWriter.newLine();
          //primary Key and Foreign key
          for(int c = 0; c < erd_constraints.size();c++) {
            if (tname.equals(tableName.get(c)) && pk==false) {
              erdWriter.write("Primary Key |" + erd_constraints.get(c));
              erdWriter.newLine();
              pk = true;
            }
            else if(tname.equals(tableName.get(c)))
            {
              erdWriter.write("Foreign Key |" + erd_constraints.get(c));
              erdWriter.newLine();
            }
          }
          erdWriter.write("ColumnName |  DataType");
          erdWriter.newLine();
          erdWriter.write("----------------------------------------------");
          erdWriter.newLine();
          for(int i = 0; i < allColumns.size();i++) {
            HashMap erd = (HashMap) allColumns.get(i);
            Iterator value_iterator = erd.entrySet().iterator();
            while (value_iterator.hasNext()) {
              Map.Entry mapElement = (Map.Entry) value_iterator.next();
              if(mapElement.getKey().toString().equals("column_name") || mapElement.getKey().toString().equals("data_type")) {
                erdWriter.write(mapElement.getValue().toString() + "   ");
              }
            }
            erdWriter.newLine();
          }
          erdWriter.write("==============================================");
          erdWriter.newLine();
          pk = false;
        }
        System.out.println("ERD Generated Successfully!");
        erdWriter.flush();
      }
    }
    catch (IOException e) {
      e.printStackTrace();
    }

  }

  public boolean linkTables(List Name) {
    Iterator<Map.Entry<String, ArrayList<HashMap>>> gdd_iterator = GlobalDict.entrySet().iterator();
    boolean exist = false;
    while (gdd_iterator.hasNext()) {
      Map.Entry<String, ArrayList<HashMap>> listEntry = gdd_iterator.next();
      for (int i = 0; i < Name.size() ; i++) {
        if (listEntry.getKey().equals(Name.get(i))) {
          exist = true;
        }
      }
      if(!exist)
      {
        //System.out.println("Some Tables does not exist!");
      }
    }
    return exist;
  }
}
