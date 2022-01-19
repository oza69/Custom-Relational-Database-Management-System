package com.csci5408;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class QueryParser {
  public static HashMap parseQuery(String query) {
    if(query.charAt(query.length() - 1) == ';') {
      query = query.substring(0, query.length() - 1);
    }
    String[] elementArray = query.split(" ");
    HashMap result = new HashMap();
    switch (elementArray[0].toLowerCase()) {
      case "select":
        result = parseSelect(query);
        break;
      case "create":
        result = parseCreateTable(query);
        break;
      case "insert":
        result = parseInsertInto(query);
        break;
      case "update":
        result = parseUpdate(query);
        break;
      case "delete":
        result = parseDelete(query);
        break;
      case "quit":
        break;
      case "get_sql_dump":
        break;
      case "erd":
        break;
      case "begin_transaction":
        break;
      case "end_transaction":
        break;
      default:
        System.out.println("Cannot recognize operation.");
    }
    result.put("queryType", elementArray[0].toLowerCase());
    return result;
  }

  private static HashMap parseDelete(String query) {
    String[] elementArray = query.split(" ");
    HashMap data = new HashMap();
    data.put("tableName", elementArray[2]);
    Matcher conditionMatcher = Pattern.compile("([\\w]*)( )*=( )*('([\\w ]*)'|([\\w]*))").matcher(query);
    ArrayList<HashMap> conditions = new ArrayList();
    while (conditionMatcher.find()) {
      String[] values = conditionMatcher.group().split("=");
      String recordKey = values[0].trim();
      String recordValue = values[1].trim();
      if(recordValue.charAt(0) == '\'') {
        recordValue = recordValue.substring(1, recordValue.length() - 1);
      }
      HashMap fieldMap = new HashMap();
      fieldMap.put("columnName", recordKey);
      fieldMap.put("value", recordValue);
      conditions.add(fieldMap);
    }
    data.put("condition", conditions);
    return data;
  }

  private static HashMap parseUpdate(String query) {
    String[] elementArray = query.split(" ");
    HashMap data = new HashMap();
    data.put("tableName", elementArray[1]);

    Matcher valueMatcher = Pattern.compile("([\\w]*)( )*=( )*([\\w \\']*)").matcher(query.split("(?i)where")[0]);
    ArrayList<HashMap> fields = new ArrayList();
    while (valueMatcher.find()) {
      String[] values = valueMatcher.group().split("=");
      String recordKey = values[0].trim();
      String recordValue = values[1].trim();
      if(recordValue.charAt(0) == '\'') {
        recordValue = recordValue.substring(1, recordValue.length() - 1);
      }
      HashMap fieldMap = new HashMap();
      fieldMap.put("columnName", recordKey);
      fieldMap.put("value", recordValue);
      fields.add(fieldMap);
    }
    data.put("columnValue", fields);

    Matcher conditionMatcher = Pattern.compile("([\\w]*)( )*=( )*('([\\w ]*)'|([\\w]*))").matcher(query.split("(?i)where")[1]);
    ArrayList<HashMap> conditions = new ArrayList();
    while (conditionMatcher.find()) {
      String[] values = conditionMatcher.group().split("=");
      String recordKey = values[0].trim();
      String recordValue = values[1].trim();
      if(recordValue.charAt(0) == '\'') {
        recordValue = recordValue.substring(1, recordValue.length() - 1);
      }
      HashMap fieldMap = new HashMap();
      fieldMap.put("columnName", recordKey);
      fieldMap.put("value", recordValue);
      conditions.add(fieldMap);
    }
    data.put("condition", conditions);
    return data;
  }

  private static HashMap parseInsertInto(String query) {
    String[] elementArray = query.split(" ");
    HashMap data = new HashMap();
    data.put("tableName", elementArray[2]);

    // Get column name and values bracket
    Matcher p = Pattern.compile("\\(([\\w|\\*|\\, \\')]*)\\)").matcher(query);
    String columnName = "", values = "";
    ArrayList<String> columnArray = new ArrayList(), valuesArray = new ArrayList();

    // Extract columns from query bracket
    if (p.find()) {
      columnName = p.group().substring(1, p.group().length() - 1);
      Matcher columnNameMatcher = Pattern.compile("([\\w)]*)").matcher(columnName);
      while (columnNameMatcher.find()) {
        if (columnNameMatcher.group().length() > 0) {
          columnArray.add(columnNameMatcher.group());
        }
      }
    }

    // Extract values from query bracket
    if (p.find()) {
      values = p.group().substring(1, p.group().length() - 1);
      Matcher valueMatcher = Pattern.compile("([\\w)]*)").matcher(values);
      while (valueMatcher.find()) {
        if (valueMatcher.group().length() > 0) {
          valuesArray.add(valueMatcher.group());
        }
      }
    }
    if(columnArray.size() != valuesArray.size()) {
      return null;
    } else {
      // Create array of column name and values
      ArrayList valueList = new ArrayList();
      for (int i = 0; i < columnArray.size(); i++) {
        HashMap<String, String> fields = new HashMap();
        fields.put("columnName", columnArray.get(i));
        fields.put("value", valuesArray.get(i));
        valueList.add(fields);
      }
      data.put("columnValue", valueList);
    }
    return data;
  }

  private static HashMap parseCreateTable(String query) {
    String[] elementArray = query.split(" ");
    HashMap data = new HashMap();
    data.put("tableName", elementArray[2]);

    // Get columns bracket data from query
    Matcher p = Pattern.compile("\\(([\\w|\\*|\\, \\\\(\\\\)]*)\\)").matcher(query);
    if (p.find()) {
      String columnString = p.group().substring(1, p.group().length() - 1);

      // Get array of column string
      String[] columnStringArray = columnString.split(",");
      ArrayList<HashMap> finalColumnList = new ArrayList<>();
      String primaryColumn = "";
      for (String column : columnStringArray) {
        // Get 3 group of string for each column(column_name data_type(data_length)) and add details hashmap to 1 arraylist
        Matcher columnMatch = Pattern.compile("([\\w|\\*|\\,]*)").matcher(column.trim());
        HashMap<String, String> columnData = new HashMap<>();
        if (columnMatch.find()) {
          // If primary key constraint then store primary key column
          if(columnMatch.group().toLowerCase().equals("primary")) {
            columnMatch.find();
            columnMatch.find();
            columnMatch.find();
            columnMatch.find();
            System.out.println(columnMatch.group());
            primaryColumn = columnMatch.group();
            continue;
          } else {
            columnData.put("column_name", columnMatch.group());
            columnMatch.find();
          }
        }
        if (columnMatch.find()) {
          columnData.put("data_type", columnMatch.group());
          columnMatch.find();
        }
        if (columnMatch.find()) {
          columnData.put("data_length", columnMatch.group());
          columnMatch.find();
        }
        if (!columnData.get("column_name").isEmpty()) {
          finalColumnList.add(columnData);
        }
      }
      if(primaryColumn != null && !primaryColumn.equals("")) {
        for (HashMap col : finalColumnList) {
          if(col.get("column_name").equals(primaryColumn)) {
            col.put("is_primary_key", true);
          }
        }
      }
      data.put("columnList", finalColumnList);
    }
    return data;
  }

  private static HashMap parseSelect(String query) {
    String[] elementArray = query.split(" ");
    HashMap data = new HashMap();

    data.put("tableName", elementArray[3]);
    if (elementArray[1].equals("*")) {
      data.put("allData", true);
    } else {
      data.put("columns", elementArray[1].split(","));
    }
    String[] conditionPart = query.split("(?i)where");
    if(conditionPart.length > 1) {
      Matcher conditionMatcher = Pattern.compile("([\\w]*)( )*=( )*('([\\w ]*)'|([\\w]*))").matcher(conditionPart[1]);
      ArrayList<HashMap> conditions = new ArrayList();
      while (conditionMatcher.find()) {
        String[] values = conditionMatcher.group().split("=");
        String recordKey = values[0].trim();
        String recordValue = values[1].trim();
        if(recordValue.charAt(0) == '\'') {
          recordValue = recordValue.substring(1, recordValue.length() - 1);
        }
        HashMap fieldMap = new HashMap();
        fieldMap.put("columnName", recordKey);
        fieldMap.put("value", recordValue);
        conditions.add(fieldMap);
      }
      data.put("condition", conditions);
    }
    return data;
  }
}
