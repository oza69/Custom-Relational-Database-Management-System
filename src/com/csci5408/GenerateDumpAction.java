package com.csci5408;


import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;

public class GenerateDumpAction implements Action {
  Query q;
  HashMap<String, ArrayList<HashMap>> gdd;

  public GenerateDumpAction(Query q, HashMap<String, ArrayList<HashMap>> globalDict) {
    this.q = q;
    this.gdd = globalDict;
  }

  @Override
  public void execute() throws IOException, ClassNotFoundException, ParseException {
    q.generateDump(this.gdd);
  }
}
