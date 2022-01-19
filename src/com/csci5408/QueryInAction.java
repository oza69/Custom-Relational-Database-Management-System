package com.csci5408;

import java.io.IOException;
import java.text.ParseException;


public class QueryInAction {
  Action a;


  public QueryInAction() {

  }

  public void setAction(Action a) {
    this.a = a;
  }

  public void triggerquery() throws IOException, ClassNotFoundException, ParseException {
    a.execute();

  }
}
