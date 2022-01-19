package com.csci5408;


import java.io.IOException;
import java.text.ParseException;

/*Action Interface */
public interface Action {
  void execute() throws IOException, ClassNotFoundException, ParseException;
}
