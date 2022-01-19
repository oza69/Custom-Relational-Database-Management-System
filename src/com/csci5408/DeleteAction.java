package com.csci5408;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;


public class DeleteAction implements Action
{
    Query q;
    HashMap<String, ArrayList<HashMap>> gdd;
    HashMap<String, String> values_to_delete;

    public DeleteAction(Query q, HashMap<String, ArrayList<HashMap>> globalDict, HashMap<String, String> data) {
        this.q = q;
        this.gdd = globalDict;
        this.values_to_delete = data;
    }


    @Override
    public void execute() throws IOException, ClassNotFoundException, ParseException {
        q.delete(this.gdd,this.values_to_delete);

    }
}
