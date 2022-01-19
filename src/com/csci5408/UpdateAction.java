package com.csci5408;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;

public class UpdateAction implements Action
{
    Query q;
    HashMap<String, ArrayList<HashMap>> gdd;
    HashMap<String, String> values_to_update;

    public UpdateAction(Query q, HashMap<String, ArrayList<HashMap>> globalDict, HashMap<String, String> data) {
        this.q = q;
        this.gdd = globalDict;
        this.values_to_update = data;
    }


    @Override
    public void execute() throws IOException, ClassNotFoundException, ParseException {
         q.update(this.gdd,this.values_to_update);

    }
}
