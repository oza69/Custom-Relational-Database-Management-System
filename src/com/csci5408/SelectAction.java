package com.csci5408;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;


public class SelectAction implements Action
{
    Query q;
    HashMap column_name;
    HashMap<String, ArrayList<HashMap>> gdd;
    public SelectAction(Query q,HashMap<String, ArrayList<HashMap>> global_value, HashMap data)
    {
        this.q = q;
        this.column_name = data;
        this.gdd = global_value;

    }

    @Override
    public void execute() throws IOException, ClassNotFoundException {
        q.select(this.gdd,this.column_name);

    }
}
