package com.csci5408;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;


public class CreateAction implements Action
{
    Query q;
    HashMap<String,ArrayList<HashMap>> gdd;
    HashMap table_metadata;

    public CreateAction(Query q, HashMap<String, ArrayList<HashMap>> globalDict,HashMap data)
    {
        this.q = q;
        this.gdd = globalDict;
        this.table_metadata = data;
    }

    @Override
    public void execute() throws IOException, ClassNotFoundException {
         q.create(this.gdd, this.table_metadata);
    }
}
