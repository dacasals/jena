package org.utfsm.utils;
import org.apache.jena.atlas.json.JsonString;
import org.apache.jena.sparql.algebra.op.OpSlice;
import org.apache.jena.tdb.store.Hash;
import org.json.simple.JSONArray;

import java.util.ArrayList;
import java.util.HashMap;

public class BTNode<T>
{
    public T data;
    public BTNode<T> left;
    public BTNode<T> right;
    public BTNode(T item)
    {
        data = item;
        left = right = null;
    }

    @Override
    public String toString() {
        JSONArray json = new JSONArray();


        if(data instanceof HashMap){
            if(((HashMap<?, ?>) data).containsKey("name")){
                String name  = (String) ((HashMap<?, ?>) data).get("name");
                if(name.equals("slice") || name.equals("top") ){
                    String start  = (String) ((HashMap<?, ?>) data).get("start");
                    String limit  = (String) ((HashMap<?, ?>) data).get("limit");
                    json.add(new JsonString(name.concat("ᶲ").concat(start).concat("ᶲ").concat(limit)));
                }
                else {
                    json.add(new JsonString(name));
                }

            }else{
                json.add(new JsonString(((HashMap<String, ArrayList>) data).get("tpf_type").get(0).toString()));
            }
        }
        else {
            json.add(new JsonString(data.toString()));
        }
        //Add Children
        JSONArray children = new JSONArray();
        if(this.left != null){
            children.add(this.left);
        }else {
            children.add(new JsonString("NONE"));
        }
        if(this.right != null){
            children.add(this.right);
        }else {
            children.add(new JsonString("NONE"));
        }
        json.add(children);
        return  json.toString();
    }
}