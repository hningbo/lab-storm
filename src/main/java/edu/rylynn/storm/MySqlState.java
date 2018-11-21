package edu.rylynn.storm;

import org.apache.storm.trident.state.State;
import org.apache.storm.trident.state.map.IBackingMap;

import java.util.List;
/*
https://blog.csdn.net/jediael_lu/article/details/76794843
 */

public class MySqlState<T> implements IBackingMap<T> {
    private String location;



    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    @Override
    public List<T> multiGet(List<List<Object>> list) {
        return null;
    }

    @Override
    public void multiPut(List<List<Object>> list, List<T> list1) {

    }
}
