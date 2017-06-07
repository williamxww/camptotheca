package com.bow.camptotheca.hdfs.utils;


import java.text.SimpleDateFormat;
import java.util.Date;

public class DateUtils {
    private static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmssSSS");
    public static String getDate(){
        return dateFormat.format(new Date());
    }
}
