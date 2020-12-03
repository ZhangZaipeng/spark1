package com.exmaple.common;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

public class Utils {
    //2019-03-23T00:22:00.000Z

    public static void main(String[] args) throws ParseException {
        String oldDateStr = "2018-12-01T04:07:00.000Z";
        //此格式只有  jdk 1.7才支持  yyyy-MM-dd'T'HH:mm:ss.SSSXXX

        DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");  //yyyy-MM-dd'T'HH:mm:ss.SSSZ
        Date date = df.parse(oldDateStr);
        System.out.println(date);

        SimpleDateFormat df1 = new SimpleDateFormat("EEE MMM dd HH:mm:ss Z yyyy", Locale.UK);
        System.out.println(df1.parse(date.toString()));
        System.out.println(df1.format(date));

        DateFormat df2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        System.out.println(df2.format(date));
    }

}
