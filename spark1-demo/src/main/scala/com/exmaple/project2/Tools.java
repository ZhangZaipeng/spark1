package com.exmaple.project2;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class Tools {

    /**
     * yyyy-MM-dd
     * @return
     */
    public static  String getYesterday(){

        Calendar calendar = Calendar.getInstance();
        calendar.setTime(new Date());
        calendar.add(Calendar.DAY_OF_YEAR , -1);
        Date yesterday = calendar.getTime();

        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        return simpleDateFormat.format(yesterday);
    }


    public static double  formatDouble(double num,int scale){
        BigDecimal bigDecimal = new BigDecimal(num);
        return bigDecimal.setScale(scale,BigDecimal.ROUND_HALF_UP).doubleValue();
    }

}
