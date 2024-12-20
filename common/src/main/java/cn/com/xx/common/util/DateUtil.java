package cn.com.xx.common.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.Date;

public class DateUtil {

    public static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm");

    /**
     * add mins
     * @param min
     * @return
     */
    public static String addMins(String time, int min) throws ParseException {

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        Calendar nowTime = Calendar.getInstance();
        nowTime.setTime(sdf.parse(time));
        nowTime.add(Calendar.MINUTE, min);
        return sdf.format(nowTime.getTime());

    }


    public static String substractHour(String datestr, int hour){

        SimpleDateFormat sdf= new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Calendar calendar = Calendar.getInstance();
        try {

            Date date = sdf.parse(datestr);
            calendar.setTime(date);
            calendar.add(Calendar.HOUR, hour);
        } catch (Throwable ignore) {
            // TODO 时间字符串转Calendar异常
        }


        return sdf.format(calendar.getTime());

    }

    public static Date addDay(int day){

        Calendar nowTime = Calendar.getInstance();
        nowTime.add(Calendar.DAY_OF_WEEK, day);
        return nowTime.getTime();

    }

    public static long subtract(Date time){

        long sec = (time.getTime() - new Date().getTime())/1000;

        return sec;

    }

    public static String getBirthdayByAge(int age){

        Calendar calendar = Calendar.getInstance();

        int currentYear = calendar.get(Calendar.YEAR);
        int birthYear = currentYear - age;

        calendar.set(birthYear, Calendar.JANUARY, 1);

       // 获取出生日期
        int birthDate = calendar.get(Calendar.DATE);

        int birthMonth = calendar.get(Calendar.MONTH) + 1; // 月份从0开始，所以需要加1

        return birthYear + "-" + birthMonth + "-" + birthDate;

    }

    /**
     * 获取当天的日期
     * @return
     */
    public static String getToday(){

        Calendar calendar = Calendar.getInstance();
        // 获取当前月
        int month = calendar.get(Calendar.MONTH) + 1;
        // 获取当前日
        int day = calendar.get(Calendar.DATE);


        String birthday = "-"+ (month < 10 ? ("0" + month) : month)  +"-"+ (day < 10 ? ("0" + day) : day);

        System.out.println("birthday:"+birthday);

//        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
//        String today = sdf.format(new Date());

        System.out.println("today:"+birthday);

        return  birthday;

    }

    public static void main(String[] args) throws ParseException {

//        String date = "2023-09-12T02:51".replace("T"," ");
//
//        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm");
//
//        SimpleDateFormat sdf_mm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//
//        System.out.println(sdf_mm.format(sdf.parse(date)));
//        System.out.println(DateUtil.format(DateUtil.parse(date)));



        //2023-10-12T03:14:28.000+0000
        System.out.println(getToday());

    }

}
