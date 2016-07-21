package com.hadoop.util;

/**
 * Created by Administrator on 2015/10/28.
 */
public class SysCodeUtils {

    public static String[] getSysCodes(String str){
        if(StringUtils.isNotBlank(str)){
            String [] sysCodeStr=str.split(",");
            return sysCodeStr;
        }
        return null;
    }
}
