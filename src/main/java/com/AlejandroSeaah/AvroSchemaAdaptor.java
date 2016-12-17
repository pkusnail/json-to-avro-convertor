package com.AlejandroSeaah;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;

/**
 * Created by alejandroseaah on 16/12/17.
 */
public class AvroSchemaAdaptor {

    private static String line(String key, String value) {
        return item(key, value).concat(",");
    }

    private static String item(String key, String value) {
        return quote(key).concat(":").concat(quote(value));
    }

    private static String quote(String str) {
        return "\"" + str + "\"";
    }

    public static String typeJudeg(Object obj){
        if (obj instanceof Boolean) {
            return "\"boolean\",\"default\":" + Boolean.TRUE;
        }else  if ( (obj instanceof Long ) || (obj instanceof BigInteger )) {
            return "\"long\",\"default\":" + Long.MIN_VALUE;
        }else if (obj instanceof String) {
            return "\"string\",\"default\":\"\"" ;
        }else if (obj == null) {
            return "\"null\",\"default\":null" ;
        }else if (obj instanceof Integer) {
            return "\"int\",\"default\":" + Integer.MIN_VALUE ;
        }else {
            return "\"double\",\"default\":" + Double.MIN_VALUE ;
        }
    }
    private static String getNum(){
        int length = 10;
        String base = "abcdefghijklmnopqrstuvwxyzQWERTYUIOPASDFGHJKLZXCVBNM0123456789";
        Random random = new Random();
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < length; i++) {
            int number = random.nextInt(base.length());
            sb.append(base.charAt(number));
        }
        return sb.toString();
    }

    public static String recordWrapper(String fieldStr){
        return " {\"name\":\"recordNo"+ getNum() +"\"," +
                "\"type\":\"record\"," +
                "\"fields\":["+ fieldStr +"]" +
                "}";
    }

    public static String  avroSchemaConstructor(Object obj) {
        StringBuilder sb = new StringBuilder();
        try {// conver to json object, in case of record
            JSONObject jsonObject = JSON.parseObject(obj.toString());
            Iterator<Map.Entry<String, Object>> iterator = jsonObject.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<String, Object> entry = iterator.next();
                sb.append("{");
                sb.append(line("name", entry.getKey()));
                sb.append(quote("type").concat(":"));
                if (entry.getValue() instanceof JSONArray){
                    sb.append(avroSchemaConstructor(entry.getValue()));
                }else if (entry.getValue() instanceof JSONObject ){
                    sb.append(avroSchemaConstructor(entry.getValue()));
                } else {
                    sb.append(typeJudeg(entry.getValue()));
                }
                sb.append("}");
                if (iterator.hasNext()) {
                    sb.append(",");
                }
            }
            return  recordWrapper(sb.toString());
        } catch (Exception e) {// conver to json array
            try {
                JSONArray jsonArray = JSON.parseArray(obj.toString());
                for( int i = 0; i < jsonArray.size();i++){
                    if ((jsonArray.get(i) instanceof Long) || (jsonArray.get(i) instanceof BigInteger) || (jsonArray.get(i) instanceof Integer)) {
                        sb.append("[{\"type\":\"array\",\"items\":\"long\"}],\"default\":" + Long.MIN_VALUE);
                        return sb.toString();
                    }else if ((jsonArray.get(i) instanceof String)) {
                        sb.append("[{\"type\":\"array\",\"items\":\"string\"}],\"default\":null");
                        return sb.toString();
                    } else if ((jsonArray.get(i) instanceof Double) || (jsonArray.get(i) instanceof BigDecimal) || (jsonArray.get(i) instanceof Float)) {
                        sb.append("[{\"type\":\"array\",\"items\":\"double\"}],\"default\":" + Double.MIN_VALUE);
                        return sb.toString();
                    }else if ((jsonArray.get(i) instanceof Boolean) ) {
                        sb.append("[{\"type\":\"array\",\"items\":\"boolean\"}],\"default\":" + Boolean.TRUE);
                        return sb.toString();
                    } else {// object type
                        sb.append("[{\"type\":\"array\",\"items\":" + avroSchemaConstructor(jsonArray.get(i)) + "}],\"default\":null");
                        return sb.toString();
                    }
                }
            } catch (Exception ee) {
                try{
                    sb.append(typeJudeg(obj));
                    return sb.toString(); // just return
                }catch ( Exception eee){
                    eee.printStackTrace();
                }
            }
        }
        return null;
    }

    public static String avroSchemaAutoAdaption(Object obj) throws DataProcessException {//input must be json object
        if ( ( null == obj ) || obj.toString().equals("")){
            throw new DataProcessException("输入异常");
        }
        try {
            JSONObject jsonObject = JSON.parseObject(obj.toString());
        }catch (Exception e){
            throw new DataProcessException("输入异常");
        }
        return avroSchemaConstructor(obj);
    }
}
