package com.gasq.bdp.task.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import nl.bitwalker.useragentutils.UserAgent;
@JsonIgnoreProperties(ignoreUnknown = true)
public class CommonUtils {
	static Logger logger = LoggerFactory.getLogger(CommonUtils.class);
	static ObjectMapper mapper = new ObjectMapper();
	
	public static String android="Android";
	public static String iphone="iPhone";
	public static String ipad="iPad";

	public static boolean is(Boolean b) {
		return b!=null && b.booleanValue();
	}
	
	public static boolean isEmpty(Object v) {
		return isEmpty(v, true);
	}
	
	public static boolean isEmpty(Object v, boolean trim) {
		if(v == null) return true;
		if(v instanceof String) {
			String sv = (String) v;
			return trim ? sv.trim().length()==0 : sv.length()==0;
		}else {
			return false;
		}
	}
	
	public static String getArrayToString(String[] array,String split){
		if(isEmpty(array))return "";
		String str = "";
		for (int i = 0; i < array.length; i++){ 
			if(isEmpty(array[i]))continue;
			str+=array[i]+split;
		}
		if(str.length()>0)str = str.substring(0,str.length()-split.length());
		return str;
	}
	
	
	public static String createUUID(){
		return createUUID(1).toString();
	}
	
	public static String[] createUUID(int size){
		if(size<=0) return null;
		String uuids[] = new String[size];
		for (int i = 0; i < size; i++) {
			uuids[i] = UUID.randomUUID().toString();
		}
		return uuids;
	} 
	/**
	 * 将字符串转换为数据库可执行的字符串
	 * 处理数据库字符串字段 eg:aaaaa/aaaaa,bbbbb
	 * @param str eg：123ace---->'123ace'
	 */
	public static String changeStrToSqlValue(String str,String tag){
		if(str.length()<=0) return null;
		if(str.indexOf(tag)!=-1){
			String[] s = str.split(tag);
			str="";
			for (int i = 0; i < s.length; i++) {
				if(i==0) str = "'"+s[i]+"'";
				else str += ",'" + s[i]+"'"; 
			}
		}else str = "'"+str+"'";
		return str;
	}
	
	/**
	 * 获取随机数
	 * @param digit 位数
	 * @return
	 */
	public static String getRandomcode(int digit){
		int max=10;
        int min=1;
        for (int i = 1; i < digit; i++) {
			max = max*10;
			min = min*10;
		}
        Random random = new Random();
        int s = random.nextInt(max)%(max-min+1) + min;
		return s+"";
	}
	 public static String encodeStr(String str) {  
         try {  
             return new String(str.getBytes("ISO-8859-1"), "UTF-8");  
         } catch (UnsupportedEncodingException e) {  
             e.printStackTrace();  
             return null;  
         }  
     } 
	 
	 public static String BeanToJSON(Object obj){
		 String str = null;
		 try {
			 if(mapper == null){
				 mapper = new ObjectMapper();
			 }
			 mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
			 mapper.setDateFormat(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"));
			 str = mapper.writeValueAsString(obj);
		} catch (JsonGenerationException e) {
			e.printStackTrace();
		} catch (JsonMappingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}finally {
			obj = null;
		}
		return str;
	 }
	 public static Object JsonToBean(String jsonstr,Class<?> cls) {
		try {
			 if(mapper == null){
				 mapper = new ObjectMapper();
			 }
			 mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
			 mapper.setDateFormat(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"));
			 return mapper.readValue(jsonstr,cls);
		} catch (Exception e) {
			e.printStackTrace();
		}finally {
			jsonstr = null;
		}
		return null;
	}
	 public static <T> String bean2Json(T bean) {  
	        try {
	        	if(mapper == null){
					 mapper = new ObjectMapper();
				 }
	        	mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
	        	mapper.setDateFormat(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"));
	            return mapper.writeValueAsString(bean);  
	        } catch (Exception e) {  
	            e.printStackTrace();  
	        }finally {
	        	bean = null;
			}
	        return "";  
	    }  
	      
	    @SuppressWarnings("rawtypes")
		public static String map2Json(Map map) {  
	        try {
	        	if(mapper == null){
					 mapper = new ObjectMapper();
				 }
	        	mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
	        	mapper.setDateFormat(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"));
	            return mapper.writeValueAsString(map);  
	        } catch (Exception e) {  
	            e.printStackTrace();  
	        }finally {
	        	map = null;
			}  
	        return "";  
	    }  
	      
	    @SuppressWarnings("rawtypes")
		public static String list2Json(List list) {  
	        try {
	        	if(mapper == null){
					 mapper = new ObjectMapper();
				 }
	        	mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
	        	mapper.setDateFormat(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"));
	            return mapper.writeValueAsString(list);  
	        } catch (Exception e) {  
	            e.printStackTrace();  
	        }finally {
	        	list = null;
			} 
	        return "";  
	    }  
	      
	    public static <T> T json2Bean(String json, Class<T> beanClass) {  
	        try {
	        	if(mapper == null){
					 mapper = new ObjectMapper();
				 }
	        	mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
	        	mapper.setDateFormat(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"));
	            return mapper.readValue(json, beanClass);  
	        } catch (Exception e) {  
	            e.printStackTrace();  
	        }finally {
	        	json = null;
			}  
	        return null;  
	    }  
	      
	    @SuppressWarnings("unchecked")
		public static <T> List<T> json2List(String json, Class<T> beanClass) {  
	        try {
	        	if(mapper == null){
					 mapper = new ObjectMapper();
				 }
	        	mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
	        	mapper.setDateFormat(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"));
	            return (List<T>)mapper.readValue(json, getCollectionType(List.class, beanClass));  
	        } catch (Exception e) {  
	            e.printStackTrace();  
	        }finally {
	        	json = null;
			}  
	        return null;  
	    }  
	      
	    @SuppressWarnings("rawtypes")
		public static Map json2Map(String json) {  
	        try {  
	        	if(mapper == null){
					 mapper = new ObjectMapper();
				 }
	        	mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
	        	mapper.setDateFormat(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"));
	            return (Map)mapper.readValue(json, Map.class);  
	        } catch (Exception e) {  
	            e.printStackTrace();  
	        }finally {
	        	json = null;
			}  
	        return null;  
	    }  
	      
	    @SuppressWarnings("deprecation")
		public static JavaType getCollectionType(Class<?> collectionClass, Class<?>... elementClasses) {
	    	mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
	        return mapper.getTypeFactory().constructParametricType(collectionClass, elementClasses);     
	    }   
	 
	/**
	 * 两个数据相除 并保留两位小数
	 * @param str1
	 * @param str2
	 * @return result
	 * */
	 public static String divideTempResult(String str1, String str2){
		 String temp = "0";
		 try{
			 if(StringUtils.isEmpty(str1) && StringUtils.isEmpty(str2)){
				 BigDecimal tempStr1 = new BigDecimal(str1);
				 BigDecimal tempStr2 = new BigDecimal(str2);
				 BigDecimal tempResult = tempStr1.divide(tempStr2, 2, BigDecimal.ROUND_HALF_UP);
				 temp = tempResult + "";
			 }
		 }catch(Exception e){
			 e.printStackTrace();
		 }
		 return temp;
	 }
	 
	 /**
	  * 转换DOUBLE类型保留后几位小数
	  * @param d
	  * @param subindex
	  * @return
	  */
	 public static double transformDouble(double d,int subindex){
		 BigDecimal bd = new BigDecimal(d);  
		 double f1 = bd.setScale(subindex,BigDecimal.ROUND_HALF_UP).doubleValue();
		 return f1;
	 }
	 
	 /**
	  * 转换DOUBLE类型保留后几位小数
	  * @param d
	  * @param subindex
	  * @return
	  */
	 public static double transformStringToDouble(String strd,int subindex){
		 BigDecimal bd = new BigDecimal(strd);  
		 double f1 = bd.setScale(subindex,BigDecimal.ROUND_HALF_UP).doubleValue();
		 return f1;
	 }

	public static Map<String, Object> checkSqlEnable(String sql){
		Map<String,Object> map = new HashMap<String,Object>();
		if(isEmpty(sql)){
			map.put("status", false);
			map.put("mssg", "语句不能为空！");
			return map;
		}
		else if(sql.toLowerCase().indexOf("drop")!=-1 || sql.toLowerCase().indexOf("database")!=-1 || sql.toLowerCase().indexOf("use")!=-1|| sql.toLowerCase().indexOf("alter")!=-1){
			map.put("status", false);
			map.put("mssg", "此语句没有权限执行！");
			return map;
		}else{
			map.put("status", true);
			return map;
		}
	}
	
	
	/**
     * 将字符串text中由openToken和closeToken组成的占位符依次替换为args数组中的值
     * @param openToken
     * @param closeToken
     * @param text
     * @param args
     * @return
     */
    public static String parse(String openToken, String closeToken, String text, Object... args) {
        if (args == null || args.length <= 0) {
            return text;
        }
        int argsIndex = 0;

        if (text == null || text.isEmpty()) {
            return "";
        }
        char[] src = text.toCharArray();
        int offset = 0;
        // search open token
        int start = text.indexOf(openToken, offset);
        if (start == -1) {
            return text;
        }
        final StringBuilder builder = new StringBuilder();
        StringBuilder expression = null;
        while (start > -1) {
            if (start > 0 && src[start - 1] == '\\') {
                // this open token is escaped. remove the backslash and continue.
                builder.append(src, offset, start - offset - 1).append(openToken);
                offset = start + openToken.length();
            } else {
                // found open token. let's search close token.
                if (expression == null) {
                    expression = new StringBuilder();
                } else {
                    expression.setLength(0);
                }
                builder.append(src, offset, start - offset);
                offset = start + openToken.length();
                int end = text.indexOf(closeToken, offset);
                while (end > -1) {
                    if (end > offset && src[end - 1] == '\\') {
                        // this close token is escaped. remove the backslash and continue.
                        expression.append(src, offset, end - offset - 1).append(closeToken);
                        offset = end + closeToken.length();
                        end = text.indexOf(closeToken, offset);
                    } else {
                        expression.append(src, offset, end - offset);
                        offset = end + closeToken.length();
                        break;
                    }
                }
                if (end == -1) {
                    // close token was not found.
                    builder.append(src, start, src.length - start);
                    offset = src.length;
                } else {
                    ///////////////////////////////////////仅仅修改了该else分支下的个别行代码////////////////////////

                    String value = (argsIndex <= args.length - 1) ?
                            (args[argsIndex] == null ? "" : args[argsIndex].toString()) : expression.toString();
                    builder.append(value);
                    offset = end + closeToken.length();
                    argsIndex++;
                    ////////////////////////////////////////////////////////////////////////////////////////////////
                }
            }
            start = text.indexOf(openToken, offset);
        }
        if (offset < src.length) {
            builder.append(src, offset, src.length - offset);
        }
        return builder.toString();
    }

    public static String parse0(String text, Object... args) {
        return parse("${", "}", text, args);
    }

    public static String parse1(String text, Object... args) {
        return parse("{", "}", text, args);
    }
    public static <K, V> Map<K, V> list2Map3(List<V> list, String keyMethodName,Class<V> c) {  
        Map<K, V> map = new HashMap<K, V>();  
        if (list != null) {  
            try {  
                Method methodGetKey = c.getMethod(keyMethodName);  
                for (int i = 0; i < list.size(); i++) {  
                    V value = list.get(i);  
                    @SuppressWarnings("unchecked")  
                    K key = (K) methodGetKey.invoke(list.get(i));  
                    map.put(key, value);  
                }  
            } catch (Exception e) {  
                throw new IllegalArgumentException("field can't match the key!");  
            }  
        }  
        return map;  
    }
    /**
     * 将ResultSet转换为list<map>
     */
	public static List<Map<String,Object>> ResultSetToList(ResultSet rs) throws SQLException{
		List<Map<String,Object>> results = new ArrayList<Map<String,Object>>();
		if(rs==null) return results;
		ResultSetMetaData rsmd = rs.getMetaData();
		int colCount=rsmd.getColumnCount();
		while(rs.next()){
			Map<String, Object> rowData=new HashMap<String, Object>();
			for(int i=1;i<=colCount;i++){
				rowData.put(rsmd.getColumnName(i), rs.getObject(i));
			}
			results.add(rowData);
		}
		return results;
	}
	
	/**
     * MD5方法
     * 
     * @param text 明文
     * @return 密文
     * @throws Exception
     */
    public static String md5(String text) throws Exception {
        //加密后的字符串
        String encodeStr=DigestUtils.md5Hex(text);
        return encodeStr;
        }

    /**
     * MD5验证方法
     * 
     * @param text 明文
     * @param key 密钥
     * @return true/false
     * @throws Exception
     */
    public static boolean verify(String text, String key) throws Exception {
        //根据传入的密钥进行验证
        String md5Text = md5(text);
        if(md5Text.equalsIgnoreCase(key))
        {
            return true;
        }
        return false;
    }
    @SuppressWarnings("unchecked")
    public static <T> List<T> deepCopyList(List<T> src)
    {
        List<T> dest = null;
        try
        {
            ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
            ObjectOutputStream out = new ObjectOutputStream(byteOut);
            out.writeObject(src);
            ByteArrayInputStream byteIn = new ByteArrayInputStream(byteOut.toByteArray());
            ObjectInputStream in = new ObjectInputStream(byteIn);
            dest = (List<T>) in.readObject();
        }
        catch (IOException e)
        {
        	e.printStackTrace();
        }
        catch (ClassNotFoundException e)
        {
        	e.printStackTrace();
        }
        return dest;
    }
    
	//获取用户操作系统
	public static String getOS(String userAgent){
		if (userAgent.contains(android)) {
			return android;
		}else if (userAgent.contains(iphone)){
			return iphone;
		}else if (userAgent.contains(ipad)){
			return ipad;
		}else {
			return "others";
		}
	}
    
    public static String getPhone(String userAgent){
		String OS = getOS(userAgent);
		String phoneStr="不知名的手机";
		if (OS.equals(android)) {
			String rex="[()]+";
			String[] str=userAgent.split(rex);
			str = str[1].split("[;]");
			String[] res=str[str.length-2].split("Build/");
			return android+"\t"+res[0].trim();
		}else if (OS.equals(iphone)) {
			String[] str=userAgent.split("[()]+");
			String res=str[1].split("OS")[1].split("like")[0].toString().trim();
			return iphone+"\t"+res;
		}else if (OS.equals(ipad)) {
			return ipad;
		}else {
			return phoneStr;
		}
	}
    
    public static String disposePhoneInfo(String str) throws IOException{
    	String resultstr = null;
    	String phone = getOS(str);
    	UserAgent userAgentInfo = UserAgent.parseUserAgentString(str);
		String model = null;
		if(str.indexOf("Linux")!=-1) {
			model = getPhoneModel(str);
		}else {
			if(str.contains("iPad")) model = "iPad";
			else if(str.contains("iPhone")) model = "iPhone";
		}
		logger.debug("机型：" + model);
		String network = null;
		if(str.indexOf("NetType/")!=-1) {
			String nta = str.substring(str.indexOf("NetType/")+"NetType/".length(), str.length());
			network = nta.split(" ")[0];
		}
		logger.debug("网络：" + network);
		resultstr = String.join("\t",phone,model,userAgentInfo.getBrowser().getName()+userAgentInfo.getBrowser().getVersion(str),network);
		return resultstr;
    }
    
  //获取用户手机型号  
    public static String getPhoneModel(String userAgent){
    	if(null == userAgent || "" == userAgent) return "";
        if (userAgent.indexOf("Android")!=-1) {
            String rex="[()]+";
            String[] str=userAgent.split(rex);
            str = str[1].split("[;]");
            String[] res=str[str.length-2].trim().split("Build/");
            return res[0].trim();
        }else if (userAgent.indexOf("iPhone")!=-1) {
            String[] str=userAgent.split("[()]+");
            String res="iphone"+str[1].split("OS")[1].split("like")[0];
            return res;
        }else {
            return null;
        }
    }

    
    /**
     * 使用示例
     * @param args
     */
    public static void main(String... args) {
//    	Function<String,String> func =  String::toUpperCase;
//    	System.out.println(CommonUtils::disposePhoneInfo);
    	args = new String[5];
    	args[0] = "Mozilla/5.0 (Linux; Android 8.0; FRD-AL10 Build/HUAWEIFRD-AL10; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/57.0.2987.132 MQQBrowser/6.2 TBS/044204 Mobile Safari/537.36 MicroMessenger/6.6.7.1321(0x26060739) NetType/WIFI Language/zh_CN";
    	args[1] = "Mozilla/5.0 (Linux; Android 7.0; Redmi Note 4X Build/NRD90M; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/57.0.2987.132 MQQBrowser/6.2 TBS/044204 Mobile Safari/537.36 MicroMessenger/6.7.1321(0x26070030) NetType/WIFI Language/zh_CN";
    	args[2] = "Mozilla/5.0 (Linux; Android 6.0.1; Redmi Note 3 Build/MMB29M; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/57.0.2987.132 Mobile Safari/537.36communitysdk"; 
    	args[3] = "Mozilla/5.0 (iPhone; CPU iPhone OS 10_3_3 like Mac OS X) AppleWebKit/603.3.8 (KHTML, like Gecko) Mobile/14G60 MicroMessenger/6.7.1 NetType/WIFI Language/zh_CN\r\n";
    	args[3] = "Mozilla/5.0 (iPhone; CPU iPhone OS 10_1_1 like Mac OS X) AppleWebKit/602.2.14 (KHTML, like Gecko) Mobile/14B100 guoanshequ";
    	args[3] = "Mozilla/5.0 (iPad; CPU OS 11_3 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E302 MicroMessenger/6.7.1 NetType/WIFI Language/zh_CN";
    	Arrays.asList(args).stream().forEach(CommonUtils::getPhoneModel);
    	//    	try {
//			String disposePhoneInfo = CommonUtils.disposePhoneInfo(args[3]);
//			System.out.println(disposePhoneInfo);
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
    	
    	//{}被转义，不会被替换
//        System.out.println(parse("{", "}", "我的名字是\\{},结果是{}，可信度是%{}", "雷锋", true, 100));
//        System.out.println(parse0("我的名字是'${}',结果是'${}'，可信度是%${}", "雷锋", true, 100));
//        System.out.println(parse1("我的名字是{},结果是{}，可信度是%{}", "雷锋", true, 100));
//        输出结果如下：
//        我的名字是{},结果是true，可信度是%100
//        我的名字是雷锋,结果是true，可信度是%100
//        我的名字是雷锋,结果是true，可信度是%100
    }
}
