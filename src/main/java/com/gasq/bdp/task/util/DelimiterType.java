/**
 * 
 */
package com.gasq.bdp.task.util;

/**
 * @author 巨伟刚
 * @packageName com.gasq.bdp.logn.utils
 * @creatTime 2018年3月17日下午5:40:11
 * @remark 
 */
public enum DelimiterType {
	
	defaultHive(""),XieT("\\t"),blank(" ");
	
	private final String value;
	
    private DelimiterType(String value) {
        this.value = value;
    }
    
    public String getValue() {
        return value;
    }
    public static String getValueByName(String name) {
    	String result = null;
    	switch (name) {
    	case "defaultHive":
			result = "";
			break;
		case "XieT":
			result = "\\t";
			break;
		case "blank":
			result = " ";
			break;
		default:
			break;
		}
    	return result;
    }
    
    public static String valueOfName(String code) {
    	switch(code) {
    		case "": return "defaultHive";
    		case "\\t": return "XieT";
	    	case " ": return "blank";
    		default : 
    		return "defaultHive";
    	}
    }
}
