/**
 * 
 */
package com.gasq.bdp.task.log;

import java.time.Duration;
import java.time.Instant;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONObject;
import com.gasq.bdp.task.util.CommonUtils;

/**
 * @author Ju_weigang
 * @时间 2018年9月7日上午9:48:25
 * @项目路径 com.gasq.bdp.task.log
 * @描述 
 */
public class HandleWXlog {
	static Logger logger = LoggerFactory.getLogger(HandleWXlog.class);
	
	public static String handleWXlog(String f) {
		if(f.indexOf("production.INFO: 传递参数")!=-1) {
			try {
				Instant start1 = Instant.now();
				String rpf = f.replace("production.INFO: 传递参数","@@");
				String[] rpfs = rpf.split("@@");
				String datetime = rpfs[0].trim();
				String newdt = datetime.substring(1, datetime.length()-1);
				String[] datetimes = newdt.split(" ");
				String fparams = rpfs[1].trim();
				JSONObject obj = JSONObject.parseObject(fparams);
				String header = obj.getString("header");
//				String post = obj.getString("post");
				String body = obj.getString("Body");
//				String method = obj.getString("method");
				String url = obj.getString("url");
				String params = obj.getString("params");
				if(StringUtils.isNotBlank(body)&&!body.equals("null")&&!body.equals("NULL")) params +=body;
				String route = obj.getString("route");
				String customer = obj.getString("customer");
				String mdop = obj.getString("mdop");
				String adtag = obj.getString("adtag");
				JSONObject jsonheader = JSONObject.parseObject(header);
				String store_id = jsonheader.getString("storeid");
				String apptypeplatform = jsonheader.getString("apptypeplatform");
				String x_requested_with = jsonheader.getString("x-requested-with");
				String user_agent = jsonheader.getString("user-agent");
				String referer = jsonheader.getString("referer");
				String phoneInfo = CommonUtils.disposePhoneInfo(user_agent);
				String source = "0";
				if(user_agent.contains("guoanshequ")) {//app来源
					source = "1";
				}
				if(StringUtils.isBlank(apptypeplatform)) {  //检查apptypeplatform是否为空，如果为空，根据os-family来判断
					if(phoneInfo.contains(CommonUtils.iphone) || phoneInfo.contains(CommonUtils.ipad)) {
						apptypeplatform = "ios";
					} else {
						apptypeplatform = "android";
					}
				}
				
				String bean = String.join("\t",datetimes[1], store_id, customer, apptypeplatform, route, params, url, x_requested_with, user_agent, referer, mdop, adtag,source,phoneInfo, datetimes[0]);
				logger.info("WechatLog处理转换完成--------------总用时："+Duration.between(start1, Instant.now()).getSeconds()+"秒！");
				return bean;
			} catch (Exception e) {
				logger.error("WechatLog处理转换异常--------------错误信息："+e.getMessage(),e);
				return null;
			}
		}else {
			return null;
		}
	}
	public static Row handleWXlog1(String f) {
		if(f.indexOf("production.INFO: 传递参数")!=-1) {
			try {
				Instant start1 = Instant.now();
				String rpf = f.replace("production.INFO: 传递参数","@@");
				String[] rpfs = rpf.split("@@");
				String datetime = rpfs[0].trim();
				String newdt = datetime.substring(1, datetime.length()-1);
				String[] datetimes = newdt.split(" ");
				String fparams = rpfs[1].trim();
				JSONObject obj = JSONObject.parseObject(fparams);
				String header = obj.getString("header");
//				String post = obj.getString("post");
				String body = obj.getString("Body");
//				String method = obj.getString("method");
				String url = obj.getString("url");
				String params = obj.getString("params");
				if(StringUtils.isNotBlank(body)&&!body.equals("null")&&!body.equals("NULL")) params +=body;
				String route = obj.getString("route");
				String customer = obj.getString("customer");
				String mdop = obj.getString("mdop");
				String adtag = obj.getString("adtag");
				JSONObject jsonheader = JSONObject.parseObject(header);
				String store_id = jsonheader.getString("storeid");
				String apptypeplatform = jsonheader.getString("apptypeplatform");
				String x_requested_with = jsonheader.getString("x-requested-with");
				String user_agent = jsonheader.getString("user-agent");
				String referer = jsonheader.getString("referer");
				String phoneInfo = CommonUtils.disposePhoneInfo(user_agent);
				int source = 0;
				if(user_agent.contains("guoanshequ")) {
					source = 1;
				}
				if(StringUtils.isBlank(apptypeplatform)) {  //检查apptypeplatform是否为空，如果为空，根据os-family来判断
					if(phoneInfo.contains(CommonUtils.iphone) || phoneInfo.contains(CommonUtils.ipad)) {
						apptypeplatform = "ios";
					} else {
						apptypeplatform = "android";
					}
				}
				String[] phonespt = phoneInfo.split("\t");
				Row row = RowFactory.create(datetimes[1], store_id, customer, apptypeplatform, route, params, url, x_requested_with, user_agent, referer, mdop, adtag,source,phonespt[0],phonespt[1],phonespt[2],phonespt[3], datetimes[0]);
				logger.info("WechatLog处理转换完成--------------总用时："+Duration.between(start1, Instant.now()).getSeconds()+"秒！");
				return row;
			} catch (Exception e) {
				logger.error("WechatLog处理转换异常--------------错误信息："+e.getMessage(),e);
				return null;
			}
		}else {
			return null;
		}
	}
//	public static void main(String[] args) throws IOException {
//		Instant start1 = Instant.now();
//		String inputpath = "D:\\logs\\20180905.log";
////		List<String> lines = FileUtils.readLines(new File(inputpath), "UTF-8");
//		List<String> lines = new ArrayList<>();
//		lines.add("[2018-09-06 10:28:53] production.INFO: 传递参数 {\"header\":{\"x-alicdn-da-via\":\"180.163.159.14,180.163.190.249,39.105.224.250\",\"ali-swift-range-cache\":\"on\",\"ali-swift-origin-host\":\"wxapijs.guoanshequ.com\",\"accept-language\":\"zh-cn\",\"referer\":\"https://wx.guoanshequ.com/wx/index.wx.php?code=081SRwiS0x84082mUtjS0MENiS0SRwib&state=1\",\"user-agent\":\"Mozilla/5.0 (iPhone; CPU iPhone OS 11_4_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15G77 MicroMessenger/6.6.7 NetType/WIFI Language/zh_CN\",\"accept-encoding\":\"br, gzip, deflate\",\"md5\":\"0c4ade35dc8879a97eb1f84f824d6eb3\",\"origin\":\"https://wx.guoanshequ.com\",\"accept\":\"*/*\",\"mdop\":\"d8141d3e04f56cb82122697910f2b77c\",\"storeid\":\"8ad8ac8b588863150158b542f0d211c3\",\"x-client-scheme\":\"https\",\"ali-swift-stat-host\":\"wxapi.guoanshequ.com\",\"ali-swift-log-host\":\"wxapi.guoanshequ.com\",\"ali-cdn-real-ip\":\"180.175.167.254\",\"ali-cdn-real-port\":\"60587\",\"eagleeye-traceid\":\"b4a39f4215362009335344414e\",\"via\":\"cn497.l1, l2et2.l2, l2nu29-1.l2\",\"connection\":\"close\",\"x-forwarded-for\":\"180.175.167.254, 39.105.224.178\",\"x-real-ip\":\"39.105.224.178\",\"host\":\"wxapijs.guoanshequ.com\",\"content-length\":\"\",\"content-type\":\"\",\"user_store\":\"8ad8ac8b588863150158b542f0d211c3\",\"all_store\":[\"8ad8ac8b588863150158b542f0d211c3\"],\"firststore\":\"normal\"},\"post\":\"\",\"Body\":null,\"method\":\"GET\",\"url\":\"wechat/product/f1dfeb259d2fa8d1a35124c4490b06f4\",\"params\":[\"wechat\",\"product\",\"f1dfeb259d2fa8d1a35124c4490b06f4\"],\"route\":\"商品详情\",\"customer\":\"\",\"mdop\":\"d8141d3e04f56cb82122697910f2b77c\",\"adtag\":\"\"}");
//		lines.add("[2018-09-06 08:49:51] production.INFO: 传递参数 {\"header\":{\"x-alicdn-da-via\":\"211.91.163.101,27.221.50.146,39.105.224.249\",\"ali-swift-range-cache\":\"on\",\"ali-swift-origin-host\":\"wxapijs.guoanshequ.com\",\"x-requested-with\":\"com.tencent.mm\",\"accept-language\":\"zh-CN,en-US;q=0.8\",\"accept-encoding\":\"gzip, deflate\",\"referer\":\"https://wx.guoanshequ.com/wx/index.wx.php?code=071ObPYK0mFhv62MuJ0L0bSXYK0ObPY9&state=1\",\"accept\":\"*/*\",\"token\":\"customer_app_328166b6bb847aed01b3245f39eac727\",\"storeid\":\"9647d86ca45541c39b62b61e5732acec\",\"mdop\":\"c420a33cfe58d0bfa1e8b5ec63e4793d\",\"user-agent\":\"Mozilla/5.0 (Linux; Android 7.1.1; OPPO R11s Build/NMF26X; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/57.0.2987.132 MQQBrowser/6.2 TBS/044207 Mobile Safari/537.36 MicroMessenger/6.7.2.1340(0x26070239) NetType/WIFI Language/zh_CN\",\"origin\":\"https://wx.guoanshequ.com\",\"md5\":\"bcd66c084cd53c9ad448a41b898bb22e\",\"x-client-scheme\":\"https\",\"ali-swift-stat-host\":\"wxapi.guoanshequ.com\",\"ali-swift-log-host\":\"wxapi.guoanshequ.com\",\"ali-cdn-real-ip\":\"183.95.184.47\",\"ali-cdn-real-port\":\"39770\",\"eagleeye-traceid\":\"d35ba34515361949916336443e\",\"via\":\"cn552.l1, l2cm9-2.l2, l2nu29-1.l2\",\"connection\":\"close\",\"x-forwarded-for\":\"183.95.184.47, 39.105.224.169\",\"x-real-ip\":\"39.105.224.169\",\"host\":\"wxapijs.guoanshequ.com\",\"content-length\":\"\",\"content-type\":\"\",\"user_store\":\"9647d86ca45541c39b62b61e5732acec\",\"all_store\":[\"9647d86ca45541c39b62b61e5732acec\"],\"firststore\":\"normal\"},\"post\":\"\",\"Body\":null,\"method\":\"GET\",\"url\":\"wechat/cartprods\",\"params\":[\"wechat\",\"cartprods\"],\"route\":\"购物车列表\",\"customer\":\"fcecbf505d904d94a459e5cee3459559\",\"mdop\":\"c420a33cfe58d0bfa1e8b5ec63e4793d\",\"adtag\":\"\"}");
//		for (String f : lines) {
//			logger.info(handleWXlog(f));
//		}
//		logger.info("本地处理微信日志转换完成--------------总用时："+Duration.between(start1, Instant.now()).getSeconds()+"秒！");
//	}
}
