package com.hadoop.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import com.hadoop.common.SysConstants;


/**
 * 
 * @author yoUng
 * 
 * @description 发送http请求
 * 
 * @filename HttpUtil.java
 * 
 * @time 2011-6-15 下午05:26:36
 * 
 * @version 1.0
 */

public class HttpUtil {
	
	public static void main(String[] args) throws Exception{
		String url="http://huodong.wangfujing.com/weChatpay/TestServlet";
		Map<String,String> map=new HashMap<String, String>();
		map.put("code", "0414f74a199993fdfd7ca2a28005053g");
		map.put("state", "testState");
		String result=http(url, map);
		System.out.println(result);
	}

	public static String http(String url, Map<String, String> params)
			throws Exception {
		URL u = null;
		HttpURLConnection con = null;
		// 构建请求参数
		StringBuffer sb = new StringBuffer();
		if (params != null && !params.isEmpty()) {
			for (Entry<String, String> e : params.entrySet()) {
				if(e.getValue()!=null){
					sb.append(URLEncoder.encode(e.getKey(), "UTF-8"));
					sb.append("=");
					sb.append(URLEncoder.encode(e.getValue(), "UTF-8"));
					sb.append("&");
				}
			}
		}
		// 尝试发送请求
		try {
			u = new URL(url);
			con = (HttpURLConnection) u.openConnection();
			con.setRequestMethod("POST");
			con.setDoOutput(true);
			con.setDoInput(true);
			con.setUseCaches(false);
			con.setConnectTimeout(SysConstants.CONN_TIMEOUT_SECOND);
			con.setRequestProperty("Content-Type",
					"application/x-www-form-urlencoded; text/html; charset=utf-8");
			OutputStreamWriter osw = new OutputStreamWriter(
					con.getOutputStream(), "UTF-8");
			osw.write(sb.toString());
			osw.flush();
			osw.close();
		} catch (Exception e) {
			throw e;
		} finally {
			if (con != null) {
				con.disconnect();
			}
		}
		// 读取返回内容
		StringBuffer buffer = new StringBuffer();
		try {
			BufferedReader br = new BufferedReader(new InputStreamReader(
					con.getInputStream(), "UTF-8"));
			String temp;
			while ((temp = br.readLine()) != null) {
				buffer.append(temp);
			}
		} catch (Exception e) {
			throw e;
		}
		return buffer.toString();
	}

  /** 
	 * 签名后的http请求
	 *
	 * @param url
	 * @param params
	 * @return
	 **/ 
/*	public static String httpSignature(String url, Map<String, String> params)
			throws Exception {
		if(params!=null){
			String sign=Signature.createSign(params, SysProperties.getProperty(SysConstants.KEY_PRIVATE));
			params.put("sign", sign);
		}
		return http(url, params);
	}*/

	/**
	 * 从request中获得参数Map，并返回可读的Map
	 * 
	 * @param request
	 * @return
	 */
	@SuppressWarnings({"rawtypes", "unchecked"})
	public static Map<String, String> getParameterMap(Map params) {
		// 返回值Map
		Map<String, String> returnMap = new HashMap<String, String>();
		Iterator<Map> entries = params.entrySet().iterator();
		Map.Entry entry;
		String name = "";
		String value = "";
		while (entries.hasNext()) {
			entry = (Map.Entry) entries.next();
			name = (String) entry.getKey();
			Object valueObj = entry.getValue();
			if (null == valueObj) {
				value = "";
			} else if (valueObj instanceof String[]) {
				String[] values = (String[]) valueObj;
				for (int i = 0; i < values.length; i++) {
					value = values[i] + ",";
				}
				value = value.substring(0, value.length() - 1);
			} else {
				value = valueObj.toString();
			}
			returnMap.put(name, value);
		}
		return returnMap;
	}

	/**
	 * 从request中获得XML
	 * 
	 * @param inputStream
	 *            流文件 codeFormat 编码格式
	 * @return
	 * @throws IOException
	 * @throws JDOMException
	 */
//	@SuppressWarnings({"rawtypes"})
//	public static Map<String,String> getXmlMap(InputStream inputStream) {
//		Map<String,String> m = new HashMap<String,String>();
//		try {
//			SAXBuilder builder = new SAXBuilder();
//			Document doc = builder.build(inputStream);
//			Element root = doc.getRootElement();
//			List list = root.getChildren();
//			Iterator it = list.iterator();
//			while (it.hasNext()) {
//				Element e = (Element) it.next();
//				String k = e.getName();
//				String v = "";
//				List children = e.getChildren();
//				if (children.isEmpty()) {
//					v = e.getTextNormalize();
//				} else {
//					v = HttpUtil.getChildrenText(children);
//				}
//				m.put(k, v);
//			}
//		} catch (Exception e1) {
//		} 
//		return m;
//	}

	/**
	 * 获取子结点的xml
	 * 
	 * @param children
	 * @return String
	 */
//	public static String getChildrenText(List children) {
//		StringBuffer sb = new StringBuffer();
//		if (!children.isEmpty()) {
//			Iterator it = children.iterator();
//			while (it.hasNext()) {
//				Element e = (Element) it.next();
//				String name = e.getName();
//				String value = e.getTextNormalize();
//				List list = e.getChildren();
//				sb.append("<" + name + ">");
//				if (!list.isEmpty()) {
//					sb.append(HttpUtil.getChildrenText(list));
//				}
//				sb.append(value);
//				sb.append("</" + name + ">");
//			}
//		}
//
//		return sb.toString();
//	}

	// public static void main(String[] args) {
	// Map<String, String> params=new HashMap<String, String>();
	// params.put("name", "ds");
	// params.put("age", "ds111");
	// System.out.println(http("http://localhost:8080/wfj-trade-service/sayHello/postMethod",
	// params));
	// }


	/**
	 * Https POST
	 * 
	 * @throws
	 **/
	public static String httpsPostMethod(String postURL, String postData) {
		OutputStreamWriter out = null;
		HttpsURLConnection conn = null;
		InputStream is = null;
		StringBuffer buffer = new StringBuffer();
		try {
			SSLContext sc = SSLContext.getInstance("SSL");
			sc.init(null, new TrustManager[]{new TrustAnyTrustManager()},
					new java.security.SecureRandom());
			// 更换服务如何获取凭证
			URL console = new URL(postURL);
			conn = (HttpsURLConnection) console.openConnection();
			conn.setSSLSocketFactory(sc.getSocketFactory());
			conn.setHostnameVerifier(new TrustAnyHostnameVerifier());
			conn.setDoOutput(true);
			conn.setConnectTimeout(SysConstants.CONN_TIMEOUT_SECOND);
			out = new OutputStreamWriter(conn.getOutputStream(), "utf-8");
			out.write(postData);
			out.flush();
			conn.connect();

			// 读取返回内容
			is = conn.getInputStream();

			BufferedReader br = new BufferedReader(new InputStreamReader(is,
					"UTF-8"));
			String temp;
			while ((temp = br.readLine()) != null) {
				buffer.append(temp);
				buffer.append("\n");
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (conn != null) {
				try {
					out.close();
					is.close();
					conn.disconnect();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		return buffer.toString();
	}

	// https证书验证
	private static class TrustAnyTrustManager implements X509TrustManager {
		public void checkClientTrusted(X509Certificate[] chain, String authType)
				throws CertificateException {
		}
		public void checkServerTrusted(X509Certificate[] chain, String authType)
				throws CertificateException {
		}
		public X509Certificate[] getAcceptedIssuers() {
			return new X509Certificate[]{};
		}
	}
	private static class TrustAnyHostnameVerifier implements HostnameVerifier {
		public boolean verify(String hostname, SSLSession session) {
			return true;
		}
	}
	// public static void main(String[] args) {
	// Map<String, String> params=new HashMap<String, String>();
	// params.put("name", "ds");
	// params.put("age", "ds111");
	// System.out.println(http("http://localhost:8080/wfj-trade-service/sayHello/postMethod",
	// params));
	// }
}
