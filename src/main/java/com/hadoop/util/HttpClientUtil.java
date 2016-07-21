package com.hadoop.util;


import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import org.apache.http.HttpEntity;
import org.apache.http.HttpException;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.NameValuePair;
import org.apache.http.ParseException;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.ConnectTimeoutException;
import org.apache.http.conn.ConnectionPoolTimeoutException;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.protocol.HTTP;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;

import com.hadoop.common.SysConstants;


/**
 * Created by MaYong on 2014/12/23.
 */
public class HttpClientUtil {
    private final static Logger logger =Logger.getLogger(HttpClientUtil.class);
    private static HttpClient httpClient = new DefaultHttpClient();
    
    /**
     * 发送Get请求
     *
     * @param url
     * @param params
     * @return
     */
    public static String get(String url, List<NameValuePair> params) {
        String body = null;
        try {
            // Get请求
            HttpGet httpget = new HttpGet(url);
            // 设置参数
            String str = EntityUtils.toString(new UrlEncodedFormEntity(params));
            httpget.setURI(new URI(httpget.getURI().toString() + "?" + str));
            // 发送请求
            HttpResponse httpresponse = httpClient.execute(httpget);
            // 获取返回数据
            HttpEntity entity = httpresponse.getEntity();
            body = EntityUtils.toString(entity);
            if (entity != null) {
                entity.consumeContent();
            }
        } catch (ParseException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        return body;
    }

    /**
     * 发送 Post请求
     *
     * @param url
     * @param reqXml
     * @return
     */
    public static String post(String url, String reqXml) {
        String body = null;
        try {
            // Post请求
            HttpPost httppost = new HttpPost(url);
            // 设置参数
            List<NameValuePair> params = new ArrayList<NameValuePair>();
            params.add(new BasicNameValuePair("body", reqXml));
            httppost.setEntity(new UrlEncodedFormEntity(params, HTTP.UTF_8));
            //设置报文头
            httppost.setHeader("Content-Type", "text/xml;charset=UTF-8");
            // 发送请求
            HttpResponse httpResponse = httpClient.execute(httppost);
            // 获取返回数据
            HttpEntity entity = httpResponse.getEntity();
            //获取响应码
            int statusCode = httpResponse.getStatusLine().getStatusCode();
            body = EntityUtils.toString(entity);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (ClientProtocolException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return body;
    }

    public static String postBody(String url, String json) throws Exception {
        String result = null;
        HttpPost request = new HttpPost(url);
        request.addHeader(HTTP.CONTENT_TYPE, "application/json; charset=utf-8");
        try {
            if (json != null) {
                HttpEntity paramEntity = new StringEntity(json, "utf8");
                request.setEntity(paramEntity);
            }
            HttpResponse response = new DefaultHttpClient().execute(request);
            Integer statusCode = response.getStatusLine().getStatusCode();
            if (statusCode == 200) {
                result = EntityUtils.toString(response.getEntity());
            } else {
                throw new Exception(statusCode.toString());

            }
        } catch (ClientProtocolException e) {
            logger.error(e.getMessage());
            throw new Exception("999");
        } catch (IOException e) {
            logger.error(e.getMessage());
            throw new Exception("999");
        }
        return result;
    }

    public static String postBody(String url, String json, int timeout) throws Exception {
        String result = null;
        HttpPost request = new HttpPost(url);
        request.addHeader(HTTP.CONTENT_TYPE, "application/json; charset=utf-8");
        try {
            //3.1写法
//            HttpClient client = new DefaultHttpClient();
            //4.3写法
            CloseableHttpClient httpClient = HttpClients.createDefault();
            HttpPost httpPost = new HttpPost(url);
            RequestConfig requestConfig = RequestConfig.custom().setSocketTimeout(timeout)
                    .setConnectTimeout(timeout).build();//设置请求和传输超时时间
            httpPost.setConfig(requestConfig);
            if (json != null) {
                HttpEntity paramEntity = new StringEntity(json, "utf8");
                httpPost.setEntity(paramEntity);
            }
            HttpResponse response = httpClient.execute(httpPost);//执行请求
            Integer statusCode = response.getStatusLine().getStatusCode();
            if (statusCode == 200) {
                result = EntityUtils.toString(response.getEntity());
            } else {
                throw new Exception(statusCode.toString());
            }
        } catch (ClientProtocolException e) {
            logger.error(e.getMessage());
            throw new Exception("999");
        } catch (IOException e) {
            logger.error(e.getMessage());
            throw new Exception("999");
        }
        return result;
    }

    public static String postJSON(String url, String json) throws Exception {
        String result = "";
        HttpPost request = new HttpPost(url);
        //需要设置timeout时间，
        // set Timeout
        RequestConfig requestConfig = RequestConfig.custom().setConnectionRequestTimeout(SysConstants.CONN_TIMEOUT_MS)
                .setConnectTimeout(SysConstants.CONN_TIMEOUT_MS).setSocketTimeout(SysConstants.CONN_TIMEOUT_MS).build();
        request.setConfig(requestConfig);

        request.addHeader(HTTP.CONTENT_TYPE, "application/json; charset=utf-8");
        /*
         * 
         Map heads;
         if (heads != null) {
			for (Entry e : heads.entrySet()) {
				httppost.addHeader(e.getKey(), e.getValue());
			}
		}		
		*/
        try {
            if (json != null) {
                HttpEntity paramEntity = new StringEntity(json, "UTF-8");
                request.setEntity(paramEntity);
            }
            HttpClient httpClient = HttpClients.createDefault();
            HttpResponse response = httpClient.execute(request);
            
            request.abort();
            // get http status code
            Integer statusCode = response.getStatusLine().getStatusCode();
            if (statusCode == HttpStatus.SC_OK) {
                // get result data
                HttpEntity entity = response.getEntity();
                result = EntityUtils.toString(entity,"utf-8");
                logger.info("接收netty服务器返回的消息 : "+result);
            } else {
                throw new Exception(statusCode.toString());
            }
        } catch (ConnectionPoolTimeoutException e) {
            logger.error("http post throw ConnectionPoolTimeoutException", e);
            result ="false";
        } catch (ConnectTimeoutException e) {
        	result ="false";
            logger.error("http post throw ConnectTimeoutException", e);
        } catch (SocketTimeoutException e) {
        	result ="false";
            logger.error("http post throw SocketTimeoutException", e);
        } catch (HttpException e) {
        	result ="false";
        	logger.error("HttpException: "+e.getMessage());
        } catch (ClientProtocolException e) {
        	result ="false";
            logger.error("ClientProtocolException"+e.getMessage());
        } catch (IOException e) {
        	result ="false";
            logger.error("IOException  "+e.getMessage());
        }
        return result;
    }
    
   

}

