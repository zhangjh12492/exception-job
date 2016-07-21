package com.hadoop.util;

import java.io.InputStream;
import java.util.Properties;

public class SystemBootstrap {
	private static final String CONFIG_FILE_PATH = "/exception-job-config.properties";

	private static String sysCodes[]=null;

	public static String [] getSysCodes(){
		return sysCodes;
	}

	private static void setSysCodes(String sysCodeStr){
		if(StringUtils.isNotBlank(sysCodeStr)){
			sysCodes=sysCodeStr.split(",");
		}
	}

	public static void afterPropertiesSet() {
		InputStream configInputStream = null;
		Properties propertiesConfigPro = new Properties();
		try {
			configInputStream = SystemBootstrap.class.getResourceAsStream(CONFIG_FILE_PATH);
			propertiesConfigPro.load(configInputStream);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				configInputStream.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		PropertiesLoad.init(propertiesConfigPro);
		setSysCodes(PropertiesLoad.getProperties("sys_codes"));
	}
}

/* Location:           E:\EclipseEE\test_project\exception-client-framework\target\classes\
 * Qualified Name:     com.wfj.exception.util.SystemBootstrap
 * JD-Core Version:    0.6.0
 */