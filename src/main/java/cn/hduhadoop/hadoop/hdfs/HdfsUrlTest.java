package cn.hduhadoop.hadoop.hdfs;

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.io.IOUtils;

import java.io.InputStream;
import java.net.URL;

public class HdfsUrlTest {

	// 注册  让Java程序识别HDFS URL形式
	static {
		URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
	}
	public static void main(String[] args) {
		String fileUrl = //
				"hdfs://172.16.0.2:9000"//
				+ "/user/zhuyh/mr/input/account.txt";
		
		InputStream inStream = null;
		
		try {
			inStream = new URL(fileUrl).openStream();
			IOUtils.copyBytes(inStream, System.out, 4096, false);
		}catch(Exception e)
		{
			e.printStackTrace();
			
		}finally {
			IOUtils.closeStream(inStream);
		}
	}

}
