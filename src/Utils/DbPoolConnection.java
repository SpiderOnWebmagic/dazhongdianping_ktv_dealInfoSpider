package Utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.Properties;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidDataSourceFactory;
import com.alibaba.druid.pool.DruidPooledConnection;

public class DbPoolConnection {
	//Á¬½Ó³Ø
	private static DbPoolConnection databasePool = null;
	private static DruidDataSource dds = null;
	static {
		Properties properties = new Properties();
		InputStream inputStream = null;
		try {
			inputStream = new FileInputStream(new File("db_server.properties"));
			properties.load(inputStream);
			dds = (DruidDataSource) DruidDataSourceFactory
					.createDataSource(properties);
		} catch (Exception e1) {
			e1.printStackTrace();
		} finally {
			try {
				if (inputStream != null)
					inputStream.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	private DbPoolConnection() {
	}

	public static synchronized DbPoolConnection getInstance() {
		if (null == databasePool) {
			databasePool = new DbPoolConnection();
		}
		return databasePool;
	}

	public DruidPooledConnection getConnection() throws SQLException {
		return dds.getConnection();
	}

}