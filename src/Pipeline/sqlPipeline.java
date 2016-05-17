package Pipeline;

import java.sql.SQLException;
import java.util.List;

import Spider.mainPageProcessor;
import Utils.JdbcUtils;
import us.codecraft.webmagic.ResultItems;
import us.codecraft.webmagic.Task;
import us.codecraft.webmagic.pipeline.Pipeline;

public class sqlPipeline implements Pipeline {

	boolean res = false;

	@Override
	public void process(ResultItems arg0, Task arg1) {
		List<Object> params = arg0.get("params");
		String sql = arg0.get("sql");

		try {
			res = JdbcUtils.updateByPreparedStatement(sql, params,
					mainPageProcessor.dbp.getConnection());
			if (res) {
				System.out.println("success!");
			} else {
				System.out.println("fail!");
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}

	}
}
