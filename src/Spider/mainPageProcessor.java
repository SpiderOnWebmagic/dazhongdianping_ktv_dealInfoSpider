package Spider;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


import Pipeline.sqlPipeline;
import Utils.DbPoolConnection;
import Utils.JdbcUtils;
import Utils.toZero;
import us.codecraft.webmagic.Page;
import us.codecraft.webmagic.Request;
import us.codecraft.webmagic.Site;
import us.codecraft.webmagic.Spider;
import us.codecraft.webmagic.processor.PageProcessor;
import us.codecraft.webmagic.selector.Html;
import us.codecraft.webmagic.selector.Selectable;

public class mainPageProcessor implements PageProcessor {

	public static DbPoolConnection dbp;

	public static List<Map<String, Object>> idList = new ArrayList<Map<String, Object>>();
	public static final int TYPE_FINDDEAL = 1;
	public static final int TYPE_DEALINFO = 2;
	public static final int TYPE_DEALCOMMENT = 3;
	public static final int TYPE_SHOPCOMMENT = 4;

	public static final String DEALINFO = "insert into dazhongdianping_shopdeal_ktv_20160428"
			+ "(shopId,dealId,dealName,sub_title,price_display,price_discount,price_original,consume_num,star_score,comment_num,validate_date,detail_deal,score1,score2,score3,score4,score5)"
			+ "values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

	public static String unixTime;
	public static List<Map<String, Object>> cookies;
	public static int cookieNum = 0;
	public static int nowCookie = 0;

	private Site site = Site
			.me()
			.setRetryTimes(3)
			.setSleepTime(1000)
			.setCycleRetryTimes(3)
			.setTimeOut(5000)
			.setUserAgent(
					"Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.152 Safari/537.36");

	@Override
	public Site getSite() {
		System.out.println(site);
		return site;
	}

	public void setSite(Site site) {
		this.site = site;
	}

	@Override
	public void process(Page page) {
		Request request = page.getRequest();
		int type = (int) request.getExtra("type");
		switch (type) {
		case TYPE_FINDDEAL: {// 重商户详情页得到团购链接
			Selectable selectable = page.getHtml().xpath(
					"//div[@class='mod sales-promotion clearfix']");
			if (null == selectable.toString()) {
				System.out.println("null");
			} else {
				List<String> temStrings = selectable.links()
						.regex("http://t\\.dianping\\.com/deal/\\d+").all();
				for (String tem : temStrings) {
					Request tarRequest = new Request(tem);
					tarRequest.putExtra("shopId", request.getExtra("shopId"));
					tarRequest.putExtra("type", TYPE_DEALINFO);
					page.addTargetRequest(tarRequest);
				}
				System.out.println(temStrings);
			}
			page.setSkip(true);
			selectable = null;
			break;
		}

		case TYPE_DEALINFO: {// 团购链接中进入获取详情信息
			String url = page.getUrl().toString();
			Html temHtml = page.getHtml();
			List<Object> params = new ArrayList<Object>();
			String shopId = (String) request.getExtra("shopId");
			String dealId = url.substring(27, url.length());
			params.add(shopId);
			params.add(dealId);
			params.add(temHtml.xpath("//h1[@class='title']/text()").toString());
			params.add(temHtml.xpath("//h2[@class='sub-title']/span/text()")
					.toString());
			params.add(temHtml.xpath("//span[@class='price-display']/text()")
					.toString());
			params.add(temHtml.xpath("//span[@class='price-discount']/text()")
					.toString());
			// 3-length
			String priceString = temHtml.xpath(
					"//span[@class='price-original']/text()").toString();
			params.add(priceString.substring(3, priceString.length()));
			params.add(toZero.toZeroUtil(temHtml.xpath(
					"//em[@class='J_current_join']/text()").toString()));
			params.add(temHtml.xpath("//span[@class='star-rate']/text()")
					.toString());
			params.add(toZero.toZeroUtil(temHtml.xpath(
					"//a[@class='comments-count J_main_comment_jump']/text()")
					.toString()));

			// 5-lenth
			String dateString = temHtml.xpath(
					"//div[@class='validate-date']/span/text()").toString();
			params.add(dateString.substring(5, dateString.length()));
			String temDetail = temHtml.xpath(
					"//div[@id='tab_show_1']/script[1]").toString();
			if (null != temDetail) {
				temDetail = temDetail.substring(47, temDetail.length() - 10);
			}
			params.add(temDetail);
			//构造团购评论的链接
			Request commentRequest = new Request(
					"http://t.dianping.com/ajax/detailDealRate?dealGroupId="
							+ dealId + "&pageNo=1&filtEmpty=1&timestamp="
							+ unixTime);
			commentRequest.putExtra("dealId", dealId);
			commentRequest.putExtra("shopId", shopId);
			commentRequest.putExtra("pageNum", 1);
			commentRequest.putExtra("type", TYPE_DEALCOMMENT);
			commentRequest.putExtra("dealInfo", params);
			page.addTargetRequest(commentRequest);
			System.out.println(params);
			page.setSkip(true);
			params = null;
			temHtml = null;
			break;
		}

		case TYPE_DEALCOMMENT: {// 团购评论
			Html html = page.getHtml();
			int pageNum = (int) request.getExtra("pageNum");
			String dealId = (String) request.getExtra("dealId");
			String shopId = (String) request.getExtra("shopId");
			if (pageNum == 1) {//第一页时 抓取评论分数段人数
				List<Object> params = (List<Object>) request
						.getExtra("dealInfo");
				List<String> temlList = html.xpath(
						"//span[@class='p-num']/text()").all();
				if (temlList.size() != 5) {
					for (int i = 1; i <= 5; i++) {
						params.add("0");
					}
				} else {
					for (String string : temlList) {
						string = string.trim();
						params.add(toZero.toZeroUtil(
								string.substring(0, string.length() - 1))
								.trim());
					}
				}
				page.putField("score", params);
				page.putField("scoreSql", DEALINFO);
				params = null;
			}
			List<Object> params2 = new ArrayList<Object>();
			// 每个页面10条评论
			for (int i = 1; i <= 10; i++) {
				Selectable temSelectable = html.xpath("//ul/li[@class='Fix']["
						+ i + "]");
				if (null == temSelectable || null == temSelectable.toString()) {
					break;
				} else {
					String star_icon = temSelectable
							.xpath("//span[@class='star-rating']/span/@class")
							.toString().trim();
					params2.add(shopId);
					params2.add(dealId);
					params2.add(toZero.toZeroUtil(temSelectable.xpath(
							"//div[@class='J_brief_cont_full']/text()")
							.toString()));
					params2.add(toZero.toZeroUtil(temSelectable.xpath(
							"//span[@class='name']/text()").toString()));
					params2.add(toZero.toZeroUtil(temSelectable.xpath(
							"//span[@class='date']/text()").toString()));
					if (null == star_icon || "".equals(star_icon)) {
						params2.add("0");
					} else {
						params2.add(toZero.toZeroUtil(star_icon.substring(11,
								star_icon.length() - 10)));
					}
				}
			}
			System.out.println(params2.toString());
			if (params2.isEmpty() == false) {
				//抓取评论
				StringBuffer commentSql = new StringBuffer(
						"insert into dazhongdianping_deal_comment_20160428(shopId,dealId,content,nickname,create_time,score) values ");
				int size = params2.size();
				for (int i = 1; i <= size; i += 6) {
					if (i == 1)
						commentSql.append("(?,?,?,?,?,?)");
					else
						commentSql.append(",(?,?,?,?,?,?)");
				}
				page.putField("params", params2);
				page.putField("sql", commentSql.toString());
				//下一页
				pageNum++;
				Request request2 = new Request();
				request2.setUrl("http://t.dianping.com/ajax/detailDealRate?dealGroupId="
						+ dealId
						+ "&pageNo="
						+ pageNum
						+ "&filtEmpty=1&timestamp=" + unixTime);
				request2.putExtra("pageNum", pageNum);
				request2.putExtra("dealId", dealId);
				request2.putExtra("shopId", shopId);
				request2.putExtra("type", TYPE_DEALCOMMENT);
				page.addTargetRequest(request2);
				request2 = null;
				commentSql = null;
			} else {
				page.setSkip(true);
			}
			html = null;
			params2 = null;
			break;
		}

		default:
			System.out.println("fail!");
			break;
		}
		request = null;
	}

	public static void main(String[] args) throws SQLException {
		// init
		unixTime = String.valueOf(System.currentTimeMillis());
		dbp = DbPoolConnection.getInstance();
		// 查询所有cookie
		cookies = JdbcUtils.findModeResult("select `key` from cookies3", null,
				dbp.getConnection());
		cookieNum = cookies.size();

		// 查询所有shopID 从shop详情页中获取团购信息，构造链接
		int size;
		String queryListSql = "select shopId from dazhongdianping_shopinfo_single_ktv_20160424";
		idList = JdbcUtils.findModeResult(queryListSql, null,
				dbp.getConnection());
		size = idList.size();
		Spider spider = null;
		mainPageProcessor pageProcessor1 = new mainPageProcessor();
		pageProcessor1.setSite(pageProcessor1.getSite().addCookie(
				".dianping.com", "_hc.v",
				(String) cookies.get(nowCookie).get("key")));

		spider = Spider.create(pageProcessor1).addPipeline(new sqlPipeline());
		for (int i = 0; i < size; i++) {
			String shopId = (String) idList.get(i).get("shopId");
			if (i % 5 == 0) {
				// 每5个ID构造一个spider
				// cookie 循环使用
				if (nowCookie >= cookieNum) {
					nowCookie %= cookieNum;
				}
				String cookie = (String) cookies.get(nowCookie).get("key");
				mainPageProcessor pageProcessor = new mainPageProcessor();
				// 更改site中的cookie
				pageProcessor.setSite(pageProcessor.getSite().addCookie(
						".dianping.com", "_hc.v", cookie));

				spider = Spider.create(pageProcessor).addPipeline(
						new sqlPipeline());
			}
			Request request = new Request("http://www.dianping.com/shop/"
					+ shopId);
			request.putExtra("type", TYPE_FINDDEAL);
			request.putExtra("shopId", shopId);
			spider.addRequest(request);
			if ((i % 5 == 4 || i == size - 1) && spider != null) {
				spider.thread(3).run();
				spider.close();
				spider = null;
			}
		}

		// shop comment
		// http://www.dianping.com/shop/1788163/review_more?pageno=23

	}
}
