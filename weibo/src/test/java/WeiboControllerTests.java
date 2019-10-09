import com.sitech.weibo.common.exception.NotSupportException;
import com.sitech.weibo.controller.WeiboController;
import com.sitech.weibo.entity.WeiboEntity;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class WeiboControllerTests {


    WeiboController weiboController = new WeiboController();

    @Test
    public void testGetWeiboByUserId() throws IOException, NotSupportException {
        String userId = "u10002";
        List<WeiboEntity> webos = (List<WeiboEntity>) weiboController.getWeiboByUserId(userId, 2)
                .getContent();
        webos.forEach(System.out::println);
    }

    @Test
    public void testPublish() throws IOException, NotSupportException {
//        weiboController.publish("NBA总裁说支持莫雷发声 这位网友回应一针见血", "u10002");
        weiboController.publish("今天hbase课程要结束了", "u10003");
//        weiboController.publish("肖华再发声明：依然支持莫雷 已联系姚明他很生气", "u10002");
    }

    @Test
    public void testGetStarsWeiboByFansId() throws IOException, NotSupportException {
        String fansId = "u10001";
        List<WeiboEntity> weibos = (List<WeiboEntity>) weiboController.getStarsWeiboByFansId(fansId).getContent();
        weibos.forEach(System.out::println);
    }

    @Test
    public void testUnFollowSomeone() throws IOException, NotSupportException {
        //u10003取关了u10002
        String fansId = "u10003";
        String starId = "u10002";
        weiboController.unFollowSomeone(fansId, starId);
    }

    @Test
    public void testFollowSomeone() throws IOException, NotSupportException {
        String fansId = "u10001";
        String starId = "u10003";
        weiboController.followSomeone(fansId, starId);
    }

    @Test
    public void testInit() throws IOException {
        weiboController.init();
    }
}
