import com.sitech.weibo.common.Constants;
import com.sitech.weibo.common.exception.NotSupportException;
import com.sitech.weibo.common.rowkey.Generator;
import com.sitech.weibo.common.rowkey.JointGenerator;
import com.sitech.weibo.dao.WeiboDao;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

public class WeiboDaoTests {

    WeiboDao weiboDao = new WeiboDao();

    Generator generator = new JointGenerator();


    @Test
    public void testgetCellByRowkeyAndVersion() throws NotSupportException, IOException {
        byte[] rowKey = generator.getRowKey("u10001", "u10002");
        List<Cell> weiboIds = weiboDao.getCellByRowkeyAndVersion(Constants.INBOX_TABLE_NAME, rowKey,
                Constants.INBOX_FAMILYS_NAME, "weiboId", 3);
        weiboIds.stream().forEach(cell -> System.out.println(Bytes.toString(CellUtil.cloneValue(cell))));
    }


    @Test
    public void testScanByPrefix() throws NotSupportException, IOException {
        List<Result> results = weiboDao.scanByPrefix(Constants.RELATIONSHIP_TABLE_NAME,
                generator.getRowKey("1", "u10003"),
                Constants.RELATIONSHIP_FAMILYS_NAME, "targetUserId", Constants.DELETE_FLAG);
        Iterator<Result> iter = results.iterator();
        while (iter.hasNext()) {
            Result next = iter.next();
            next.listCells().forEach(cell -> {
                System.out.println("qualifier:" + Bytes.toString(CellUtil.cloneQualifier(cell)));
                System.out.println("value:" + Bytes.toString(CellUtil.cloneValue(cell)));
                System.out.println("======================");
            });
        }
    }
}
