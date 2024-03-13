import org.junit.Assert;
import org.junit.Test;
import com.red.udf.DateSub;

public class UdfTest {
    @Test
    public void testDaysAgoNoDash() {
        DateSub udf = new DateSub();
        Assert.assertEquals("20230731", udf.eval("20230801", 1));
        Assert.assertEquals("20230802", udf.eval("20230801", -1));
        Assert.assertEquals("20230801", udf.eval("20230801", 0));
        Assert.assertEquals("20230901", udf.eval("20230801", -31));
        Assert.assertEquals("20230701", udf.eval("20230801", 31));
    }
}
