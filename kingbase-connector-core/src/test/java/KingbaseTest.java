/**
 * 兼容入口：保留旧路径，内部转发到带包名的标准测试类。
 */
public class KingbaseTest {

    public static void main(String[] args) throws Exception {
        io.debezium.connector.kingbasees.KingbaseTest.main(args);
    }
}
