import org.apache.hadoop.hive.ql.exec.UDF;

public class AddActiveDay
        extends UDF
{
    public int evaluate(int value, int day)
    {
        int new_value = 0;
        try
        {
            int day_v = (int)Math.pow(2.0D, day - 1);
            return value | day_v;
        }
        catch (Exception e) {}
        return value;
    }
}
