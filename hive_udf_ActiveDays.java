import org.apache.hadoop.hive.ql.exec.UDF;

public class ActiveDays
        extends UDF
{
    public int evaluate(int value, int day0, int day1)
    {
        int sum = 0;
        int itv = (int)(Math.pow(2.0D, day1) - Math.pow(2.0D, day0 - 1));
        value &= itv;
        while (value > 0)
        {
            sum++;
            value &= value - 1;
        }
        return sum;
    }
}
