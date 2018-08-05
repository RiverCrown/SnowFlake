
/**
 * twitter的snowflake算法 -- java实现
 * 
 * @author beyond
 * @date 2016/11/26
 */
public class SnowFlake {

    /**
     * 起始的时间戳
     */
    private long startStamp;

    /**
     * 每一部分占用的位数
     */
//    private final long SEQUENCE_BIT; //序列号占用的位数
//    private final long MACHINE_BIT;   //机器标识占用的位数
//    private final long DATACENTER_BIT;//数据中心占用的位数

    /**
     * 每一部分的最大值
     */
    private final long MAX_DATACENTER_NUM;
    private final long MAX_MACHINE_NUM;
    private final long MAX_SEQUENCE;

    /**
     * 每一部分向左的位移
     */
    private final long MACHINE_LEFT;
    private final long DATACENTER_LEFT;
    private final long TIMESTMP_LEFT;

    //每一部分的默认值
    private final static long SEQ_BIT_DEFAULT = 12;
    private final static long MACHINE_BIT_DEFAULT = 5;
    private final static long DATACENTER_BIT_DEFAULT = 5;

    //每一部分的最少位数和最大位数
    private final static int MIN_BIT = 1;
    private final static int MAX_BIT = 60;

    private final static int TOTAL_BIT = 63;//位数总和最大值

    private long dataCenterId;  //数据中心
    private long machineId;     //机器标识
    private long sequence = 0L; //序列号
    private long lastStmp = -1L;//上一次时间戳

    public SnowFlake(long dataCenterId, long machineId) {
        this(System.currentTimeMillis(), SEQ_BIT_DEFAULT, MACHINE_BIT_DEFAULT, DATACENTER_BIT_DEFAULT, 
                dataCenterId, machineId);
    }
    public SnowFlake(long startStamp, long sequenceBit, long machineBit, long dataCenterBit,
                     long dataCenterId, long machineId) {
        //时间戳配置
        this.startStamp = startStamp;

        //计算最大值
        long[] nums = {sequenceBit, machineBit, dataCenterBit};
        validateBit(nums);
        MAX_DATACENTER_NUM = -1L ^ (-1L << dataCenterBit);
        MAX_MACHINE_NUM = -1L ^ (-1L << machineBit);
        MAX_SEQUENCE = -1L ^ (-1L << sequenceBit);

        //计算左移位数
        MACHINE_LEFT = sequenceBit;
        DATACENTER_LEFT = MACHINE_LEFT + machineBit;
        TIMESTMP_LEFT = DATACENTER_LEFT + dataCenterBit;

        //判断 dataCenterId 和 machineId 的准确性
        if (dataCenterId > MAX_DATACENTER_NUM || dataCenterId < 0) {
            throw new IllegalArgumentException("dataCenterId can't be greater than MAX_DATACENTER_NUM or less than 0");
        }
        if (machineId > MAX_MACHINE_NUM || machineId < 0) {
            throw new IllegalArgumentException("machineId can't be greater than MAX_MACHINE_NUM or less than 0");
        }
        this.dataCenterId = dataCenterId;
        this.machineId = machineId;
    }

    private static void validateBit(long[] nums) {
        long totalBitCount = 0;
        for (long num : nums) {
            //判断各个部分的位数是否在合法范围内
            if (num < MIN_BIT || num > MAX_BIT)
                throw new IllegalArgumentException("bit out of range (should between [" + MIN_BIT +
                        ", " + MAX_BIT + "])");
            else
                totalBitCount += num;
        }
        //判断总位数是否合法
        if (totalBitCount > TOTAL_BIT - MIN_BIT)
            throw new IllegalArgumentException("total bit is too large (should be smaller than " + (TOTAL_BIT - MIN_BIT) + " )");
    }

    private long getNextMill() {
        long mill = getNewstmp();
        while (mill <= lastStmp) {
            mill = getNewstmp();
        }
        return mill;
    }

    private long getNewstmp() {
        return System.currentTimeMillis();
    }

    /**
     * 产生下一个ID
     *
     * @return
     */
    public synchronized long nextId() {
        long currStmp = getNewstmp();
        if (currStmp < lastStmp) {
            throw new RuntimeException("Clock moved backwards.  Refusing to generate id");
        }

        if (currStmp == lastStmp) {
            //相同毫秒内，序列号自增
            sequence = (sequence + 1) & MAX_SEQUENCE;
            //同一毫秒的序列数已经达到最大
            if (sequence == 0L) {
                currStmp = getNextMill();
            }
        } else {
            //不同毫秒内，序列号置为0
            sequence = 0L;
        }

        lastStmp = currStmp;

        return (currStmp - startStamp) << TIMESTMP_LEFT //时间戳部分
                | dataCenterId << DATACENTER_LEFT       //数据中心部分
                | machineId << MACHINE_LEFT             //机器标识部分
                | sequence;                             //序列号部分
    }

//    设置开始时间戳，用于刷新 ID
//    注意修改了 startStamp 之后就会生成重复的 ID
//    只有当该算法生成的 ID 只是序列号的一部分时（如序列号由 (日期 + ID) 组成时）
//    才应该考虑使用 set 重置开始时间戳
    public void setStartStamp(long startStamp) {
        this.startStamp = startStamp;
    }

    public static void main(String[] args) {
        SnowFlake snowFlake = new SnowFlake(System.currentTimeMillis(), 11, 2, 2, 0, 0);

        for (int i = 0; i < 20; i++) {
            System.out.println(snowFlake.nextId());
        }

        snowFlake.setStartStamp(System.currentTimeMillis());

        for (int i = 0; i < 20; i++) {
            System.out.println(snowFlake.nextId());
        }
    }
}
