package com.app.batchdatagenerate;


import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * @ClassName BatchDataGenerate
 * @Description TODO batch data generate
 * 构造两个线程，一个用于生成数据并保存在队列中，另一个用于从队列中取数据并保存在文件中。代码中几个关键的变量（参数）介绍如下：
 *
 * TOTAL_NUM：需要生成的数据总条数
 * BATCH_SIZE：控制台打印进度信息及队列当前信息的数据条数间隔，同时也是每批次写文件的数据量大小
 * QUEUE_CAPACITY：队列大小，若队列满则阻塞写线程
 * DEST_FILE_PATH：数据文件存储目录
 * queue：缓存队列
 * @Author zy
 * @Date 2019/8/6 18:33
 * @Version 1.0
 **/
public class BatchDataGenerate implements Runnable {

    // total record
    private final Integer TOTAL_NUM = 1 * 1000;
    // batch size of writing to file and echo infos to console
    private final Integer BATCH_SIZE = 1 * 100;
    // capacity of the array blocking queue
    private final Integer QUEUE_CAPACITY = 100 * 10000;
    // csv file path that save the data
    private final String DEST_FILE_PATH = "E:\\order_detail.csv";
    // the queue which the write thread write into and the read thread read from
    private ArrayBlockingQueue<String> queue = new ArrayBlockingQueue<>(QUEUE_CAPACITY);

    // the cardinalities of the fields(dimensions) which means the distinct
    // number of a column
    private final int CARDINALITY_SALES_AREA_ID = 100;
    private final int CARDINALITY_SALES_ID = 10000;
    private final int CARDINALITY_ORDER_INPUTER = 100;
    private final int CARDINALITY_PRO_TYPE = 1000;
    private final int CARDINALITY_CURRENCY = 50;
    private final int CARDINALITY_EXCHANGE_RATE = 1000;
    private final int CARDINALITY_UNIT_COST_PRICE = 10000;
    private final int CARDINALITY_UNIT_SELLING_PRICE = 10000;
    private final int CARDINALITY_ORDER_NUM = 1000;
    private final int CARDINALITY_ORDER_DISCOUNT = 9;
    private final int CARDINALITY_ORDER_TIME = 8000000;
    private final int CARDINALITY_DELIVERY_CHANNEL = 80;
    private final int CARDINALITY_DELIVERY_ADDRESS = 10000000;
    private final int CARDINALITY_RECIPIENTS = 10000000;
    private final int CARDINALITY_CONTACT = 10000000;
    private final int CARDINALITY_DELIVERY_DATE = 10000;
    private final int CARDINALITY_COMMENTS = 10000000;

    @Override
    public void run() {
        try {
            if ("tGenerate".equals(Thread.currentThread().getName())) {
                // data generating thread
                generateData();
            } else if ("tWrite".equals(Thread.currentThread().getName())) {
                // data writing thread
                saveDataToFile();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * @Description: generating data for table order_details and called by thread tGenerate
     * @param @throws InterruptedException
     * @return void
     * @throws
     * @Author zy
     * @date 2017年3月9日 下午7:19:16
     */
    private void generateData() throws InterruptedException {
        for (int i = 0; i < TOTAL_NUM; i++) {
            StringBuffer sb = new StringBuffer();
            sb.append(i + 1)
                    // 2.订单编号
                    .append(",").append(UUID.randomUUID())
                    // 3.销售区域ID
                    .append(",").append((i+3) % CARDINALITY_SALES_AREA_ID)
                    // 4.销售人员ID
                    .append(",").append((i + 4) % CARDINALITY_SALES_ID)
                    // 5.录单人员ID
                    .append(",").append((i + 5) % CARDINALITY_ORDER_INPUTER)
                    // 6.产品型号
                    .append(",").append("PRO_TYPE_" + (i + 6) % CARDINALITY_PRO_TYPE)
                    // 7.订单币种
                    .append(",").append((i + 7) % CARDINALITY_CURRENCY)
                    // 8.当前汇率
                    .append(",").append((i + 8) % CARDINALITY_EXCHANGE_RATE)
                    // 9.成本单价
                    .append(",").append((i + 9) % CARDINALITY_UNIT_COST_PRICE)
                    // 10.销售单价
                    .append(",").append((i + 10) % CARDINALITY_UNIT_SELLING_PRICE)
                    // 11.订单数量
                    .append(",").append((i + 11) % CARDINALITY_ORDER_NUM)
                    // 12.订单金额
                    .append(",").append(((i + 10) % CARDINALITY_UNIT_SELLING_PRICE) * ((i + 11) % CARDINALITY_ORDER_NUM))
                    // 13.订单折扣
                    .append(",").append(String.format("%.2f", (i + 13) % CARDINALITY_ORDER_DISCOUNT * 0.1))
                    // 14.实际金额
                    .append(",")
                    .append(String.format("%.2f",
                            ((i + 10) % CARDINALITY_UNIT_SELLING_PRICE) * ((i + 11) % CARDINALITY_ORDER_NUM)
                                    * ((i + 13) % CARDINALITY_ORDER_DISCOUNT * 0.1)))
                    // 15.下单时间
                    .append(",")
                    .append(LocalDateTime.now().plusSeconds(i % CARDINALITY_ORDER_TIME)
                            .format(DateTimeFormatter.ofPattern("yyyy-MM-dd hh:mm:ss")))
                    // 16.发货渠道ID
                    .append(",").append((i + 16) % CARDINALITY_DELIVERY_CHANNEL)
                    // 17.发货地址
                    .append(",").append("DELIVERY_ADDRESS_" + (i + 17) % CARDINALITY_DELIVERY_ADDRESS)
                    // 18.收件人
                    .append(",").append("RECIPIENTS_" + (i + 18) % CARDINALITY_RECIPIENTS)
                    // 19.联系方式
                    .append(",").append(13800000000l + (i + 19) % CARDINALITY_CONTACT)
                    // 20.发货日期
                    .append(",").append(LocalDate.now().plusDays(i % CARDINALITY_DELIVERY_DATE))
                    // 21.备注
                    .append(",").append("RECIPIENTS_" + (i + 21) % CARDINALITY_COMMENTS);

            queue.put(sb.toString());
            if (i % BATCH_SIZE == 0) {
                System.out.println(i + " records have generated successfully.");
                System.out.println("current queue length is: " + queue.size());
            }
        }
    }

    /**
     * @Description: writing data from array block queue to file and called by thread tWrite
     * @param @throws InterruptedException
     * @param @throws IOException
     * @return void
     * @throws
     * @Author zy
     * @date 2017年3月9日 下午8:16:59
     */
    private void saveDataToFile() throws InterruptedException, IOException {
        int i = 0;
        StringBuffer sb = new StringBuffer();
        while (true) {
            sb.append(queue.take()).append("\n");
            i++;
            if (i % BATCH_SIZE == 0) {
                Files.write(Paths.get(DEST_FILE_PATH), sb.toString().getBytes(),
                        StandardOpenOption.CREATE, StandardOpenOption.APPEND);
                sb.setLength(0);

                System.out.println(i + " records have written to file successfully.");
                System.out.println("current queue length is: " + queue.size());
            }
        }
    }

    public static void main(String[] args) {
        BatchDataGenerate batchDataGenerate = new BatchDataGenerate();

        // data generating thread
        Thread thread1 = new Thread(batchDataGenerate, "tGenerate");
        // data writing thread
        Thread thread2 = new Thread(batchDataGenerate, "tWrite");

        thread1.start();
        thread2.start();
    }
}
