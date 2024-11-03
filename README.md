实验2实验报告
task1：
任务要求：根据 user_balance_table 表中的数据，统计所有⽤户每⽇的资⾦流⼊与流出情况，资⾦流⼊和流出量分别由字段 total_purchase_amt 和 total_redeem_amt表示,输出格式为<⽇期> TAB <资⾦流⼊量>,<资⾦流出量>
设计思路：日期和total_purchase_amt 和 total_redeem_amt分别为第2,5,9列，程序只需要提取除了第一行外的每一行的第2,5,9个数据，然后对相同日期的资金流入和流出进行求和，最后直接输出就好了
核心代码：提取每行的数据
            String[] fields = line.split(",");
            if (fields.length > 9) {
                // 提取第2个、第5个和第9个字段
                String transactionTime = fields[1].trim();
                int income = Integer.parseInt(fields[4].trim());
                int outcome = Integer.parseInt(fields[8].trim());
                // 流入金额
                date.set(transactionTime);
                amount.set(income);
                context.write(date, new IntWritable(amount.get()));                 
                // 流出金额
                amount.set(outcome);
                context.write(date, new IntWritable(-amount.get())); // 流出为正数，取负标记为流出
            }
       统计相同日期的数据
       // 对相同日期的资金流入和流出进行求和
            for (IntWritable value : values) {
                int amount = value.get();
                if (amount > 0) {
                    totalPurchaseAmt += amount; // 流入
                } else {
                    totalRedeemAmt += -amount; // 流出
                }
            }
成功运行截图：
<img width="733" alt="1730616633048" src="https://github.com/user-attachments/assets/e3bd6e9b-cf9b-45ca-9f31-c411c4f9c90f">
输出文件截图：
<img width="655" alt="1730616979319" src="https://github.com/user-attachments/assets/d837b8c9-1392-4518-b907-5b688b2a150d">
遇到的问题：面对资金流入和流出两个值时，提取需要使用两个变量来进行储存，比较麻烦，故设计为使用同一个数组amount来储存两个量，流入记为正数，流出记为负数，再统计求和的时候来判断数值的政府来决定加到totalPurchaseAmt还是totalRedeemAmt（为0就都不加）

task2:
任务要求：基于task1的结果，统计⼀周七天中每天的平均资⾦流⼊与流出情况，并按照资⾦流⼊量从⼤到⼩排序
设计思路：将task1的输出文件作为此次的输入文件，每一行有三个数据，分别是<⽇期> ，<资⾦流⼊量>,<资⾦流出量>，提取这三个数据，然后使用LocalDate计算是哪一天，最后将相同星期数的数据求平均
核心代码：解析日期：
                String dateStr = parts[0];
                double income = Double.parseDouble(parts[1]);
                double outcome = Double.parseDouble(parts[2]);
                // 解析日期
                int year = Integer.parseInt(dateStr.substring(0, 4));//前4个字节是年份
                int month = Integer.parseInt(dateStr.substring(4, 6));//然后2个字节是月份
                int day = Integer.parseInt(dateStr.substring(6, 8));//然后2个字节是日期
                LocalDate date = LocalDate.of(year, month, day);
                // 获取对应的星期
                int dayIndex = date.getDayOfWeek().getValue() - 1; 
         求平均值：   
            double totalIncome = 0.0;
            double totalOutcome = 0.0;
            int count = 0;
            for (Text value : values) {
                String[] incomeOutcome = value.toString().split(",");
                totalIncome += Double.parseDouble(incomeOutcome[0]);
                totalOutcome += Double.parseDouble(incomeOutcome[1]);
                count++;
            }
            // 计算平均值
            if (count > 0) {
                totals.put(key.toString(), new double[]{totalIncome / count, totalOutcome / count});
            }
          排序：
          ArrayList<Map.Entry<String, double[]>> entries = new ArrayList<>(totals.entrySet());
            entries.sort((entry1, entry2) -> Double.compare(entry2.getValue()[0], entry1.getValue()[0]));//进行比较，返回值为正表示 entry2 的收入大于 entry1，因此使用降序排列
            for (Map.Entry<String, double[]> entry : entries) {
                context.write(new Text(entry.getKey()), new Text(entry.getValue()[0] + "," + entry.getValue()[1]));
            }
成功运行截图：
<img width="749" alt="1730618259991" src="https://github.com/user-attachments/assets/6cc262b8-85b3-4e3a-bf15-a65410b00b32">
输出文件截图：
<img width="675" alt="1730618298613" src="https://github.com/user-attachments/assets/03e3d6f7-d4b2-4643-9949-55f3bc94b443">
遇到的问题：一开始我不是使用LocalDate来计算的，因为我发现一共427行，每天都是连在一起的，所以想通过读取行数来判断第一行，比如第1,8,15行，他们的key为0,7,14，能够被7整除，就是周一，但是后来发现，
每一行读取到的内容和应该的内容完全不一样，后来和同学讨论得知，有的数据文件为将整体分开，key可能会重置，并不是从0到426，所以不可以使用行数来判断日期，一定要使用LocalDate

task3:
任务要求：当⽤户当⽇有直接购买（ direct_purchase_amt 字段⼤于0）或赎回⾏为（ total_redeem_amt字段⼤于0）时，则该⽤户当天活跃。统计每个⽤户的活跃天数，并按照活跃天数降序排列。
设计思路：读取输入文件除了第一行的每一行的第1，6,9行的数据，分别为用户id和 direct_purchase_amt和total_redeem_amt，若如果directPurchaseAmt > 0 或totalRedeemAmt > 0，则认为该用户活跃，数组写入1，若不活跃则写入0
最后遍历整个数组，将相同用户的活跃天数加起来，再排序输出
核心代码：判断是否活跃：
                // 如果金额大于0，则认为该用户活跃
                if (directPurchaseAmt > 0 || totalRedeemAmt > 0) {
                    context.write(userId, activeDays);
                } else {
                    // 即使不活跃也输出，活跃天数为 0
                    context.write(userId, new LongWritable(0));
                }
         求和：
         for (LongWritable val : values) {
                activeDaysCount += val.get();
            }
         将结果按活跃天数降序排列：
            List<Map.Entry<Text, LongWritable>> entryList = new ArrayList<>(userActivityMap.entrySet());
            Collections.sort(entryList, new Comparator<Map.Entry<Text, LongWritable>>() {
                @Override
                public int compare(Map.Entry<Text, LongWritable> o1, Map.Entry<Text, LongWritable> o2) {
                    return Long.compare(o2.getValue().get(), o1.getValue().get()); // 降序
                }
            });
成功运行截图：
<img width="729" alt="1730619109267" src="https://github.com/user-attachments/assets/d8dc81a9-f68c-4bd4-a55a-0a0562cc89ef">
输出文件截图：
<img width="488" alt="1730619163040" src="https://github.com/user-attachments/assets/4f821381-9f89-4c4e-bfb0-f7abc9b35036">
遇到的问题：一开始判断是否活跃时，我只给活跃的写入了1，但是不活跃的没有写入0，相当于没有记录，最后在输出时，输出文件的末尾的活跃天数都是1，没有记录活跃天数为0的用户，后来要修改为不活跃也要记录0，这样才有该用户的记录，不然会查无此人

task4:
任务要求：从其他的表中⾃⾏选取研究对象，统计该因素和用户的数值关系，阐述某⼀因素对⽤户交易⾏为的影响
设计思路：选取user_profile_table.csv作为输入文件，记录每一行的第三个数据城市代号，该文件每一行都是一个用户的数据信息，统计每个城市的用户数量，然后降序输出
核心代码：获取城市代号：
            String[] fields = value.toString().split(",");
            if (fields.length > 2) {
                // 获取城市代号（第3列）
                cityId.set(fields[2].trim());
                context.write(cityId, one);
            }
          记录每个城市的用户数量：
          int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            cityCountMap.put(new Text(key), new IntWritable(sum));
          排序输出：
          cityCountMap.entrySet().stream()
                .sorted((e1, e2) -> e2.getValue().get() - e1.getValue().get())
                .forEach(entry -> {
                    try {
                        context.write(entry.getKey(), entry.getValue());
                    } catch (IOException | InterruptedException e) {
                        e.printStackTrace();
                    }
                });
成功运行截图：
<img width="729" alt="1730619709668" src="https://github.com/user-attachments/assets/f879a4c0-4579-4bad-b77b-96bd3790ac4b">
输出文件截图：
<img width="686" alt="1730619744713" src="https://github.com/user-attachments/assets/7d4f9095-87f1-4bcc-874c-b51101e6ceeb">
分析：由此可见，不同城市的用户数量差距极大，得出结论：地区对⽤户交易⾏为有着极大影响
