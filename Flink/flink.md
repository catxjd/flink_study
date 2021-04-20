## 第一章 概述

### 1.1流处理技术的演变

​		在开源世界里，Apache Storm项目是流处理的先锋。Storm最早由Nathan Marz和创业公司BackType的一个团队开发，后来才被Apache基金会接纳。Storm提供了低延迟的流处理，但是它为实时性付出了一些代价：很难实现高吞吐，并且其正确性没能达到通常所需的水平，换句话说，它并不能保证exactly-once，即便是它能够保证的正确性级别，其开销也相当大。

​		在低延迟和高吞吐的流处理系统中维持良好的容错性是非常困难的，但是为了得到有保障的准确状态，人们想到了一种替代方法：将连续时间中的流数据分割成一系列微小的批量作业。如果分割得足够小（即所谓的微批处理作业），计算就几乎可以实现真正的流处理。因为存在延迟，所以不可能做到完全实时，但是每个简单的应用程序都可以实现仅有几秒甚至几亚秒的延迟。这就是在Spark批处理引擎上运行的Spark Streaming所使用的方法。 

​		更重要的是，使用微批处理方法，可以实现exactly-once语义，从而保障状态的一致性。如果一个微批处理失败了，它可以重新运行，这比连续的流处理方法更容易。Storm Trident是对Storm的延伸，它的底层流处理引擎就是基于微批处理方法来进行计算的，从而实现了exactly-once语义，但是在延迟性方面付出了很大的代价。

​		 对于Storm Trident以及Spark Streaming等微批处理策略，只能根据批量作业时间的倍数进行分割，无法根据实际情况分割事件数据，并且，对于一些对延迟比较敏感的作业，往往需要开发者在写业务代码时花费大量精力来提升性能。这些灵活性和表现力方面的缺陷，使得这些微批处理策略开发速度变慢，运维成本变高。

​		于是，Flink出现了，这一技术框架可以避免上述弊端，并且拥有所需的诸多功能，还能按照连续事件高效地处理数据。

### 1.2初识Flink

![image-20210419165531856](https://github.com/catxjd/flink_study/blob/master/Flink/images/flink_logo.png?raw=true)



​		Flink主页在其顶部展示了该项目的理念：“Apache Flink是为分布式、高性能、随时可用以及准确的流处理应用程序打造的开源流处理框架”。 



### 1.3 Flink核心计算框架

​		Flink的核心计算架构是下图中Flink Runtime执行引擎，它是一个分布式系统，能够接受数据流程序并在一台或多台机器上以容错方式执行。

​		Flink Runtime执行引擎可以作为YARN的应用程序在集群上运行，也可以在Mesos集群上运行，还可以在单机上运行。

![image-20210419165531857](https://github.com/catxjd/flink_study/tree/master/Flink/images/Flink核心计算框架.png)



​		上图为Flink技术栈的核心组成部分，值得一提的是，<b><font color='red'>Flink分别提供了面向流式处理的接口（DataStream API）和面向批处理的接口（DataaSet API）</font></b>。因此，Flink既可以完成批处理 ，又可以完成流处理。Flink支持的拓展库设计机器学习（FlinkML）、复杂事件处理（CEP）、以及图计算（Gelly），还有分别针对流处理和批处理的Table API。

​		能被Flink Runtime执行引擎接受的程序很强大，但是这样的程序有着冗长的代码，编写起来也费力，基于这样原因，Flink提供了封装在Runtime执行引擎上的API，以帮助用户方便的生成流式计算程序。<b><font color='red'>Flink提供了用于 流处理的DataStream API和用于批处理的DataSet API</font></b>。值得注意的是，尽管Flink Runtime执行引擎是基于流处理的，但是DataSet API先于DataStream API被开发出来，这是因为工业界对无限流处理的需求在Flink诞生之初并不大。

​		<b><font color='red'>Flink的分布式特点体现在它能够在成百上千台机器上运行，它将大型的计算任务分成许多小的部分，每个机器执行一部分</font></b>。Flink能够自动地确保发生机器故障或者其他错误时计算能够持续进行，或者在修复bug或进行版本升级后有计划地再执行一次。这种能力使得开发人员不需要担心运行失败。Flink本质上使用容错性数据流，这使得开发人员可以分析持续生成且永远不结束的数据（即流处理）。



##  第二章 Flink基本架构

### 2.1 JobManager与TaskManager 

Flink运行时包含了两种类型的处理器： 

<b>JobManager处理器</b>：也称之为Master，用于协调分布式执行，它们用来调度task，协调检查点，协调失败时恢复等。Flink运行时至少存在一个master处理器，如果配置高可用模式则会存在多个master处理器，它们其中有一个是leader，而其他的都是standby。 

<b>TaskManager处理器</b>：也称之为Worker，用于执行一个dataflow的task(或者特殊的subtask)、数据缓冲和data stream的交换，Flink运行时至少会存在一个worker处理器。 

​		Master和Worker处理器可以直接在物理机上启动，或者通过像YARN这样的资源调度框架。

​		 Worker连接到Master，告知自身的可用性进而获得任务分配。

### 2.2 无界数据流与有界数据流 

Flink用于处理有界和无界数据： 

**无界数据流**：**<font color='red'>无界数据流有一个开始但是没有结束</font>**，它们不会在生成时终止并提供数据，必须连续处理无界流，也就是说必须在获取后立即处理event。对于无界数据流我们无法等待所有数据都到达，因为输入是无界的，并且在任何时间点都不会完成。处理无界数据通常要求以特定顺序（例如事件发生的顺序）获取event，以便能够推断结果完整性，无界流的处理称为流处理。 		
 **有界数据流**：**<font color='red'>有界数据流有明确定义的开始和结束</font>**，可以在执行任何计算之前通过获取所有数据来处理有界流，处理有界流不需要有序获取，因为可以始终对有界数据集进行排序，有界流的处理也称为批处理。

![avatar](\images\有界流和无界流.png)





​		在无界数据流和有界数据流中我们提到了批处理和流处理，这是大数据处理系统中常见的两种数据处理方式。

**<font color='red'>批处理的特点是有界、持久、大量，批处理非常适合需要访问全套记录才能完成的计算工作，一般用于离线统计。流处理的特点是无界、实时，流处理方式无需针对整个数据集执行操作，而是对通过系统传输的每个数据项执行操作，一般用于实时统计。</font>**

​		 在Spark生态体系中，对于批处理和流处理采用了不同的技术框架，批处理由SparkSQL实现，流处理由Spark Streaming实现，这也是大部分框架采用的策略，使用独立的处理器实现批处理和流处理，而Flink可以同时实现批处理和流处理。 

​		Flink是如何同时实现批处理与流处理的呢？答案是，**<font color='red'>Flink将批处理（即处理有限的静态数据）视作一种特殊的流处理</font>**。         		**<font color='red'>Apache Flink是一个面向分布式数据流处理和批量数据处理的开源计算平台，它能够基于同一个Flink运行时(Flink Runtime)，提供支持流处理和批处理两种类型应用的功能</font>**。现有的开源计算方案，会把流处理和批处理作为两种不同的应用类型，因为它们要实现的目标是完全不相同的：**<font color='red'>流处理一般需要支持低延迟、Exactly-once保证，而批处理需要支持高吞吐、高效处理</font>**，所以在实现的时候通常是分别给出两套实现方法，或者通过一个独立的开源框架来实现其中每一种处理方案。例如，实现批处理的开源方案有MapReduce、Tez、Crunch、Spark，实现流处理的开源方案有Samza、Storm。 

​			Flink在实现流处理和批处理时，与传统的一些方案完全不同，它从另一个视角看待流处理和批处理，将二者统一起来：**<font color='red'>Flink是完全支持流处理，也就是说作为流处理看待时输入数据流是无界的；批处理被作为一种特殊的流处理，只是它的输入数据流被定义为有界的</font>**。基于同一个Flink运行时(Flink Runtime)，分别提供了流处理和批处理API，而这两种API也是实现上层面向流处理、批处理类型应用框架的基础。 

### 2.3 数据流编程模型 

<font size=2>Flink提供了不同级别的抽象，以开发流或批处理作业，如下图所示</font>

![streammodel](https://github.com/catxjd/flink_study/tree/master/Flink/images/流编程模型.png)



​		<font size=2>最底层级的抽象仅仅提供了有状态流，它将通过过程函数（Process Function）被嵌入到DataStream API中。底层过程函数（Process Function） 与 DataStream API 相集成，使其可以对某些特定的操作进行底层的抽象，它允许用户可以自由地处理来自一个或多个数据流的事件，并使用一致的容错的状态。除此之外，用户可以注册事件时间并处理时间回调，从而使程序可以处理复杂的计算。</font>

​		 <font size=2>实际上，**<font color='red'>大多数应用并不需要上述的底层抽象，而是针对核心API（Core APIs） 进行编程，比如DataStream API（有界或无界流数据）以及DataSet API（有界数据集）</font>**。这些API为数据处理提供了通用的构建模块，比如由用户定义的多种形式的转换（transformations），连接（joins），聚合（aggregations），窗口操作（windows）等等。DataSet API 为有界数据集提供了额外的支持，例如循环与迭代。这些API处理的数据类型以类（classes）的形式由各自的编程语言所表示。</font>

 		<font size=2>Table API 以表为中心，其中表可能会动态变化（在表达流数据时）。Table API遵循（扩展的）关系模型：表有二维数据结构（schema）（类似于关系数据库中的表），同时API提供可比较的操作，例如select、project、join、group-by、aggregate等。Table API程序声明式地定义了什么逻辑操作应该执行，而不是准确地确定这些操作代码的看上去如何 。 尽管Table API可以通过多种类型的用户自定义函数（UDF）进行扩展，其仍不如核心API更具表达能力，但是使用起来却更加简洁（代码量更少）。除此之外，Table API程序在执行之前会经过内置优化器进行优化。 </font>

​		<font size=2 color='red'>**你可以在表与 DataStream/DataSet 之间无缝切换，以允许程序将 Table API 与 DataStream 以及 DataSet 混合使用**。</font>

<font size=2>		 Flink提供的最高层级的抽象是 SQL 。这一层抽象在语法与表达能力上与 Table API 类似，但是是以SQL查询表达式的形式表现程序。SQL抽象与Table API交互密切，同时SQL查询可以直接在Table API定义的表上执行。</font>



## 第三章 Flink运行架构

###   3.1 任务提交流程

![任务提交流程](https://github.com/catxjd/flink_study/tree/master/Flink/images/任务提交流程.png)



​		Flink任务提交后，Client向HDFS上传Flink的Jar包和配置，之后向Yarn ResourceManager提交任务，ResourceManager分配Container资源并通知对应的NodeManager启动ApplicationMaster，ApplicationMaster启动后加载Flink的Jar包和配置构建环境，然后启动JobManager，之后ApplicationMaster向ResourceManager申请资源启动TaskManager，ResourceManager分配Container资源后，由ApplicationMaster通知资源所在节点的NodeManager启动TaskManager，NodeManager加载Flink的Jar包和配置构建环境并启动TaskManager，TaskManager启动后向JobManager发送心跳包，并等待JobManager向其分配任务。

###  3.2 TaskManager与Slots

​		**<font color='red'>每一个TaskManager是一个JVM进程，它可能会在独立的线程上执行一个或多个subtask</font>**。为了控制一个worker能接收多少个task，worker通过task slot来进行控制（一个worker至少有一个task slot）。

​		每个task slot表示TaskManager拥有资源的一个固定大小的子集。假如一个TaskManager有三个slot，那么它会将其管理的内存分成三份给各个slot。**<font color='red'>资源slot化意味着一个subtask将不需要跟来自其他job的subtask竞争被管理的内存，取而代之的是它将拥有一定数量的内存储备</font>**。需要注意的是，这里不会涉及到CPU的隔离，slot目前仅仅用来隔离task的受管理的内存。

​		 **<font color='red'>通过调整task slot的数量，允许用户定义subtask之间如何互相隔离</font>**。如果一个TaskManager一个slot，那将意味着每个task group运行在独立的JVM中（该JVM可能是通过一个特定的容器启动的），而一个TaskManager多个slot意味着更多的subtask可以共享同一个JVM。而在同一个JVM进程中的task将共享TCP连接（基于多路复用）和心跳消息。它们也可能共享数据集和数据结构，因此这减少了每个task的负载。

![slots](https://github.com/catxjd/flink_study/tree/master/Flink/images/slots.png)



​		**<font color='red'>Task Slot是静态的概念，是指TaskManager具有的并发执行能力</font>**，可以通过参数taskmanager.numberOfTaskSlots进行配置，而**<font color='red'>并行度parallelism是动态概念，即TaskManager运行程序时实际使用的并发能力</font>**，可以通过参数parallelism.default进行配置。 

​		也就是说，假设一共有3个TaskManager，每一个TaskManager中的分配3个TaskSlot，也就是每个TaskManager可以接收3个task，一共9个TaskSlot，如果我们设置parallelism.default=1，即运行程序默认的并行度为1，9个TaskSlot只用了1个，有8个空闲，因此，设置合适的并行度才能提高效率。

## 第四章 Flink DataStream API

###  4.1 Flink运行模型

![Flink运行模型.png](https://github.com/catxjd/flink_study/tree/master/Flink/images/Flink运行模型.png)



​		以上为Flink的运行模型，Flink的程序主要由三部分构成，分别为Source、Transformation、Sink。DataSource主要负责数据的读取，Transformation主要负责对属于的转换操作，Sink负责最终数据的输出。

### 4.2 Flink程序架构

 每个Flink程序都包含以下的若干流程：

-  获得一个执行环境；（Execution Environment） 
- 加载/创建初始数据；（Source）
-  指定转换这些数据；（Transformation）
-  指定放置计算结果的位置；（Sink）
-  触发程序执行

### 4.3 Enviorment

​		**<font size=3 color='red'>执行环境StreamExecutionEnvironment是所有Flink程序的基础。</font>** 

创建执行环境有三种方式，分别为

```
StreamExecutionEnvironment.getExecutionEnvironment StreamExecutionEnvironment.createLocalEnvironment StreamExecutionEnvironment.createRemoteEnvironment 
```

​	第一种：创建一个执行环境，表示当前执行程序的上下文。如果程序是独立调用，则返回本地执行环境；如果是从命令行客户端的调用程序以提交到集群，则此方法返回此集群的执行环境。

```
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
```



第二种：返回本地执行环境，需要在调用时指定默认的并行度。

```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);
```

第三种：返回集群执行环境，将Jar提交到远程服务器。需要在调用时指定JobManager的IP和端口号，并指定要在集群中运行的Jar包。

```
StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment("10.10.1.1",123,"test.jar");
```



### 4.4 Source

#### 4.4.1基于File的数据源

- ***readTextFile(path)*** 

一列一列的读取遵循TextInputFormat规范的文本文件，并将结果作为String返回

```
env.readTextFile("/opt/modules/test.txt") 
```

- **readFile(fileInputFormat, path)**

按照指定的文件格式读取文件。

```
 env.readFile(new TextInputFormat(path), "/opt/modules/test.txt") 
```

#### 4.4.2 基于socket的数据源

***socketTextStream(ip,port)***

从Socket中读取信息，元素可以用分隔符分开。

```
 env.socketTextStream("localhost", 11111) 
```

#### 4.4.3基于集合的数据源

fromCollection(seq)

从集合中创建一个数据流，集合中所有元素的类型是一致的。

```
val list = List(1,2,3,4) 
val stream = env.fromCollection(list)
```

fromCollection(Iterator) 

从迭代(Iterator)中创建一个数据流，指定元素数据类型的类由iterator返回

```
val iterator = Iterator(1,2,3,4) 
val stream = env.fromCollection(iterator)
```

fromElements(elements:_*) 

```java
 List<Integer> list1 = Arrays.asList(1, 2, 3, 4);
 DataStreamSource<List<Integer>> listDataStreamSource = env.fromElements(list1);
```

generateSequence(from, to) 

从给定的间隔中并行地产生一个数字序列

```
val stream = env.generateSequence(1,10)
```

### 4.5 Sink

​	Data Sink消费DataStream中的数据，并将它们转发到文件、套接字、外部系统或者打印出。

​	Flink有许多封装在DataStream操作里面的内置输出格式。

#### 4.5.1 writeAsText

​		将元素以字符串形式逐行写入（TextOutputFormat），这些字符串通过调用每个元素的toString()方法来获取。

#### 4.5.2 WriteAsCsv

将元组以逗号分隔写入文件中（CsvOutputFormat），行及字段之间的分隔是可配置的。每个字段的值来自对象的toString()方法。

#### 4.5.3 print/printToErr

打印每个元素的toString()方法的值到标准输出或者标准错误输出流中。或者也可以在输出流中添加一个前缀，这个可以帮助区分不同的打印调用，如果并行度大于1，那么输出也会有一个标识由哪个任务产生的标志。

#### 4.5.4 writeUsingOutputFormat

自定义文件输出的方法和基类（FileOutputFormat），支持自定义对象到字节的转换。 

#### 4.5.5 writeToSocket

根据SerializationSchema 将元素写入到socket中。



### 4.6 Transformation

#### 4.6.1 Map

输入是一个数据流，输出也是一个数据流，常做清洗或者转换成对象。

eg1：将数据流的年龄进行清洗

```
SingleOutputStreamOperator<Student> map = student.map(new MapFunction<Student, Student>() {
    @Override
    public Student map(Student value) throws Exception {
        Student s1 = new Student();
        s1.id = value.id;
        s1.name = value.name;
        s1.password = value.password;
        s1.age = value.age + 5;
        return s1;
    }
})
```

eg2:将输入流转换成UserBehavior类型

```
 DataStream<UserBehavior> dataStream = inputDataStream.map(line -> {
            String[] fields = line.split(",");
            return new UserBehavior(new Long(fields[0]), new Long(fields[1]), new Integer(fields[2]),fields[3], new Long(fields[4]));     
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
            @Override
            public long extractAscendingTimestamp(UserBehavior userBehavior) {
                return userBehavior.getTimestamp() * 1000L;
            }
        });
```

#### 4.6.2 FlatMap

FlatMap 采用一条记录并输出零个，一个或多个记录。

```
SingleOutputStreamOperator<Student> flatMap = student.flatMap(new FlatMapFunction<Student, Student>() {
    @Override
    public void flatMap(Student value, Collector<Student> out) throws Exception {
        if (value.id % 2 == 0) {
            out.collect(value);
        }
    }
});

```

这里将id为偶数的聚集起来。

#### 4.6.3 Filter

过滤筛选，将所有符合判断条件的结果集输出

```
SingleOutputStreamOperator<Entity> result = StreamRecord
       .filter(new FilterFunction<Entity>() {
        @Override
       public boolean filter(Entity entity) throws Exception {
              if (entity.phoneName.equals("HUAWEI")) {
                  return true;
              }
              return false;
       }
});
```

将所有phoneName是HUAWEI的值过滤，在直接输出。

#### 4.6.4 Connect

![Connect算子.png](https://github.com/catxjd/flink_study/tree/master/Flink/images/Connect算子.png)

​		**DataStream,DataStream → ConnectedStreams**：连接两个保持他们类型的数据流，两个数据流被Connect之后，只是被放在了一个同一个流中，内部依然保持各自的数据和形式不发生任何变化，两个流相互独立。



####  4.6.5 CoMap,CoFlatMap

![comap](https://github.com/catxjd/flink_study/tree/master/Flink/images/comap.png)

​		**ConnectedStreams → DataStream**：作用于ConnectedStreams上，功能与map和flatMap一样，对ConnectedStreams中的每一个Stream分别进行map和flatMap处理。 

#### 4.6.6 split

![split](https://github.com/catxjd/flink_study/tree/master/Flink/images/split.png)

​		**DataStream → SplitStream**：根据某些特征把一个DataStream拆分成两个或者多个DataStream。

```
SplitStream<UserBehavior> splitStream = dataStream.split(new OutputSelector<UserBehavior>() {
            @Override
        public Iterable<String> select(UserBehavior userBehavior) {
                List<String> filterList = new ArrayList<String>();
                if ("pv".equals(userBehavior.getBehavior())) {
                    filterList.add("pv");
                } else if (userBehavior.getBehavior().equals("cart")) {
                    filterList.add("cart");
                } else {
                    filterList.add("other");
                }
                return filterList;
            }
        });
```

​		将datastream拆分成3类，一类包含pv一类为cart，其他一类。

#### 4.6.7 select

![select](https://github.com/catxjd/flink_study/tree/master/Flink/images/select.png)

​		**SplitStream→DataStream**：从一个SplitStream中获取一个或者多个DataStream。

```
DataStream<UserBehavior> cartdataStream = splitStream.select("cart");
DataStream<UserBehavior> pvdataStream = splitStream.select("pv");
```

将splitstream中获取dataStream



#### 4.6.8 Union

<img src="https://github.com/catxjd/flink_study/tree/master/Flink/images/union.png" alt="union" style="zoom:50%;" />

​		**DataStream → DataStream**：对两个或者两个以上的DataStream进行union操作，产生一个包含所有DataStream元素的新DataStream。注意:如果你将一个DataStream跟它自己做union操作，在新的DataStream中，你将看到每一个元素都出现两次。

```
DataStream<UserBehavior> cartdataStream = splitStream.select("cart");
DataStream<UserBehavior> pvdataStream = splitStream.select("pv");
cartdataStream.union(pvdataStream).print();
```

将只包含pv的和只包含cart的合并一起的结果。

#### 4.6.9 KeyBy

​		**DataStream → KeyedStream**：输入必须是Tuple类型，逻辑地将一个流拆分成不相交的分区，每个分区包含具有相同key的元素，在内部以hash的形式实现的。

```
 DataStream windowAggStream = dataStream.filter(data -> "pv".equals(data.getBehavior()))
                .keyBy("itemId","userId")
                .timeWindow(Time.hours(1), Time.minutes(5))
                .aggregate(new ItemCountAgg(), new WindowCountResult());
```

#### 4.6.10 Reduce

​		**KeyedStream → DataStream**：一个分组数据流的聚合操作，合并当前的元素和上次聚合的结果，产生一个新的值，返回的流中包含每一次聚合的结果，而不是只返回最后一次聚合的最终结果

```
//分组
 KeyedStream<SensrReading, Tuple> keyedStream = dataStream.keyBy("id");
  //reduce聚合，取最大的温度值，以及当前最新的时间戳
 keyedStream.reduce(new ReduceFunction<SensrReading>() {
     @Override
     public SensrReading reduce(SensrReading value1, SensrReading value2) throws Exception {
                return new SensrReading(value1.getId(),value2.getTimestamp(),Math.max(value1.getTemperature(),value2.getTemperature()));
            }
        });

```

#### 4.6.11 Fold

​		**KeyedStream → DataStream**：一个有初始值的分组数据流的滚动折叠操作，合并当前元素和前一次折叠操作的结果，并产生一个新的值，返回的流中包含每一次折叠的结果，而不是只返回最后一次折叠的最终结果。



#### 4.6.12 Aggregations

​		**KeyedStream → DataStream**：分组数据流上的滚动聚合操作。min和minBy的区别是min返回的是一个最小值，而minBy返回的是其字段中包含最小值的元素(同样原理适用于max和maxBy)，返回的流中包含每一次聚合的结果，而不是只返回最后一次聚合的最终结果。 



## 第五章  Time与Window

### 5.1 Time

在Flink的流式处理中，会涉及到时间的不同概念，如下图所示：

![flink_time](https://github.com/catxjd/flink_study/tree/master/Flink/images/flink_time.png)

​	**Event Time**：是事件创建的时间。它通常由事件中的时间戳描述，例如采集的日志数据中，每一条日志都会记录自己的生成时间，Flink通过时间戳分配器访问事件时间戳。

​	**Ingestion Time**：是数据进入Flink的时间。 

​	**Processing Time**：是每一个执行基于时间操作的算子的本地系统时间，与机器相关，默认的时间属性就是Processing Time。

​		<font size=3>例如，一条日志进入Flink的时间为2017-11-12 10:00:00.123，到达Window的系统时间为2017-11-12 10:00:01.234，日志的内容如下：</font>

<font size=2>2017-11-02 18:37:15.624 INFO Fail over to rm2 </font>

​		<font size=3>对于业务来说，要统计1min内的故障日志个数，哪个时间是最有意义的？—— eventTime，因为我们要根据日志的生成时间进行统计。 </font>

### 5.2 Window类型

#### 5.2.1 window概述

​	streaming流式计算是一种被设计用于处理无限数据集的数据处理引擎，而无限数据集是指一种不断增长的本质上无上限的数据集，而windwow是一种切割无限数据为有限块进行处理的手段。

​		window是无限数据流处理的核心，Window将一个无限的stream拆分成有限打的“buckets”桶，我们可以在这些桶上做计算操作。

#### 5.2.2 Window类型

window可以分为两类：

- CountWindow：按照指定数据条数生成一个window，与时间无关。
- TimeWindow：按照时间生成window

对于TimeWindow，可以根据窗口实现原理的不同分成三类：滚动窗口（Tumbling Window）、滑动窗口(slidiing Window)和会话窗口。

1. 滚动窗口（Tumbling Windows）

**<font color='red'>将数据依据固定的窗口长度对数据进行切片。</font>**

**特点：<font color='red'>时间对齐，窗口长度固定，没有重叠。</font>** 

滚动窗口分配器将每个元素分配到一个指定窗口大小的窗口中，滚动窗口有一个固定的大小，并且不会出现重叠。例如：如果你指定了一个5分钟大小的滚动窗口，窗口的创建如下图所示： 

![滚动窗口](/images/滚动窗口.png)

**适用场景**：适合做BI统计等（做 每个时间段的聚合计算）。

2. 滑动窗口（Sliding Windows） 

**<font color='red'>滑动窗口是固定窗口的更广义的一种形式，滑动窗口由固定的窗口长度和滑动间隔组成</font>**。

 **特点：<font color='red'>时间对齐，窗口长度固定，有重叠</font>**。

​	滑动窗口分配器将元素分配到固定长度的窗口中，与滚动窗口类似，窗口的大小由窗口大小参数来配置，另一个窗口滑动参数控制滑动窗口开始的频率。因此，滑动窗口如果滑动参数小于窗口大小的话，窗口是可以重叠的，在这种情况下元素会被分配到多个窗口中。

​		 例如，你有10分钟的窗口和5分钟的滑动，那么每个窗口中5分钟的窗口里包含着上个10分钟产生的数据，如下图所示： 

![滑动窗口](https://github.com/catxjd/flink_study/tree/master/Flink/images/滑动窗口.png)

**适用场景**：对最近一个时间段内的统计（求某接口最近5min的失败率来决定是否要报警）。 

3. 会话窗口（Session Windows）

    **<font color='red'>由一系列事件组合一个指定时间长度的timeout间隙组成，类似于web应用的session，也就是一段时间没有接收到新数据就会生成新的窗口</font>**

    **特点：<font color='red'>时间无对齐</font>**。

   ​		session窗口分配器通过session活动来对元素进行分组，session窗口跟滚动窗口和滑动窗口相比，不会有重叠和固定的开始时间和结束时间的情况，相反，**当它在一个固定的时间周期内不再收到元素，即非活动间隔产生，那个这个窗口就会关闭**。一个session窗口通过一个session间隔来配置，这个session间隔定义了非活跃周期的长度，当这个非活跃周期产生，那么当前的session将关闭并且后续的元素将被分配到新的session窗口中去。

![会话窗口](https://github.com/catxjd/flink_study/tree/master/Flink/images/会话窗口.png)

### 5.3 Window API

#### 5.3.1 ConutWindow

​		**<font size=2 color='red'>CountWindow根据窗口中相同key元素的数量来触发执行，执行时只计算元素数量达到窗口大小的key对应的结果。</font>**

​		 **<font size=2 color='red'>注意：CountWindow的window_size指的是相同Key的元素的个数，不是输入的所有元素的总数。</font>**

1 滚动窗口 

默认的CountWindow是一个滚动窗口，只需要指定窗口大小即可，当元素数量达到窗口大小时，就会触发窗口的执行。

```
URL resource = Demo.class.getResource("/test.txt");
DataStream inputDataStream = env.readTextFile(resource.getPath());
        
DataStream<Tuple2<String, Integer>> windowCount = inputDataStream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
     public void flatMap(String line, Collector<Tuple2<String, Integer>> collector) throws Exception {
        String[] words = line.split(" ");
        for (String word : words) {
          //将每个单词与 1 组合，形成一个元组
          Tuple2<String, Integer> tp = Tuple2.of(word, 1);
           //将组成的Tuple放入到 Collector 集合，并输出
           collector.collect(tp);
          }
       }
     });
   DataStream<Tuple2<String, Integer>> sumed  = windowCount.keyBy(0).countWindow(5).sum(1);
   sumed.print();
```

2 滑动窗口

滑动窗口和滚动窗口的函数名是完全一致的，只是在传参数时需要传入两个参数，一个是window_size，一个是sliding_size。

 下面代码中的sliding_size设置为了2，也就是说，每收到两个相同key的数据就计算一次，每一次计算的window范围是5个元素。

```
DataStream<Tuple2<String, Integer>> sumed  = windowCount.keyBy(0).countWindow(5,2).sum(1);
```

#### 5.3.2 TimeWindow

​		TimeWindow是将指定时间范围内的所有数据组成一个window，一次对一个window里面的所有数据进行计算。

1. 滚动窗口 

   ​		Flink默认的时间窗口根据Processing Time 进行窗口的划分，将Flink获取到的数据根据进入Flink的时间划分到不同的窗口中。

   ```
   DataStream windowAggStream = dataStream.filter(data -> "pv".equals(data.getBehavior()))
                   .keyBy("itemId")
                   .timeWindow(Time.seconds(5))
                   .aggregate(new ItemCountAgg(), new WindowCountResult());
   ```

   ​		时间间隔可以通过Time.milliseconds(x)，Time.seconds(x)，Time.minutes(x)等其中的一个来指定。

2. 滑动窗口（SlidingEventTimeWindows）

   滑动窗口和滚动窗口的函数名是完全一致的，只是在传参数时需要传入两个参数，一个是window_size，一个是sliding_size。 下面代码中的sliding_size设置为了2s，也就是说，窗口每2s就计算一次，每一次计算的window范围是5s内的所有元素。

   ```
   DataStream windowAggStream = dataStream.filter(data -> "pv".equals(data.getBehavior()))
                   .keyBy("itemId")
                   .timeWindow(Time.seconds(5), Time.seconds(2))
                   .aggregate(new ItemCountAgg(), new WindowCountResult());
   ```

   ​		时间间隔可以通过Time.milliseconds(x)，Time.seconds(x)，Time.minutes(x)等其中的一个来指定。 
#### 5.3.3 Window Reduce

   ​		**WindowedStream → DataStream：**给window赋一个reduce功能的函数，并返回一个聚合的结果

   ```
   DataStream<WordWithCount> windowcounts = socketWord.flatMap(new FlatMapFunction<String, WordWithCount>() {
               public void flatMap(String value, Collector<WordWithCount> out) throws Exception {
                   String[] splits = value.split("\\s");
                   for (String word : splits) {
                       out.collect(new WordWithCount(word, 1));
                   }
               }
           }).keyBy("word")     
                   //.sum("count");//这里求聚合 可以用reduce和sum两种方式
                   .reduce(new ReduceFunction<WordWithCount>() {
                       public WordWithCount reduce(WordWithCount a, WordWithCount b) throws Exception {
                           return new WordWithCount(a.word, a.count + b.count);
                       }
                   });
   ```

   #### 5.3.4 Window Fold

   ​		**WindowedStream → DataStream**：给窗口赋一个fold功能的函数，并返回一个fold后的结果。

   ```
   
   KeyedStream.fold("1", new FoldFunction<Integer, String>() {
       @Override
       public String fold(String accumulator, Integer value) throws Exception {
           return accumulator + "=" + value;
       }
   })
   ```

   ***小tips：reduce和fold的区别***

   相同点：

   1 均是对相同类型的元素进行合并

   2 均是把组内的所有元素合并成一个值

   不同点：

   1 reduce是组内的2个元素合并成一个同类型的新元素；fold是组内的每个元素与累加器（一开始是初始值initialValue）合并再返回累加器，累加器的类型可以与组内的元素类型不一致；

   2 reduce可以用于DataStream或DataSet，但是fold只能用于DataStream。



#### 5.3.5 Aggregation on Window 

**WindowedStream → DataStream**：对一个window内的所有元素做聚合操作。min和 minBy的区别是min返回的是最小值，而minBy返回的是包含最小值字段的元素(同样的原理适用于 max 和 maxBy)。 



## 第六章 EventTime与Window

### 6.1 EventTime引入

​		**<font color='red'>在Flink的流式处理中，绝大部分的业务都会使用eventTime，一般只在eventTime无法使用时，才会被迫使用ProcessingTime或者IngestionTime</font>**。 

​		如果要使用EventTime，那么需要引入EventTime的时间属性，引入方式如下所示： 

```
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
 env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
```

### 6.2 Watermark

#### 6.2.1基本概念

​		我们知道，流处理从事件产生，到流经source，再到operator，中间有一个过程和时间的，虽然大部分情况下，留到operator的数据都是按照事件产生的时间顺序来的，但是也不排除由于网络原因，导致乱序的产生，所谓乱序，就是指Flink接受到的事件的先后顺序不是严格按照事件的Event Time顺序排列的。

![watermark](https://github.com/catxjd/flink_study/tree/master/Flink/images/watermark.png)

​		那么此时出现一个问题，一旦出现乱序，如果只根据eventime决定window的运行，我们不能明确数据是否全部到位，但又不能无限期的等下去，此时必须要有一个机制来保证一个特定的时间后，必须触发window去计算了，这个特别的机制就是watermark。

​		watermark是一种衡量Event Time进展的机制，它是数据本身的一个隐藏属性，数据本身携带着对应的watermark。

​		**<font color="red">watermark是用于处理乱序事件的，而正确的处理乱序事件，通常用watermark结合window来实现</font>**。

​		**<font color='red'>数据流中的watermark用于表示timestamp小于watermark的数据，都已经到达了，因此，window的执行也是由watermark触发的。</font>**

​		<font color='red'>**watermark可以理解成一个延迟触发机制，我们可以设置watermark的延时时长为t，每次系统会校验已经到达的数据中最大的maxEventTime，然后认定eventTime小于maxEventTime -t的所有数据都已经到达，如果有窗口的停止时间等于maxEventTime - t，那么这个窗口被触发执行**。</font>

​		**<font color='red'>当Flink接收到每一条数据时，都会产生一条watermark，这条watermark就等于当前所有到达数据中的maxEventTime -延迟时长，也就是说，watermark是由数据携带的，一旦数据携带的watermark比当前未触发的窗口的停止时间要晚，那么就会触发相应的窗口的执行。由于watermark是由数据携带的，因此如果运行过程中无法获取新的数据，那么没有被触发的窗口将永远不会被触发</font>**

#### 6.2.2 watermark的引入

```
DataStream<ApacheLogEvent> dataStream = inputStream
                .map(line -> {
                    String[] fields = line.split(" ");
                    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss");
                    Long timestamp = simpleDateFormat.parse(fields[3]).getTime();
                    return new ApacheLogEvent(fields[0], fields[1], timestamp, fields[5], fields[6]);
                })
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<ApacheLogEvent>(Time.seconds(3)) {
                    @Override
                    public long extractTimestamp(ApacheLogEvent element) {
                        return element.getTimestamp();
                    }
                });
```

上面的设置的延迟时长为3s

#### 6.3 EventTimeWindow API

​		当使用EventTimeWindow时，所有window在EventTime的时间轴上进行划分，也就是说，在window启动后，会根据初始的EventTime时间每隔一段时间划分一个窗口，如果window大小是3秒，那么一分钟内会把window划分为如下：

```
[00:00:00,00:00:03) 
[00:00:03,00:00:06) 
... 
[00:00:57,00:01:00)
```

​		如果Window大小是10秒，则Window会被分为如下的形式：

```
[00:00:00,00:00:10) 
[00:00:10,00:00:20)
... 
[00:00:50,00:01:00)
```

​		**<font color='red'>注意，窗口是左闭右开的，形式为[window_start_time,window_end_time)</font>**

​		**<font color='red'>window的设定无关数据本身，而是系统设定好的，也就是说，window会一直按照指定的时间间隔进行划分，不论这个window中有没有数据，EventTime在这个Window期间的数据会进入这个Window</font>**

​		Window会不断产生，属于这个Window范围的数据会被不断加入到Window中，所有未被触发的Window都会等待触发，只要Window还没触发，属于这个Window范围的数据就会一直被加入到Window中，直到Window被触发才会停止数据的追加，而当Window触发之后才接受到的属于被触发Window的数据会被丢弃。 

​		Window会在以下的条件满足时被触发执行： 

- **<font color='red'>watermark时间 >= window_end_time</font>；**
- **<font color='red'>在[window_start_time,window_end_time)中有数据存在。</font>**

我们通过下图来说明Watermark、EventTime和Window的关系。

![watermark关系图](https://github.com/catxjd/flink_study/tree/master/Flink/images/watermark关系图.png)

#### 6.3.1 滚动窗口（TumblingEventTimeWindows）

```
  SingleOutputStreamOperator<PageViewCount> windowAggStream = dataStream
                .filter(data -> "GET".equals(data.getMethod()))    // 过滤get请求
                .filter(data -> {
                    String regex = "^((?!\\.(css|js|png|ico)$).)*$";
                    return Pattern.matches(regex, data.getUrl());
                })
                .keyBy(ApacheLogEvent::getUrl)    // 按照url分组
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
//                .timeWindow(Time.minutes(10), Time.seconds(5))
                .allowedLateness(Time.minutes(1))
                .sideOutputLateData(lateTag)
                .aggregate(new PageCountAgg(), new PageCountResult());
```

**<font color='red'>结果是按照Event Time的时间窗口计算得出的，而无关系统的时间（包括输入的快慢）。</font>**

#### 6.3.2 滑动窗口（SlidingEventTimeWindows）

```
SingleOutputStreamOperator<PageViewCount> windowAggStream = dataStream
                .filter(data -> "GET".equals(data.getMethod()))    // 过滤get请求
                .filter(data -> {
                    String regex = "^((?!\\.(css|js|png|ico)$).)*$";
                    return Pattern.matches(regex, data.getUrl());
                })
                .keyBy(ApacheLogEvent::getUrl)    // 按照url分组
//                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .window(SlidingEventTimeWindows.of(Time.minutes(1),Time.seconds(5)))
//                .timeWindow(Time.minutes(10), Time.seconds(5))
                .allowedLateness(Time.minutes(1))
                .sideOutputLateData(lateTag)
                .aggregate(new PageCountAgg(), new PageCountResult());
```

#### 6.3.3 会话窗口（EventTimeSessionWindows） 

​		**<font color='red'>相邻两次数据的EventTime的时间差超过指定的时间间隔就会触发执行。</font>**如果加入Watermark，那么当触发执行时，所有满足时间间隔而还没有触发的Window会同时触发执行。

```
 SingleOutputStreamOperator<PageViewCount> windowAggStream = dataStream
                .filter(data -> "GET".equals(data.getMethod()))    // 过滤get请求
                .filter(data -> {
                    String regex = "^((?!\\.(css|js|png|ico)$).)*$";
                    return Pattern.matches(regex, data.getUrl());
                })
                .keyBy(ApacheLogEvent::getUrl)    // 按照url分组
//                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
//                .window(SlidingEventTimeWindows.of(Time.minutes(1),Time.seconds(5)))
                .window(EventTimeSessionWindows.withGap(Time.seconds(5)))
//                .timeWindow(Time.minutes(10), Time.seconds(5))
                .allowedLateness(Time.minutes(1))
                .sideOutputLateData(lateTag)
                .aggregate(new PageCountAgg(), new PageCountResult());
```

 

