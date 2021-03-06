# Spring Cloud Task Triggered Apache Spark Word Count Using Apache Camel

A demonstration of short lived micro-services which counts number of words. It uses Apache Camel routing mechanism to
integrate with Spark. This brings:
1. Power of DI using Spring Boot task, hence programing ease
2. A simple message oriented integration with Apache Spark henece loose coupling
3. Observability ese using Spring Admin, Consul etc etc
```
 The complete integration can be deployed to Spark cluster as Uber Jar
```

## Code Walk Through

### Apache Camel Configuration

```java
 // JavaRDD example with callback send via producer
 String sparkUri1 = "spark:rdd?rdd=#myRdd";
 producer.sendBodyAndHeader(sparkUri1, null, SPARK_RDD_CALLBACK_HEADER, new SerializableVoidRddCallback());

 // JavaRDD example with callback as a part of uri
 String sparkUri2 = "spark:rdd?rdd=#myRdd&rddCallback=#count";
 String output = producer.requestBody(sparkUri2, null, String.class);
  
 // DataFrame example with callback send via producer
 String sparkDataFrameUri = "spark:dataFrame?dataFrame=#fileDataFrame";
  output = producer.requestBodyAndHeader(sparkDataFrameUri, null, SPARK_DATAFRAME_CALLBACK_HEADER, new SerializableDataFrameCallback(), String.class);

```

### Spring Configuration
#### JavaRDD
```java
    
    @Bean
    public JavaSparkContext javaSparkContext() {
        SparkConf conf = new SparkConf().setAppName("wordcount").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        return sc;
    }
 ```
 
```java
     
     @Bean
     JavaRDDLike<String, JavaRDD<String>> myRdd(JavaSparkContext sparkContext, @Value("classpath:testrdd.txt") Resource resource) throws IOException {
         return sparkContext.textFile(resource.getURI().getPath());
     }  

```

#### DataSet aka DataFrame
```java
    
    @Bean
    public SparkSession sparkSession() {
        return SparkSession
                .builder().appName("wordcount").master("local[*]").getOrCreate();
    }
```

```java
     
    @Bean
    Dataset<String> fileDataFrame(SparkSession sparkSession, @Value("classpath:testrdd.txt") Resource resource) throws IOException {
        return sparkSession.read().textFile(resource.getURI().getPath());
    }  

```