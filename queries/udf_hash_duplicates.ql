add JAR /usr/hdp/3.0.1.0-187/libs/hive-common-3.1.0.jar;
add JAR /usr/hdp/3.0.1.0-187/libs/hive-exec-3.1.0.jar;
add JAR /usr/hdp/3.0.1.0-187/libs/avatica-1.11.0.jar;
add JAR /usr/hdp/3.0.1.0-187/libs/hive-serde-3.1.0.jar;
add JAR /usr/hdp/3.0.1.0-187/libs/bill_udf.jar;
create temporary function hashed_bill AS 'com.chirkunov.hive.udf.bill_hash.HashedBillUDF';
select hashed_bill(order_id, bill_raw_text), count(*) as cnt from sales group by hashed_bill(order_id, bill_raw_text) having count(*)>1;