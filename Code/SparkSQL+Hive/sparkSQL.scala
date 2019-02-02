/*

Command to run this code:

spark-shell -i '/home/cloudera/Desktop/BigDataTechnologiesProject/sparkSQL.scala'

*/



//Creating table socialmediaUsingHive

println("Creating table socialmediaUsingHive")

sqlContext.sql("CREATE TABLE socialmediaUsingHive (Key STRING, date STRING, socialmedia STRING, text STRING, analized STRING, quantity STRING) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' WITH SERDEPROPERTIES('separatorChar' = '\054','quoteChar' = '\042') STORED AS TEXTFILE")




//Loading data from CSV file

println("Loading data from CSV file")

sqlContext.sql("LOAD DATA LOCAL INPATH '/home/cloudera/Desktop/BigDataTechnologiesProject/TwitterStreaming.csv' OVERWRITE INTO TABLE socialmediaUsingHive")

//1. Show first 20 rows
println("1. Show first 30 rows")
sqlContext.sql("SELECT * FROM socialmediaUsingHive LIMIT 30").show()

/*
+--------------------+--------------------+-----------+--------------------+--------+--------+
|                 key|                date|socialmedia|                text|analized|quantity|
+--------------------+--------------------+-----------+--------------------+--------+--------+
|0005ebdb16e44aa5a...|Sat Dec 15 17:47:...|  #facebook|Apparently I am i...|negative|       1|
|0012862994914873a...|Sat Dec 15 18:16:...|  #linkedin|RT BREAKINGNEWS W...|positive|       1|
|014048820bf94af78...|Sat Dec 15 18:20:...|  #linkedin|22 Top tips to ef...|positive|       1|
|07e10b51617f476fb...|Sat Dec 15 18:14:...|  #linkedin|RT Many services ...|positive|       1|
|0a014ad5edb248bd9...|Sat Dec 15 18:16:...|  #facebook|RT Facebook sorry...|negative|       1|
|0d3fbd33fe2043afb...|Sat Dec 15 18:14:...|  #linkedin|RT BREAKINGNEWS W...|positive|       1|
|0f20ee268dc14e7aa...|Sat Dec 15 18:14:...|   #youtube|Come check out ou...| neutral|       1|
|140bef5008c547459...|Sat Dec 15 17:47:...|   #youtube|Any decent PS4 Bl...|positive|       1|
|165328a63e3248968...|Sat Dec 15 17:47:...|   #twitter|https t co ItsB2T...|negative|       1|
|171bf31fa81849528...|Sat Dec 15 18:24:...|  #facebook|MultiMedia lyrici...| neutral|       1|
|1fc189aa6cc447dba...|Sat Dec 15 17:47:...|  #linkedin|RT Connecting all...|positive|       1|
|2192ec90a7f540e38...|Sat Dec 15 18:14:...|  #facebook|facebook official...| neutral|       1|
|2293771b4be14983a...|Sat Dec 15 18:14:...|  #facebook|RT Chic Shoes htt...| neutral|       1|
|24b3a6d5d5054c909...|Sat Dec 15 18:16:...|  #facebook|Another day It s ...| neutral|       1|
|255d3287af04483f9...|Sat Dec 15 18:20:...|  #facebook|Shower thoughts S...| neutral|       1|
|26f2d82586a342f88...|Sat Dec 15 18:16:...|   #youtube|RT New Stardew Va...|negative|       1|
|281eb8dde47245a8a...|Sat Dec 15 17:47:...|   #youtube|YouTube MMD MikuM...| neutral|       1|
|2ad1f8e1d6024135b...|Sat Dec 15 18:14:...|  #facebook|MultiMedia lyrici...| neutral|       1|
|2b1b8ea1763344cba...|Sat Dec 15 18:20:...|#instragram|RT A new sense of...|positive|       1|
|2bda65d0a9af4d86a...|Sat Dec 15 18:16:...|  #facebook|RT Una nueva fall...| neutral|       1|
+--------------------+--------------------+-----------+--------------------+--------+--------+
*/

//2. Count tweets grouped by socialmedia
println("2. Count tweets grouped by socialmedia")
sqlContext.sql("SELECT socialmedia, COUNT(quantity) AS tweetsCount FROM socialmediaUsingHive GROUP BY socialmedia ORDER BY tweetsCount DESC").show()

/*
+-----------+-----------+                                                       
|socialmedia|tweetsCount|
+-----------+-----------+
|   #youtube|         24|
|  #linkedin|         23|
|  #facebook|         23|
|   #twitter|         19|
|#instragram|         10|
+-----------+-----------+
*/

//3. Count tweets grouped by analized
println("3. Count tweets grouped by analized")
sqlContext.sql("SELECT analized, COUNT(quantity) AS tweetsCount FROM socialmediaUsingHive GROUP BY analized ORDER BY tweetsCount DESC").show() 

/*
+-----------+-----------+                                                       
|socialmedia|tweetsCount|
+-----------+-----------+
|   #youtube|         24|
|  #linkedin|         23|
|  #facebook|         23|
|   #twitter|         19|
|#instragram|         10|
+-----------+-----------+

*/

//4. Count tweets grouped by socialmedia and analized
println("4. Count tweets grouped by socialmedia and analized")
sqlContext.sql("SELECT socialmedia,analized, COUNT(quantity) AS tweetsCount FROM socialmediaUsingHive GROUP BY socialmedia,analized ORDER BY socialmedia,analized, tweetsCount DESC").show()

/*
+-----------+--------+-----------+                                              
|socialmedia|analized|tweetsCount|
+-----------+--------+-----------+
|  #facebook|positive|          3|
|  #facebook| neutral|         17|
|  #facebook|negative|          3|
|#instragram|positive|         10|
|  #linkedin|positive|         17|
|  #linkedin| neutral|          6|
|   #twitter|positive|          2|
|   #twitter| neutral|         16|
|   #twitter|negative|          1|
|   #youtube|positive|          5|
|   #youtube| neutral|         18|
|   #youtube|negative|          1|
+-----------+--------+-----------+

*/

//5. Count tweets grouped by socialmedia and analized
println("Count tweets grouped by socialmedia and analized")
sqlContext.sql("SELECT socialmedia, analized, count(*)  FROM socialmediaUsingHive GROUP BY socialmedia,analized HAVING count(*) > 4").show()

/*
+-----------+--------+---+
|socialmedia|analized|_c2|
+-----------+--------+---+
|  #linkedin| neutral|  6|
|  #facebook| neutral| 17|
|#instragram|positive| 10|
|   #youtube|positive|  5|
|  #linkedin|positive| 17|
|  #facebook|positive|  3|
|   #youtube| neutral| 18|
|   #twitter| neutral| 16|
|  #facebook|negative|  3|
+-----------+--------+---+
*/

//Quitting spark-shell
//System.exit(0)

