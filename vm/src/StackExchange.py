#coding=utf-8
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import sys

#Funzione con parametro di ingresso: sys.argv[1] = topic

database = "BDABI"
collectionpost = "stackexchange"+sys.argv[1]+"post"
collectionuser = "stackexchange"+sys.argv[1]+"user"

#Configurazione SparkSession e connessione con MongoDB
spark = SparkSession.builder.appName("stackExchange").config("spark.mongodb.input.uri", "mongodb://127.0.0.1/"+database+"."+collectionpost).config("spark.mongodb.output.uri","mongodb://127.0.0.1/"+database+"."+collectionpost).getOrCreate()

#Schemi dei file XML
postSchema = StructType([StructField("_Id", StringType(), True), StructField("_PostTypeId", StringType(), True), StructField("_ParentId", StringType(), True), StructField("_AcceptedAnswerId", StringType(), True), StructField("_CreationDate", StringType(), True), StructField("_Score", IntegerType(), True), StructField("_ViewCount", IntegerType(), True), StructField("_Body", StringType(),True), StructField("_OwnerUserId", StringType(), True), StructField("_LastEditorUserId", StringType(), True), StructField("_LastEditorDisplayName", StringType(), True), StructField("_LastEditDate", StringType(), True), StructField("_LastActivityDate", StringType(), True), StructField("_CommunityOwedDate", StringType(), True), StructField("_ClosedDate", StringType(), True), StructField("_Title", StringType(), True), StructField("_Tags", StringType(), True), StructField("_AnswerCount", IntegerType(), True), StructField("_CommentCount", IntegerType(), True), StructField("_FavoriteCount", IntegerType(), True), StructField("_OwnerDisplayName", StringType(), True)])

userSchema = StructType([StructField("_Id", StringType(), True), StructField("_Reputation", IntegerType(), True), StructField("_CreationDate", StringType(), True), StructField("_DisplayName", StringType(), True), StructField("_AccountId", StringType(), True), StructField("_LastAccessDate", StringType(),True), StructField("_WebSiteUrl", StringType(), True), StructField("_Location", StringType(), True), StructField("_ProfileImageUrl", StringType(), True), StructField("_AboutMe", StringType(), True), StructField("_Views", IntegerType(), True), StructField("_UpVotes", IntegerType(), True), StructField("_DownVotes", IntegerType(), True), StructField("_Age", IntegerType(), True), StructField("EmailHash", StringType(), True)])

#Creazione DataFrame
post = spark.read.format('xml').options(rowTag='row').load('/mnt/data/'+sys.argv[1]+'/Posts.xml',schema=postSchema).withColumnRenamed("_Id","_postId")
user = spark.read.format('xml').options(rowTag='row').load('/mnt/data/'+sys.argv[1]+'/Users.xml',schema=userSchema).withColumnRenamed("_Id","_userId")

#Scrittura DataFrame in MongoDB
post.write.format("com.mongodb.spark.sql.DefaultSource").mode("append").save()
user.write.format("com.mongodb.spark.sql.DefaultSource").mode("append").option("database",database).option("collection", collectionuser).save()
