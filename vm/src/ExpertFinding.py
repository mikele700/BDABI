#coding=utf-8
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, length, min, max, sum
import sys

#Creazione della sessione Spark con relativa configurazione (wrapper di SparkContext)
spark = SparkSession.builder.appName("ExpertFinding").config("spark.mongodb.input.uri", "mongodb://127.0.0.1/BDABI.yelpbusiness").getOrCreate()

#Valutazione degli argomenti di input:
#       - nessun argomento di input --> Topic non selezionato
#       - almeno un argomento di input --> Creazione della stringa corrispondente al topic da cercare in MongoDB
if len(sys.argv)>1:

  for i in range(1,len(sys.argv)):
    line = sys.argv[i].capitalize()     #argomento in input tutto minuscolo con prima lettera maiuscola
    if i==1:
      topic = line
    else:
      topic = topic+" "+line            #spazio tra più parole dello stesso topic
  print ("Topic selezionato: %s") %topic

#Lettura dalla collezione yelpbusiness di MongoDB
  business = spark.read.format("com.mongodb.spark.sql.DefaultSource").load()
  business.createOrReplaceTempView("temp")
#Ricerca in business dei locali relativi al topic selezionato
  category = spark.sql("SELECT business_id FROM temp WHERE categories LIKE '%"+topic+"%'")

#Lettura dalla collezione yelpreview di MongoDB
  review = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("uri","mongodb://127.0.0.1/BDABI.yelpreview").load()
#Selezione delle colonne di interesse di review
  reviewsel = review.select('business_id', 'user_id', 'useful')
#Join per la ricerca delle recensioni appartenenti ai locali precedentemente trovati
  reviewtopic = category.join(reviewsel,category.business_id==reviewsel.business_id, "inner").select('user_id', 'useful')
#Filtraggio delle recensioni con utilità inferiore alla media ed ordinamento discendente
  reviewfilt = reviewtopic.filter(reviewtopic['useful'] > reviewtopic.agg(avg(col("useful"))).collect()[0][0]).orderBy('useful', ascending=False)
  if reviewfilt.count()>100:
    reviewfilt = reviewfilt.limit(100)  #riduzione a 100 righe con maggiore useful

#Lettura della collezione yelpuser di MongoDB
  user = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("uri","mongodb://127.0.0.1/BDABI.yelpuser").load()
  usersel = user.select('user_id','name','useful','fans','elite','compliment_hot','compliment_profile','compliment_list').withColumnRenamed("useful","useruseful")

#Join recensioni-utenti, calcolo della dimensione di elite, selezione colonne di interesse e somma di useful delle recensioni afferenti allo stesso utente
#La dimensione di elite è la lunghezza della stringa
  reviewer = reviewfilt.join(usersel, reviewfilt.user_id==usersel.user_id, "inner").withColumn('elitedim',length('elite')).groupBy('name','useruseful','fans','elitedim','compliment_hot','compliment_profile','compliment_list',usersel.user_id).agg({'useful':'sum'}).withColumnRenamed("sum(useful)","topicuseful")

#Calcolo score di ogni utente
#Normalizzazione in (0,1): (x-min)/(max-min)
  def normalizza(df,cl):
    minimo = df.agg(min(cl)).collect()[0][0]
    massimo = df.agg(max(cl)).collect()[0][0]
    return((df[cl]-minimo)/(massimo-minimo))

#Gli attributi contribuiscono con peso differente al calcolo dello score: 5% list, 10% fans, useruseful e profile, 15% hot, 25% elite e reviewuseful
  userscore = reviewer.withColumn('score', (0.25*normalizza(reviewer,'topicuseful'))+(0.1*normalizza(reviewer,'fans'))+(0.25*normalizza(reviewer,'elitedim'))+(0.15*normalizza(reviewer,'compliment_hot'))+(0.1*normalizza(reviewer,'compliment_profile'))+(0.05*normalizza(reviewer,'compliment_list'))+(0.1*normalizza(reviewer,'useruseful'))).select('name','score').orderBy('score',ascending=False)
  userscore.show(10)


else:
  print("Topic non selezionato")
