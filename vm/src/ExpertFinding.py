# encoding=utf8
import sys
reload(sys)
sys.setdefaultencoding('utf8')
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, length, min, max, sum
import sys

#Creazione della sessione Spark con relativa configurazione (wrapper di SparkContext)
spark = SparkSession.builder.appName("ExpertFinding").config("spark.mongodb.input.uri", "mongodb://127.0.0.1/BDABI.yelpbusiness").getOrCreate()

#Valutazione degli argomenti di input:
#       - nessun argomento di input --> Topic non selezionato
#       - almeno un argomento di input --> Creazione della stringa corrispondente al topic da cercare in MongoDB
if len(sys.argv)>1:
  topicstack = ""
  for i in range(1,len(sys.argv)):
    line = sys.argv[i].capitalize()     #argomento in input tutto minuscolo con prima lettera maiuscola
    linestack = sys.argv[i].lower()
    if i==1:
      topic = line
    else:
      topic = topic+" "+line            #spazio tra più parole dello stesso topic
    topicstack = topicstack+linestack
  print ("Topic selezionato: %s") %topic

  collectionpost = "stackexchange"+topicstack+"post"
  collectionuser = "stackexchange"+topicstack+"user"
  notyelp = False
  notstack = False

#Normalizzazione in (0,1): (x-min)/(max-min)
  def normalizza(df,cl):
    minimo = df.agg(min(cl)).collect()[0][0]
    massimo = df.agg(max(cl)).collect()[0][0]
    return((df[cl]-minimo)/(massimo-minimo))


#Lettura dalla collezione yelpbusiness di MongoDB
  business = spark.read.format("com.mongodb.spark.sql.DefaultSource").load()

#Ricerca in business dei locali relativi al topic selezionato
  category = business.where(col('categories').like(topic+",%") | col('categories').like("% "+topic+",%") | col('categories').like("% "+topic) | col('categories').like(topic)).select("business_id")

#Lettura della collezione post di stackexchange
  post = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("uri","mongodb://127.0.0.1/BDABI."+collectionpost).load()

  if category.count()>0:
    print("YELP")
#Lettura dalla collezione yelpreview di MongoDB
    review = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("uri","mongodb://127.0.0.1/BDABI.yelpreview").load()
#Selezione delle colonne di interesse di review
    reviewsel = review.select('business_id', 'user_id', 'useful')
#Join per la ricerca delle recensioni appartenenti ai locali precedentemente trovati
    reviewtopic = category.join(reviewsel,category.business_id==reviewsel.business_id, "inner").select('user_id', 'useful')
#Filtraggio delle recensioni con utilità inferiore alla media ed ordinamento discendente
    reviewfilt = reviewtopic.filter(reviewtopic['useful'] > reviewtopic.agg(avg(col("useful"))).collect()[0][0]).orderBy('useful', ascending=False)
    if reviewfilt.count()>100:
      reviewfilt = reviewfilt.limit(100)        #riduzione a 100 righe con maggiore useful

#Lettura della collezione yelpuser di MongoDB
    user = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("uri","mongodb://127.0.0.1/BDABI.yelpuser").load()
    usersel = user.select('user_id','name','useful','fans','elite','compliment_hot','compliment_profile','compliment_list').withColumnRenamed("useful","useruseful")

#Join recensioni-utenti, calcolo della dimensione di elite, selezione colonne di interesse e somma di useful delle recensioni afferenti allo stesso utente
#La dimensione di elite è la lunghezza della stringa
    reviewer = reviewfilt.join(usersel, reviewfilt.user_id==usersel.user_id, "inner").withColumn('elitedim',length('elite')).groupBy('name','useruseful','fans','elitedim','compliment_hot','compliment_profile','compliment_list',usersel.user_id).agg({'useful':'sum'}).withColumnRenamed("sum(useful)","topicuseful")

#Calcolo score di ogni utente
#Gli attributi contribuiscono con peso differente al calcolo dello score: 5% list, 10% fans, useruseful e profile, 15% hot, 25% elite e topicuseful
    userscore = reviewer.withColumn('score', (0.25*normalizza(reviewer,'topicuseful'))+(0.1*normalizza(reviewer,'fans'))+(0.25*normalizza(reviewer,'elitedim'))+(0.15*normalizza(reviewer,'compliment_hot'))+(0.1*normalizza(reviewer,'compliment_profile'))+(0.05*normalizza(reviewer,'compliment_list'))+(0.1*normalizza(reviewer,'useruseful'))).select('user_id','name','score').orderBy('score',ascending=False)
    userscore.show(10)
  else:
    notyelp = True

  if post.count()>0:
    print("STACK EXCHANGE")
    postsel = post.filter(post['_PostTypeId']=="2").select("_postId","_Score","_OwnerUserId").filter(post['_Score'] > post.agg(avg(col("_Score"))).collect()[0][0]).orderBy('_Score', ascending=False)

    if postsel.count()>100:
      postsel = postsel.limit(100)       #riduzione a 100 righe con maggiore score

#Lettura della collezione user di stackexchange
    user = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("uri","mongodb://127.0.0.1/BDABI."+collectionuser).load()
    usersel = user.select('_userId','_DisplayName','_Location','_AboutMe','_Reputation')

#Join recensioni-utenti, selezione colonne di interesse e somma di score dei post afferenti allo stesso utente
    expert = postsel.join(usersel, postsel['_OwnerUserId']==usersel['_userId'], "inner").groupBy('_userId','_DisplayName','_Location','_AboutMe','_Reputation').agg({'_Score':'sum'}).withColumnRenamed("sum(_Score)","topicscore")

#Calcolo dello score di ogni utente
    expertscore = expert.withColumn('scoreexpertise', (0.5*normalizza(expert,'topicscore'))+(0.5*normalizza(expert,'_Reputation'))).orderBy('scoreexpertise',ascending=False)
    expertscore.show(10)

  else:
    notstack = True

  if (notyelp and notstack):
    print("Topic inesistente")

else:
  print("Topic non selezionato")

