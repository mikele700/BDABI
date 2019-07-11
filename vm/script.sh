#!/bin/sh
#mongoimport --db BDABI --collection yelpbusiness --file /home/vmadmin/yelp/business.json
#mongoimport --db BDABI --collection yelpreview --file /home/vmadmin/yelp/review.json
#mongoimport --db BDABI --collection yelpuser --file /home/vmadmin/yelp/user.json
#mongoimport --db BDABI --collection yelptip --file /home/vmadmin/yelp/tip.json
#mongoimport --db BDABI --collection yelpcheckin --file /home/vmadmin/yelp/checkin.json
#mongoimport --db BDABI --collection yelpphoto --file /home/vmadmin/yelp/photo.json

#sudo transmission-cli /home/vmadmin/stackexchange_archive.torrent -w /mnt
#sudo aria2c -T /home/vmadmin/stackexchange_archive.torrent -d /mnt

#for file in /mnt/stackexchange/*.7z
#       do
#       f=$(basename "$file")
#       me=${f#*.}
#       m=${me%%.*}
#
#       topic=${f%%.*}
#       if [ "$m" != "meta" ] && [ "$topic" != "meta" ] && [ "$topic" != "stackoverflow" ];
#               then
#               sudo 7z x "$file" -o"/mnt/data/$topic"
#               sudo chmod -R go+rw /mnt/data/$topic
#               sudo chmod go+x /mnt/data/$topic
#       fi
#
        #7z t $file
#
#done
maschera="falso"
for file in /mnt/data/*
        do
        f="$(basename "$file")"
        letter="$(echo $f | head -c 3)"
        if [ "$letter" = "mat" ];
                then
                maschera="vero"
        fi

        if [ "$maschera" = "vero" ];
                then
                spark-submit --packages com.databricks:spark-xml_2.12:0.5.0,org.mongodb.spark:mongo-spark-connector_2.12:2.4.0 /home/vmadmin/src/StackExchange.py $f
        fi
done


