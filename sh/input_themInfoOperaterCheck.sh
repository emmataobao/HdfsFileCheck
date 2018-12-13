#!/bin/sh
# 此脚本在/home/jp-spark/sparkApp/theme_recommenderV2/bin/operate_data_clean.sh中被调用检查
source /etc/profile
objHome=/home/jp-spark/sparkApp/hdfsFileCheckTool

date=`date "+%Y-%m-%d"`
dt=`date +%Y/%m/%d`
export HADOOP_USER_NAME=cleandata
themeinfoPath="/sources/web/mongodb/theme/themeinfo/cbs_themes_${date}.done"
operaterparPath="/sources/web/mongodb/theme/operation/pair/Content_Pair_${date}.done"
operatersinglePath="/sources/web/mongodb/theme/operation/single/Content_Single_${date}.done"
operatertopicPath="/sources/web/mongodb/theme/operation/topic/Content_Topic_${date}.done"
operatertrainsPath="/sources/web/mongodb/theme/operation/trains/Content_Trains_${date}.done"
themeusrLogPath=/sources/web/nginx/contents/$dt
java -cp $objHome/lib/HdfsFileCheck-0.0.1-SNAPSHOT.jar com.surq.hdfs.check.InputCheck \
--toEmail surongquan@moxiu.net,yangfengmei@moxiu.net,gaozhiqiang@moxiu.net \
--waits $themeinfoPath,$operaterparPath,$operatersinglePath,$operatertopicPath,$operatertrainsPath \
--count $themeusrLogPath 7