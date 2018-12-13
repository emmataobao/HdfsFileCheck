#!/bin/sh
# 此脚本在/home/jp-spark/sparkApp/theme_recommenderV2/bin/themeRecommendV2CleanAbout.sh中被调用检查
source /etc/profile
objHome=/home/jp-spark/sparkApp/hdfsFileCheckTool

yesterday=`date -d "1 days ago" +%Y/%m/%d`
yesterdaydate="`date -d "-1 day" +%Y-%m-%d`"

export HADOOP_USER_NAME=compass
themeinfoPath=/cleandata/themeRecommendV2/themeInfo/_SUCCESS
operaterPath=/cleandata/themeRecommendV2/operationTheme/_SUCCESS
userlog=/cleandata/themeRecommendV2/scanAndDownLog/$yesterday/_SUCCESS
userpag=/cleandata/themeRecommendV2/userUrlPage/$yesterday/_SUCCESS

java -cp $objHome/lib/HdfsFileCheck-0.0.1-SNAPSHOT.jar com.surq.hdfs.check.InputCheck \
--toEmail surongquan@moxiu.net,yangfengmei@moxiu.net,gaozhiqiang@moxiu.net \
--waits $themeinfoPath,$operaterPath,$userlog,$userpag \
--write /status/compass/recommend/new_recommend_system_check.point $yesterdaydate