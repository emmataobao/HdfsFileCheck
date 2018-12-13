#!/bin/sh
binHome=/home/jp-spark/sparkApp/theme_recommenderV2

#执行【主题信息清洗】、【运营数据清洗[pair、train、topic、single]】、【用户行为日志数据清洗["Download" or "Scan"]】
nohup sh $binHome/bin/operate_data_clean.sh all
wait
#检查内容：/cleandata/themeRecommendV2/themeInfo/_SUCCESS、/cleandata/themeRecommendV2/themeInfo/_SUCCESS、/cleandata/themeRecommendV2/scanAndDownLog/$yesterday/_SUCCESS
#/cleandata/themeRecommendV2/userUrlPage/$yesterday/_SUCCESS、检通过后写入/status/compass/recommend/new_recommend_system_check.point $yesterdaydate
log=`sh /home/jp-spark/sparkApp/hdfsFileCheckTool/bin/output_themeClearCheck.sh`
echo "------文件依赖检查结果如下-----"
echo $log

# 主题标签质量回馈平台：主题信息解析插入HBse,参数1：20个主题一个页，20个index一个页
nohup sh $binHome/bin/operate_tag.sh 20 /cleandata/themeRecommendV2/themeInfo > $binHome/log/operate_tag.log &