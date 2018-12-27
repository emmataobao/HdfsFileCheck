功能介绍：

参数格式：
--delete /sources/file1,/sources/file2,......欲删除路径之间用逗号隔开
--wait_exist /sources/file1,/sources/file2,......线程堵塞，欲等待文件路径之间用逗号隔开,所检查文件不到位时每30秒查一次
--wait_count /sources/file1,/sources/file2,......线程堵塞，判断文件夹下文件个数，N为期望的个数
--count /sources/file1 N,/sources/file2 N,......判断文件夹下文件个数，N为期望的个数
--write /sources/file1 text,/sources/file2 text,......向HDFS文件中写入内容。
--toEmail surongquan@moxiu.net,......报警邮件接收人地址，用逗号隔开，不配置将不发送邮件