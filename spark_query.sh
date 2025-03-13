#!/bin/sh
source /etc/profile
source $HOME/.bash_profile

#获取参数
while [ $# -ne 0 ]
do
  while getopts :b:e:f:o:-: optname
  do
    case $optname in
    -)
        case $OPTARG in 
            debug)
                debug="true"
                ;;
            nomsg)
                nomsg="true"
                ;;
        esac
        ;;

    b)
      btime=$OPTARG #开始日期
      ;;
    e)
      etime=$OPTARG #结束日期
      ;;
    f)
      sqlfile=$OPTARG #SQL模板文件
      ;;
    o)
      outfile=$OPTARG #输出结果文件
      ;;
    esac
  done
  shift
done

# 参数异常判断
if [ -f "$sqlfile" ]
then
    echo -n
else
    echo "Usage: spark_query -f SQLFile -b StartDate -e EndDate -o OutputFile"
    echo "Example: spark_query -f sample.sql -b 20260203 -e 20260203 -o outfile.tab"
    exit 1
fi

if [ "x"$btime == "x" ]
then 
    btime=`date -d "1 day ago" +%Y%m%d`
fi

START_DATE=`date --date="$btime" +%Y%m%d`
if [ $START_DATE != $btime ]
then
    echo "Error Date: $btime"
    echo "Example: $0 -f sample.sql -b 20060203 -e 20060203"
    exit 1
fi

# 若结束日期为空 则以开始日期为结束日期
if [ "x"$etime == "x" ]
then
    etime=$btime
fi

END_DATE=`date --date="$etime" +%Y%m%d`

# SQL文件名
sqlprefix=$(echo $sqlfile | awk -F. '{for(i=1;i<NF;i++) s=s==""?$i:s"."$i; print s}')
execsqlfile=$sqlprefix"_"$btime"-"$etime".sql"

# 结果文件名
if [ "x"$outfile == "x" ]
then
    outfile=$sqlprefix"_"$btime"-"$etime".csv"
fi

#=================#
#  开始执行程序   #
#=================#


echo
echo "Start of $sqlfile ..."`date "+%Y-%m-%d %X"`

# 检查删除中间文件 结果文件
if [ -f $execsqlfile ] 
then
    rm -f $execsqlfile
fi

if [ -f $outfile ]
then
    rm -f $outfile
fi

# 循环构造SQL文件
CUR_DATE=$START_DATE
while (($CUR_DATE <= $END_DATE))
do
    echo "${CUR_DATE} begin at:" `date "+%Y-%m-%d %X"`
    CUR_DATE_FORMAT=`date --date="$CUR_DATE 0 days ago" +%Y%m%d`

    cat $sqlfile |grep -v '^--' | sed "s/\${bizdate}/${CUR_DATE_FORMAT}/g" >> $execsqlfile

    CUR_DATE=`date --date="$CUR_DATE 1 days" +%Y%m%d`

done

# 执行HIVE查询，输出到结果文件
# hive -f $execsqlfile | sed 's/\t/","/g' | sed 's/^/"/g'|sed 's/$/"/g' | iconv -f utf8 -t gbk >> $outfile
# hive -f $execsqlfile | grep -v 'WARN: ' > $outfile 

sql=$(cat $execsqlfile)
save_path=/user/pengjy/ehome/sql_result/$execsqlfile

baseDirForScriptSelf=$(cd "$(dirname "$0")"; pwd)
JAR_PATH=$baseDirForScriptSelf
JAR_FILE=$JAR_PATH/Ana.jar
if [ -f "$JAR_FILE" ];then
 echo "jar_file:$JAR_FILE"
 else
  echo "not exists!!!jar_file:$JAR_FILE"
  exit 1
  fi
echo $JAR_FILE
  spark-submit --master yarn --deploy-mode client --queue stat   \
  --name ehome_tmptask_pengjy_stat \
  --conf spark.shuffle.service.enabled=false \
  --conf spark.dynamicAllocation.enabled=false \
  --driver-memory 2g --executor-memory 4g --num-executors 80  --executor-cores 1 \
  --conf spark.sql.shuffle.partitions=2000 \
  --conf spark.yarn.executor.memoryOverhead=1024MB \
  --class com._21cn.data.tykj.tmp.SqlRun   ${JAR_FILE}  "${sql}"  "${save_path}"

hadoop fs -text $save_path/* > $outfile


if [ "x"$debug != "xtrue" ]
then
    rm -f $execsqlfile
fi

echo "End of $sqlfile. "`date "+%Y-%m-%d %X"`


#if [ "x"$nomsg != "xtrue" ]
#then
#    content="spark_query -f $sqlfile -b $btime -e $etime -o $outfile"
#    contentj="{\"msgtype\": \"text\", \"text\": {\"content\": \"数据跑完啦! $content\"}}"
#
#    curl "https://oapi.dingtalk.com/robot/send?access_token=xxx" \
#       -H "Content-Type: application/json" \
#       -d "$contentj"
#
