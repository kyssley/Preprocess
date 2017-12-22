#!/bin/sh
source $HOME/.bash_profile
source /etc/profile


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
    echo "Usage: hive_query -f SQLFile -b StartDate -e EndDate -o OutputFile"
    echo "Example: hive_query -f sample.sql -b 20060203 -e 20060203 -o outfile.tab"
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
    outfile=$sqlprefix"_"$btime"-"$etime".tsv"
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
    CUR_DATE_FORMAT=`date --date="$CUR_DATE 0 days ago" +%Y-%m-%d`

    cat $sqlfile |grep -v '^--' | sed "s/{date}/${CUR_DATE_FORMAT}/g" >> $execsqlfile

    CUR_DATE=`date --date="$CUR_DATE 1 days" +%Y%m%d`

done

# 执行HIVE查询，输出到结果文件
# hive -f $execsqlfile | sed 's/\t/","/g' | sed 's/^/"/g'|sed 's/$/"/g' | iconv -f utf8 -t gbk >> $outfile
hive -f $execsqlfile | grep -v 'WARN: ' > $outfile 

if [ "x"$debug != "xtrue" ]
then
    rm -f $execsqlfile
fi

echo "End of $sqlfile. "`date "+%Y-%m-%d %X"`


if [ "x"$nomsg != "xtrue" ]
then
    content="hivequery -f $sqlfile -b $btime -e $etime -o $outfile"
    contentj="{\"msgtype\": \"text\", \"text\": {\"content\": \"数据跑完啦! $content\"}}"

    curl "https://oapi.dingtalk.com/robot/send?access_token=xxx" \
       -H "Content-Type: application/json" \
       -d "$contentj"
fi

echo
