#!/bin/bash -ue
# Heuristically lists the csv files along with one line (hopefully the header)
# I used it on trinity, like this:
#
# ./list_csvs.sh /user/kite > hadoop_cmd_files.txt
#
# The contents of hadoop_cmd_files.txt should be something like this (filename, followed by header
# in the next line):
#
# user/kite/7509d3a3006e4987c857574b0994aed2.gojek_gopayp2p_ao260717.csv.gz
# "data_date","created_at","id","wallet_id","description","payment_type","multiplication","amount"
# /user/kite/a.csv
# a,b
# /user/kite/b.csv
# a,b
# /user/kite/cast.csv
# $ Claw,OnCreativity (2012)
# ...

ROOT=$1

header_to_skip() {
    for v in "total" "Found"; do
        if [ "$v" == "$1" ]; then
            return 0;
        fi
    done
    return 1
}

directory_to_skip() {
    for v in "operations" "entities" "partitioned" "scalars" "scalars.json" "tables"  '.staging' '.sparkStaging' '.Trash' 'exports' ; do
        if [ "$v" == "$1" ]; then
            return 0;
        fi
    done
    return 1
    
}

LSCOMMAND="hadoop fs -ls"
#LSCOMMAND="ls -l"

process_one_file() {
    if $(echo $1 | grep -q csv)  ; then
        echo "Processing: $1" 1>&2
        echo $1
        if (echo $1 | grep -q "[.]gz$"); then
            hadoop fs -cat $1 | gunzip | tr '\r' '\n' | head -n1
        else
            hadoop fs -cat $1          | tr '\r' '\n' | head -n1
        fi
    fi
}


process_dir() {
    $LSCOMMAND $1 | while read -r line; do
        if  header_to_skip "${line:0:5}" ; then
            continue;
        fi
        FILE=`echo "$line" | awk '{print $NF}'`
        Q=`basename $FILE`
        if [ ${line:0:1} == "d" ]; then
            if ! directory_to_skip $Q  ; then
                process_dir "$FILE"
                else
                echo "Skipping: $FILE" 1>&2
            fi
        else
            process_one_file "$FILE"
        fi
    done

}

process_dir $ROOT
