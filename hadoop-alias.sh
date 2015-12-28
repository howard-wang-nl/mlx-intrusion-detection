##############################################################################
# Hadoop HDFS specific alias
# Adapted from:
#   https://github.com/sudar/dotfiles/blob/master/bash/hdfs_aliases
#   https://github.com/wklken/linux_config/blob/master/aliases/hadoop_aliases
##############################################################################

alias hfs='hadoop fs'
alias hcat='hadoop fs -cat'
alias hchgrp='hadoop fs -chgrp'
alias hchmod='hadoop fs -chmod'
alias hchown='hadoop fs -chown'
alias hcf='hadoop fs -copyFromLocal'
alias hpush='hadoop fs -copyFromLocal'
alias hct='hadoop fs -copyToLocal'
alias hpull='hadoop fs -copyToLocal'
alias hcount='hadoop fs -count'
alias hcp='hadoop fs -cp'
alias hdu='hadoop fs -du'
alias hdus='hadoop fs -dus'
alias hexpunge='hadoop fs -expunge'
alias hget='hadoop fs -get'
alias hgetmerge='hadoop fs -getmerge'
alias hhelp='hadoop fs -help'
alias hkill='hadoop job -kill'
alias hls='hadoop fs -ls'
alias hlsr='hadoop fs -ls -R'
alias hmd='hadoop fs -mkdir'
alias hmf='hadoop fs -moveFromLocal'
alias hmt='hadoop fs -moveToLocal'
alias hmv='hadoop fs -mv'
alias hput='hadoop fs -put'
alias hrm='hadoop fs -rm'
alias hrmr='hadoop fs -rmr'
alias hsetrep='hadoop fs -setrep'
alias hqueue='hadoop queue -showacls'
alias hstat='hadoop fs -stat'
alias htail='hadoop fs -tail'
alias htest='hadoop fs -test'
alias htext='hadoop fs -text'
alias htouchz='hadoop fs -touchz'

# concatenate all part files in given directory
hpartcat () { hadoop fs -cat $1/part-* ; }

# concatenate all part files in given directory, piped to less
hpartless () { hadoop fs -cat $1/part-* | less ; }

# get total size of files in given directory, print human readable
# human readable code http://bit.ly/O5AWU
hduh () { hadoop fs -ls $1 | awk '{tot+=$5} END {print tot}' | \
   awk '{sum=$1;
        hum[1024**4]="Tb";hum[1024**3]="Gb";hum[1024**2]="Mb";hum[1024]="Kb";
        for (x=1024**3; x>=1024; x/=1024){
                if (sum>=x) { printf "%.2f %s\n",sum/x,hum[x];break }
   }}' ; }

