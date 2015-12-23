cat run.out | perl -ne "print if /^\|/; print if /^$/" |mmd > out.htm
