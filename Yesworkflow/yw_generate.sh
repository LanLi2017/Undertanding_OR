#!/usr/bin/env bash
alias yw='java -jar ~/yesworkflow-0.2.2.0-SNAPSHOT-jar-with-dependencies.jar'

cat ../yw_out_04_03_00_57/yw.txt | yw graph -c extract.comment='#' > ../gv/yw.gv

dot -Tpng ../gv/yw.gv -o ../png/yw.png


