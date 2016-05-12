#/bin/bash
source ~/.bashrc
spark-submit2 data_etl.py
spark-submit2 basic_stat.py