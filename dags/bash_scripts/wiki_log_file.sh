YEAR=$(date +"%Y")
MONTH=$(date +"%m")
DAY=$(date +"%d")

# Subtracting hours from the current time
HOUR=$(date -d "1 hour ago" +"%H")

# outputing the selected hour
echo $HOUR

# downloading pageviews file for the set date and time
curl -o /opt/airflow/dags/pageviews-${YEAR}${MONTH}${DAY}-${HOUR}0000.gz https://dumps.wikimedia.org/other/pageviews/${YEAR}/${YEAR}-${MONTH}/pageviews-${YEAR}${MONTH}${DAY}-${HOUR}0000.gz

#unzipping file
gunzip /opt/airflow/dags/pageviews-${YEAR}${MONTH}${DAY}-${HOUR}0000.gz


# creating table header
echo "domain_name,page_title,view_count,size,date,hour" > /opt/airflow/dags/output.csv

# grabbing  Microsoft,Apple,Facebook,Google related lines and attaching date and hour 
grep -E "Microsoft_|Apple_|Facebook|Google" /opt/airflow/dags/pageviews-${YEAR}${MONTH}${DAY}-${HOUR}0000 | \
    awk -v year="${YEAR}" -v month="${MONTH}" -v day="${DAY}" -v hour="${HOUR}" \
    '{print $1 ", " $2 ", " $3 ", " $4 ", " year "/" month "/" day ", " hour}' >> /opt/airflow/dags/output.csv

# counting rows in the file excluding header
tail -n +2 /opt/airflow/dags/output.csv | wc -l

# removing the pageviews file downloaded
rm -f opt/airflow/dags/pageviews-${YEAR}${MONTH}${DAY}-${HOUR}0000.gz
# rm -f /opt/airflow/dags/pageviews-${YEAR}${MONTH}${DAY}-${HOUR}0000