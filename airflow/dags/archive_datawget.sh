
YEAR=$1
PATH_TO_STORE=$2
URL_PREFIX="https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports"

DATA_PATH="${PATH_TO_STORE}/${YEAR}"
mkdir -pv ${DATA_PATH}

for MONTH in {1..12}; do
  FMONTH=`printf "%02d" ${MONTH}`
  for DAY in {1..31}; do
    FDAY=`printf "%02d" ${DAY}`
    if [[ "${YEAR}-${FMONTH}-${FDAY}" > "$(date +%Y-%m-%d -d "2 day ago")" ]]; then
      echo "Archive data is up to date"
      break 2
    fi
      DATASET_FILE="${FMONTH}-${FDAY}-${YEAR}.csv"
      URL="${URL_PREFIX}/${DATASET_FILE}"
      wget ${URL} -qP ${DATA_PATH}
      gzip ${DATA_PATH}/${DATASET_FILE}
      echo "downloaded and compressed: ${DATA_PATH}/${DATASET_FILE}"
  done
done  




