#!/bin/bash

# List of Glue Crawlers
CRAWLERS=(
  "CalculateAvgDailyReturnCrawler"
  "FindHighestWorthStockCrawler"
  "FindMostVolatileStockCrawler"
  "FindTop30DayReturnDatesCrawler"
)

# Function to start a crawler and wait for its completion
run_crawler() {
  local crawler_name=$1

  echo "Starting crawler: $crawler_name"
  aws glue start-crawler --name "$crawler_name"

  # Wait until the crawler completes
  while true; do
    state=$(aws glue get-crawler --name "$crawler_name" --query 'Crawler.State' --output text)
    if [ "$state" == "READY" ]; then
      echo "Crawler $crawler_name has completed."
      break
    elif [ "$state" == "RUNNING" ]; then
      echo "Crawler $crawler_name is still running..."
      sleep 10 # Wait for 10 seconds before checking again
    else
      echo "Unexpected state for crawler $crawler_name: $state"
      exit 1
    fi
  done
}

# Iterate over all crawlers and run them sequentially
for crawler in "${CRAWLERS[@]}"; do
  run_crawler "$crawler" &
done

wait
echo "All crawlers have completed successfully!"
