# **Data Engineering Assignment**

## **Introduction**
Thank you for the opportunity to work on this assignment! It was both informative and enjoyable. Below, I’ve detailed the design choices, assumptions, and implementations made during this task.

---

## **Assumptions and Design Decisions**

### **1. Scalability with Apache Spark**
- I chose **Apache Spark** for the ETL process to ensure scalability, as the task mentioned handling potentially large datasets in the future.

### **2. Output Format: Parquet**
- Output files are saved in **Parquet** format instead of CSV because:
  - **Efficient storage**: Parquet compresses data better than CSV.
  - **Query performance**: Optimized for Athena and allows scanning only relevant columns.
  - **Schema support**: Parquet includes embedded schema information, aiding Glue crawlers.

### **3. Splitting Crawlers**
- Separate crawlers were created for each dataset output to:
  - Allow **granular updates** (update only the relevant dataset’s metadata).
  - Save **time and cost** by running crawlers selectively.

### **4. Scheduling**
- **ETL job schedule**: Runs daily at **23:30** to process the input data and generate outputs.
- **Crawler schedule**: Runs daily at **00:00** to update the Glue Catalog with the latest data.
- This ensures all datasets are updated and ready for querying in **Athena** every day.

### **5. Automation with Bash Script**
- A **Bash script** is provided to run all crawlers sequentially. This simplifies maintenance and ensures proper execution.

---

## **Implementation Details**

### **Infrastructure**
- **AWS Glue**: Used for data processing and metadata management.
- **AWS S3**: Storage for input files, scripts, and outputs.
- **AWS Athena**: Query engine for analyzing processed data.

### **Output Directory Structure**
- Outputs are stored in organized S3 directories:
  - `/output/calculate_avg_daily_return/`
  - `/output/find_highest_worth_stock/`
  - `/output/find_most_volatile_stock/`
  - `/output/find_top_30_day_return_dates/`

### **ETL Process**
- The Glue job performs the following tasks:
  1. Calculate average daily returns.
  2. Find the stock with the highest average worth.
  3. Identify the most volatile stock.
  4. Find the top 3 dates with the highest 30-day returns.
- Results are saved as Parquet files in the output directories.

---

## **Conclusion**
This solution is designed to be **scalable**, **cost-efficient**, and **maintainable**. The daily schedules and optimized processes ensure data is always up to date and ready for analysis.

Thank you once again for the opportunity. Feel free to reach out with any questions or feedback!
