import spark
from pyspark.shell import spark
from pyspark.sql import SparkSession


from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType, IntegerType

employee_payroll_report_schema = StructType([
    StructField("employee", StringType(), True),
    StructField("employee_id ", StringType(), True),
    StructField("date", StringType(), True),
    StructField("in_time", StringType(), True),
    StructField("out_time", StringType(), True),
    StructField("hours", TimestampType(), True),
    StructField("regular", TimestampType(), True),
    StructField("total_hours_decimal", DoubleType(), True),
    StructField("overtime_1", TimestampType(), True),
    StructField("total_overtime_1", DoubleType(), True),
    StructField("overtime_2", TimestampType(), True),
    StructField("total_overtime_2", DoubleType(), True),
    StructField("daily_total", TimestampType(), True),
    StructField("daily_total_decimal", DoubleType(), True),
    StructField("day_of_week", StringType(), True),
    StructField("regular_rate", DoubleType(), True),
    StructField("overtime_1_rate", IntegerType(), True),
    StructField("overtime_2_rate", IntegerType(), True),
    StructField("daily_rate", DoubleType(), True),
    StructField("regular_total", TimestampType(), True),
    StructField("regular_total_decimal", DoubleType(), True),
    StructField("regular_rate_2", DoubleType(), True),
    StructField("overtime_1_total", TimestampType(), True),
    StructField("overtime_1_total_2", IntegerType(), True),
    StructField("overtime_1_total_3", IntegerType(), True),
    StructField("overtime_2_total", TimestampType(), True),
    StructField("overtime_2_total_2", IntegerType(), True),
    StructField("overtime_2_total_3", IntegerType(), True),
    StructField("overall_total", TimestampType(), True),
    StructField("overall_total_decimal", DoubleType(), True),
    StructField("overall_total_regular", DoubleType(), True),
])
labor_drive_payroll_report_schema = StructType([
    StructField("employee", StringType(), True),
    StructField("employee_id", StringType(), True),
    StructField("day", StringType(), True),
    StructField("date", StringType(), True),
    StructField("job_number", IntegerType(), True),
    StructField("customer_name", StringType(), True),
    StructField("services_provided", StringType(), True),
    StructField("po_ref_number", IntegerType(), True),
    StructField("quickbooks_class", StringType(), True),
    StructField("labor_start", StringType(), True),
    StructField("labor_end", StringType(), True),
    StructField("labor_time_11", TimestampType(), True),
    StructField("drive_time_12", TimestampType(), True),
    StructField("labor_plus_drive", TimestampType(), True),
    StructField("job_created_at", StringType(), True),
    StructField("log_entries", IntegerType(), True),
    StructField("drive_time_16", TimestampType(), True),
    StructField("labor_time_17", TimestampType(), True),
    StructField("total_labor_plus_drive", TimestampType(), True),
    StructField("job_category", StringType(), True),
])


tech_day_sheet_report_schema = StructType([
    StructField("name", StringType(), True),
    StructField("job_est_date", StringType(), True),
    StructField("job_est_time", StringType(), True),
    StructField("status", StringType(), True),
    StructField("job_est_number", StringType(), True),
    StructField("po_number", StringType(), True),
    StructField("customer", StringType(), True),
    StructField("service_location", StringType(), True),
    StructField("job_est_description", StringType(), True),
    StructField("primary_contact", StringType(), True),
    StructField("notes_for_techs", StringType(), True),
    StructField("job_est_created_at", StringType(), True),
    StructField("jobs_estimates", StringType(), True),
])

schema = StructType([
    StructField("bluecollar_solutions_fka_the_works_service_company", StringType(), True),
    StructField("_c1", StringType(), True),
    StructField("_c2", StringType(), True),
    StructField("_c3", StringType(), True),
])

expaned_tech_day_sheet_report_schema = StructType([
    StructField("Name", StringType(), True),
    StructField("Job/Est Date", StringType(), True),
    StructField("Job/Est Time", StringType(), True),
    StructField("Status", StringType(), True),
    StructField("Job/Est #", StringType(), True),
    StructField("PO #", StringType(), True),
    StructField("Customer", StringType(), True),
    StructField("Service Location", StringType(), True),
    StructField("Job/Est Description", StringType(), True),
    StructField("Primary Contact", StringType(), True),
    StructField("Notes For Techs", StringType(), True),
    StructField("Completion Notes", StringType(), True),
    StructField("Job Created At", StringType(), True),
    StructField("Total Jobs/Estimates", StringType(), True),
])
product_service_sales_report_schema = StructType([
    StructField("Service Tech", StringType(), True),
    StructField("Product or Service", StringType(), True),
    StructField("Job#", StringType(), True),
    StructField("Date", StringType(), True),
    StructField("Customer", StringType(), True),
    StructField("Qty", StringType(), True),
    StructField("Rate", StringType(), True),
    StructField("Tax Amount($)", StringType(), True),
    StructField("Tax Name", StringType(), True),
    StructField("Job Created At", StringType(), True),
    StructField("Job Category", StringType(), True),
    StructField("Product or Service Category", StringType(), True)
])


general_revenue_report_schema = StructType([
    StructField("Job#", StringType(), True),
    StructField("Date", StringType(), True),
    StructField("Time", StringType(), True),
    StructField("Status", StringType(), True),
    StructField("PO/Ref#", StringType(), True),
    StructField("Job Category", StringType(), True),
    StructField("Job Source", StringType(), True),
    StructField("Customer", StringType(), True),
    StructField("Parent Customer", StringType(), True),
    StructField("Service Location Address 1", StringType(), True),
    StructField("Service Location Address 2", StringType(), True),
    StructField("Service Location City", StringType(), True),
    StructField("Service Location State", StringType(), True),
    StructField("Service Location Zip", StringType(), True),
    StructField("Products", StringType(), True),
    StructField("Services", StringType(), True),
    StructField("Labor", StringType(), True),
    StructField("Expenses (B)", StringType(), True),
    StructField("Total", StringType(), True),
    StructField("Payments/Deposits", StringType(), True),
    StructField("Total Due", StringType(), True),
    StructField("Job Details", StringType(), True),
    StructField("Tech(s) Assigned", StringType(), True),
    StructField("Completion Notes", StringType(), True),
    StructField("Labor Time", StringType(), True),
    StructField("Drive Time", StringType(), True),
    StructField("Job Charges", StringType(), True),
    StructField("Qty", StringType(), True),
    StructField("Rate", StringType(), True),
    StructField("Total", StringType(), True),
    StructField("Cost", StringType(), True),
    StructField("Job Subtotal", StringType(), True),
    StructField("Total Time & Labor", StringType(), True),
    StructField("Total Billable Expenses", StringType(), True),
    StructField("Job Total", StringType(), True),
    StructField("Parts Cost", StringType(), True),
    StructField("Service Cost", StringType(), True),
    StructField("Drive Time Cost", StringType(), True),
    StructField("Labor Time Cost", StringType(), True),
    StructField("Job Created At", StringType(), True)
])
#employee_payroll_report_df = spark.read.csv("/Users/ayanbhatt/Downloads/EmployeePayrollReport__ (2).xlsx - Worksheet.csv", , inferSchema=True)
#labor_drive_payroll_report_df = spark.read.csv("/Users/ayanbhatt/Downloads/LaborDrivePayrollReport_08_03_2025_08_04_2025.xlsx - Worksheet.csv", header=True, inferSchema=True)
#tech_day_sheet_report_df = (spark.read.option("header", "true").option("multiLine", "true").option("quote", "\"").option("escape", "\"").option("mode", "PERMISSIVE").csv("/Users/ayanbhatt/Downloads/TechDaySheetReport_08_03_2025_08_04_2025.xlsx - Worksheet.csv"))
#df = spark.read.csv("/Users/ayanbhatt/Downloads/report.xlsx - Worksheet.csv", header=True, inferSchema=True)
#df = (spark.read.option("header", "true").option("multiLine", "true").option("quote", "\"").option("escape", "\"").option("mode", "PERMISSIVE").schema(expaned_tech_day_sheet_report_schema).csv("/Users/ayanbhatt/Downloads/ExpanedTechDaySheetReport_08_03_2025_08_04_2025.xlsx - Worksheet.csv"))
#df = spark.read.csv("/Users/ayanbhatt/Downloads/ProductServiceSalesReport_08_03_2025_08_04_2025 (1).xlsx - Worksheet.csv", header=True, schema=product_service_sales_report_schema)
#df = spark.read.csv("/Users/ayanbhatt/Downloads/ProductServiceSalesReport_08_03_2025_08_04_2025.xlsx - Worksheet.csv", header=True, inferSchema=True)
#df = (spark.read.option("header", "true").option("multiLine", "true").option("quote", "\"").option("escape", "\"").option("mode", "PERMISSIVE").csv("/Users/ayanbhatt/Downloads/Report_Invoice (2).xlsx - Worksheet.csv"))
df = (spark.read.option("header", "true").option("multiLine", "true").option("quote", "\"").option("escape", "\"").option("mode", "PERMISSIVE").schema(general_revenue_report_schema).csv("/Users/ayanbhatt/Downloads/GeneralRevenueReport_08_03_2025_08_05_2025.xlsx - Worksheet.csv"))



df.show(100,truncate=False)
df.printSchema()
df.count()
