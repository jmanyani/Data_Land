# Banking Transactions Dashboard (SQL + Power BI)

## Project Overview

This project analyzes banking transaction data using **SQL for data preparation** and **Power BI for visualization**.
The objective is to clean raw banking data, combine multiple tables, and build an interactive dashboard that provides insights into customer activity, account balances, and transaction patterns.

The project demonstrates a **typical end-to-end data analytics workflow**:

* Data generation and preparation using SQL
* Data cleaning and transformation
* Loading data into Power BI
* Creating DAX measures
* Building dashboards with multiple visualizations
* Publishing the report to Power BI Service

---

# Tools & Technologies

* SQL
* Power BI Desktop
* DAX (Data Analysis Expressions)
* Perplexity AI (for recommendations on KPIs and visualizations)

---

# Project Workflow

# Data Preparation (SQL)

Steps performed before loading into Power BI:

1. Created banking dataset using SQL.
2. Removed inconsistencies from the **Date column**.
3. Combined data from **three tables using SQL JOINs**.
4. Loaded cleaned data into **Power BI Desktop**.

---

# Data Cleaning

The following transformations were applied:

* Cleaned inconsistent date values
* Standardized date formats
* Cleaned additional date columns
* Verified data types
* Prepared the dataset for reporting

---

# Dashboard Visualizations

![Image](https://github.com/user-attachments/assets/fed6e423-f988-49d2-871c-559f6e009c66)

![Image](https://github.com/user-attachments/assets/7ea4d98a-0aa4-410b-9909-83cb76c6d2fb)

The Power BI dashboard includes the following visual insights:

* **Number of Transactions by Type**
* **Transactions by Month**
* **Top 2 Transactions by Name**
* **Total Balance by Account Type**
* **Inactive Accounts by Year & Month**
* **Customer Count by Gender**
* **Number of Customers by Age Group**
* **Tree Map for Account Distribution**

---

# DAX Measures Used

### Account Count by Type

```
Account Count by Type = 
COUNT(CombinedBankingDataset[Account_AccountID])
```

### Count of Transactions

```
Count of Transactions = 
COUNT(CombinedBankingDataset[TransactionID])
```

### Customer Count by Gender

```
Customer Count by Gender = 
DISTINCTCOUNT(CombinedBankingDataset[CustomerID])
```

### Inactive Accounts

Accounts with no transactions in the last **90 days**.

```
Inactive Accounts =
COUNTROWS(
    FILTER(
        VALUES(CombinedBankingDataset[Account_AccountID]),
        CALCULATE(MAX(CombinedBankingDataset[TransactionDate])) < TODAY() - 90
    )
)
```

### Monthly Transaction Amount

```
Monthly Transaction Amount =
CALCULATE(
    SUM(CombinedBankingDataset[Amount]),
    ALLEXCEPT(
        CombinedBankingDataset,
        CombinedBankingDataset[TransactionDate].[Month]
    )
)
```

### Monthly Transaction Balance

```
Monthly Transaction Balance =
CALCULATE(
    SUM(CombinedBankingDataset[Balance]),
    ALLEXCEPT(
        CombinedBankingDataset,
        CombinedBankingDataset[TransactionDate].[Month]
    )
)
```

### Total Balance by Account Type

```
Total Balance By Account Type =
SUM(CombinedBankingDataset[Balance])
```

---

# Key Insights Generated

The dashboard helps identify:

* Monthly transaction trends
* Customer distribution by gender and age group
* High-value transaction patterns
* Account balance distribution by account type
* Accounts that have been inactive for over 90 days

---

# Learning Outcomes

Through this project, the following skills were developed:

* SQL data cleaning and joins
* Data modeling in Power BI
* Creating advanced DAX measures
* Designing business dashboards
* Publishing reports to Power BI Service

---

# Future Improvements

Potential enhancements include:

* Adding **customer segmentation analysis**
* Building **profitability analysis per customer**
* Creating **drill-through reports**
* Integrating **real-time banking data**

---

# Author

Jatin Manyani


---
