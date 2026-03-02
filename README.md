# Modular SQL-Inspired ETL Pipeline (File-Based Data)



## 📌 Executive Overview

This project implements a **modular ETL pipeline in Python** to process structured file-based data (CSV, Excel, Parquet).

The goal is to replicate common SQL aggregation and transformation logic in a file-based environment using **Pandas**, while maintaining a clean separation of responsibilities between components.

> ⚠️ This solution is not intended to replace SQL databases.  
> It demonstrates how SQL-style business rules can be applied when working with external data files in a controlled and reusable architecture.

---

## 🏗️ Architecture

This separation allows reuse of transformation logic across different datasets.

<img width="989" height="682" alt="image" src="https://github.com/user-attachments/assets/a86cbe3f-2b73-4876-9e40-9a39011f1c30" />

## 💼 Business Use Case

### Input Files

#### Rental Data File
- Marketplace
- Customer Site ID
- Product
- Monthly Rental Amount

#### Product Mapping File
Maps products into segment groups:
- Seg 1–3
- Seg 4–6

---

## 📏 Business Rules Applied

- Include only rentals ≥ **$25 per month**
- Count how many units each customer rents
- Classify customers into:
  - 1–2 units
  - 3–5 units
  - 6+ units

---

<img width="989" height="682" alt="image" src="https://github.com/user-attachments/assets/bde8a611-2523-4a10-a399-62ffe6956807" />


## 💼 Result of the process
<img width="438" height="706" alt="image" src="https://github.com/user-attachments/assets/5f0efbb1-b852-4f59-8262-ca7e00e91e73" />


## 🧠 Skills Demonstrated

- Modular ETL design
- Separation of concerns
- Reusable transformation abstraction
- SQL-to-Pandas logic translation
- Grouping, aggregation, and segmentation logic
- Versioned output handling
- Python (Pandas, NumPy)
- SQL concepts (joins, CASE logic, aggregation)
- GitHub workflow

---

## 📊 Results

The pipeline:
- Processes raw input files
- Applies business rules
- Produces aggregated outputs ready for reporting

The architecture allows the same transformation logic to be reused with different datasets.

---

## 🚀 Next Steps

Planned improvements:

- Configuration-based transformation rules
- Logging and validation controls
- Workflow orchestration
- Separate visualization module using Matplotlib

> The visualization layer will remain outside the ETL scope to maintain architectural separation.

Separate visualization module using Matplotlib

