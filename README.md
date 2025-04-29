# 🧹 Layoffs Dataset – Data Cleaning with MySQL

This project performs a complete data cleaning process using MySQL on a dataset of global layoffs available on [Kaggle - layoffs](https://www.kaggle.com/datasets/swaptr/layoffs-2022). The goal is to transform the raw dataset into a clean, standardized, and analytics-ready format that can later be used for further data exploration, reporting, and visualization.

---

## 📌 Project Overview

The original dataset contains records of tech company layoffs from around the world. However, the raw data includes duplicates, inconsistent formats, and null values.

This project focuses on:

Creating a staging table to preserve the raw dataset

Removing duplicate records

Standardizing categorical and numerical data

Fixing incorrect or inconsistent entries (e.g., dates, strings, formatting)

Creating a clean analytics table for further analysis

---

## 🎯 Project Goals

🔍 Identify and remove duplicate records

✏️ Standardize text fields like industry and country names

🔢 Convert and clean numeric fields like total_laid_off and percentage_laid_off

📆 Clean and convert date values into proper MySQL DATE format

📊 Create an analytics table with appropriate data types and indexes

---

## 🛠 Tools and Technologies

- **MySQL**
- **MySQL Workbench / any SQL client (Dbeaver was used for this project)**
- **CSV File Source: Layoffs Dataset**
- **SQL for data transformation**
- **Git for version control**