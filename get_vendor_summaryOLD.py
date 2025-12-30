# SCRIPT TO GET VENDOR SALES SUMMARY FROM THE DATABASE, PERFORM ANALYSIS, AND STORE BACK INTO THE DATABASE

import os
import logging

# Ensure logs directory exists
os.makedirs("logs", exist_ok=True)

# Configure logging FIRST
logging.basicConfig(
    filename="logs/get_vendor_summary.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a",
    force=True
)

import sqlite3
import pandas as pd
from ingestion_db import ingest_db


#Function  to merge the different tables to get the overall vendor sales summary and adding new columns in the resultant data
def create_vendor_sales_summary(conn):
    vendor_sales_summary = pd.read_sql_query(""" WITH FreightSummary AS (
        SELECT
            VendorNumber, 
            SUM(Freight) AS Freight_Cost 
        FROM vendor_invoice 
        GROUP BY VendorNumber
    ),
 
    PurchaseSummary AS (
        SELECT 
            p.VendorNumber, 
            p.VendorName,
            p.Brand,
            p.Description,
            p.PurchasePrice,
            pp.Volume,
            pp.Price as ActualPrice,
            SUM(p.Quantity) as TotalPurchaseQuantity,
            SUM(p.Dollars) as TotalPurchaseDollars
        FROM purchases p
        JOIN purchase_prices pp
            ON p.Brand = pp.Brand
        WHERE p.PurchasePrice > 0    
        GROUP BY p.VendorNumber, p.VendorName, p.Brand, p.Description, p.PurchasePrice, pp.Volume, pp.Price
    ),
                                                    
    SalesSummary AS (
        SELECT 
            VendorNo, 
            Brand,
            SUM(SalesDollars) as TotalSalesDollars,
            SUM(SalesPrice) as TotalSalesPrice,
            SUM(SalesQuantity) as TotalSalesQuantity,
            SUM(ExciseTax) as TotalExciseTax
        FROM sales   
        GROUP BY VendorNo, Brand
    ),

    SELECT                                             
        ps.VendorNumber,
        ps.VendorName,
        ps.Brand,
        ps.Description,
        ps.ActualPrice,
        ps.PurchasePrice,
        ps.Volume,
        ps.TotalPurchaseQuantity,
        ps.TotalPurchaseDollars,
        ss.TotalSalesQuantity,
        ss.TotalSalesDollars,
        ss.TotalSalesPrice,
        ss.TotalExciseTax,
        fs.FreightCost    

    FROM PurchaseSummary ps
    LEFT JOIN SalesSummary ss 
        ON ps.VendorNumber = ss.VendorNo 
        AND ps.Brand = ss.Brand
    LEFT JOIN FreightSummary fs
        ON ps.VendorNumber = fs.VendorNumber
    ORDER BY ps.TotalPurchaseDollars DESC""",conn)

    return vendor_sales_summary

#Function to clean data and remove inconsistencies
def clean_data(df):
    
    df['Volume'] = df['Volume'].astype('float') #Convert Volume datatype to float
    df.fillna(0, inplace=True) #Fill missing values with 0
    df['VendorName'] = df['VendorName'].str.strip() #Remove leading/trailing spaces from columns
    df['Description'] = df['Description'].str.strip()

    #creating derived metrics columns for better analysis

    # global vendor_sales_summary 
    # #need to define it as a global variable to be used in this function or outside , otherwise it gives erroR of undefined variable ----- else use df as above lines

    # vendor_sales_summary['GrossProfit'] = vendor_sales_summary['TotalSalesDollars'] - vendor_sales_summary['TotalPurchaseDollars']
    # vendor_sales_summary['ProfitMargin'] = (vendor_sales_summary['GrossProfit'] / vendor_sales_summary['TotalSalesDollars']) * 100
    # vendor_sales_summary['StockTurnover'] = vendor_sales_summary['TotalSalesQuantity'] / vendor_sales_summary['TotalPurchaseQuantity']
    # vendor_sales_summary['SalestoPurchaseRatio'] = vendor_sales_summary['TotalSalesDollars'] / vendor_sales_summary['TotalPurchaseDollars']

    df['GrossProfit'] = df['TotalSalesDollars'] - df['TotalPurchaseDollars']
    df['ProfitMargin'] = (df['GrossProfit'] / df['TotalSalesDollars']) * 100
    df['StockTurnover'] = df['TotalSalesQuantity'] / df['TotalPurchaseQuantity']
    df['SalestoPurchaseRatio'] = df['TotalSalesDollars'] / df['TotalPurchaseDollars']


    return df

if __name__ == "__main__":
    # Creating connection to the SQLite database
    conn = sqlite3.connect("inventory.db")
    
    logging.info("Creatinng Vendor Summary Table ......")
    summary_df = create_vendor_sales_summary(conn)
    logging.info(summary_df.head()) 

    # Cleaning the data
    logging.info("Cleaning data and adding derived metrics ......") 
    clean_df = clean_data(summary_df)
    logging.info(clean_df.head())

    # Ingest the summary back into the database
    ingest_db(clean_df, "vendor_sales_summary", conn)
    logging.info("Vendor sales summary ingested back into the database successfully.")

sample_df = pd.read_sql_query(
    "SELECT * FROM vendor_sales_summary LIMIT 5;",
    conn
)
print(sample_df)
conn.close()

