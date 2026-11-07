SELECT substr(transaction_date,1,7) AS month,
                 COUNT(*) AS transactions, SUM(quantity) AS units_sold,
                 ROUND(SUM(net_sales),2) AS net_sales
                 FROM raw_sales GROUP BY 1 ORDER BY 1;
