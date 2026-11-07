-- 1. Jumlah baris
SELECT COUNT(*) AS total_rows FROM sales_lab.sales_transactions;

-- 2. Ringkasan penjualan
SELECT COUNT(*) AS transactions,
       SUM(quantity) AS units_sold,
       ROUND(SUM(net_sales), 2) AS total_net_sales,
       ROUND(AVG(net_sales), 2) AS avg_transaction_value
FROM sales_lab.sales_transactions;

-- 3. Penjualan per bulan
SELECT DATE_TRUNC('month', transaction_date)::date AS month,
       COUNT(*) AS transactions,
       ROUND(SUM(net_sales), 2) AS net_sales
FROM sales_lab.sales_transactions
GROUP BY 1
ORDER BY 1;

-- 4. Penjualan per cabang
SELECT branch, COUNT(*) AS transactions,
       ROUND(SUM(net_sales), 2) AS net_sales
FROM sales_lab.sales_transactions
GROUP BY branch ORDER BY net_sales DESC;

-- 5. Penjualan per kategori
SELECT category, ROUND(SUM(net_sales), 2) AS net_sales
FROM sales_lab.sales_transactions
GROUP BY category ORDER BY net_sales DESC;

-- 6. Validasi formula (toleransi 0,02 karena pembulatan)
SELECT COUNT(*) AS formula_mismatch
FROM sales_lab.sales_transactions
WHERE ABS(net_sales - ROUND(quantity * unit_price * (1-discount_rate), 2)) > 0.02;

-- 7. Cek duplikat ID
SELECT transaction_id, COUNT(*)
FROM sales_lab.sales_transactions
GROUP BY transaction_id
HAVING COUNT(*) > 1;
