import os
from embed import prepare_data, tokenizer
from train import optimize_hyperparameters, cross_validation
import json
import datetime
from transformers import DataCollatorForSeq2Seq

# Complex evaluation data with CTEs
eval_data = [
    {
        "sql": """
WITH recursive_cte AS (
    SELECT employee_id, manager_id, first_name, last_name, 1 AS level
    FROM employees
    WHERE manager_id IS NULL
    UNION ALL
    SELECT e.employee_id, e.manager_id, e.first_name, e.last_name, rc.level + 1
    FROM employees e
    JOIN recursive_cte rc ON e.manager_id = rc.employee_id
)
SELECT r.employee_id, r.first_name, r.last_name, r.level,
       d.department_name, s.salary
FROM recursive_cte r
JOIN departments d ON r.employee_id = d.manager_id
JOIN salaries s ON r.employee_id = s.employee_id
WHERE r.level <= 3
ORDER BY r.level, s.salary DESC;
        """,
        "lineage": "employees.employee_id -> result, employees.first_name -> result, employees.last_name -> result, employees.manager_id -> recursive_cte, departments.department_name -> result, departments.manager_id -> result, salaries.salary -> result, salaries.employee_id -> result"
    },
    {
        "sql": """
WITH sales_summary AS (
    SELECT 
        p.product_category,
        c.customer_segment,
        SUM(s.quantity * p.price) AS total_sales,
        AVG(s.quantity * p.price) AS avg_sale
    FROM sales s
    JOIN products p ON s.product_id = p.product_id
    JOIN customers c ON s.customer_id = c.customer_id
    WHERE s.sale_date BETWEEN '2023-01-01' AND '2023-12-31'
    GROUP BY p.product_category, c.customer_segment
),
top_categories AS (
    SELECT product_category, SUM(total_sales) AS category_sales
    FROM sales_summary
    GROUP BY product_category
    ORDER BY category_sales DESC
    LIMIT 5
)
SELECT 
    ss.product_category,
    ss.customer_segment,
    ss.total_sales,
    ss.avg_sale,
    (ss.total_sales / tc.category_sales * 100) AS percent_of_category
FROM sales_summary ss
JOIN top_categories tc ON ss.product_category = tc.product_category
ORDER BY tc.category_sales DESC, ss.total_sales DESC;
        """,
        "lineage": "sales.quantity -> sales_summary.total_sales, sales.quantity -> sales_summary.avg_sale, products.price -> sales_summary.total_sales, products.price -> sales_summary.avg_sale, products.product_category -> result, customers.customer_segment -> result, sales_summary.total_sales -> result, sales_summary.avg_sale -> result, sales_summary.total_sales -> top_categories.category_sales, top_categories.category_sales -> result"
    },
    {
        "sql": """
WITH RECURSIVE date_series AS (
    SELECT DATE('2023-01-01') AS date
    UNION ALL
    SELECT date(date, '+1 day')
    FROM date_series
    WHERE date < DATE('2023-12-31')
),
daily_orders AS (
    SELECT 
        ds.date,
        COALESCE(COUNT(o.order_id), 0) AS order_count,
        COALESCE(SUM(o.total_amount), 0) AS daily_revenue
    FROM date_series ds
    LEFT JOIN orders o ON ds.date = DATE(o.order_date)
    GROUP BY ds.date
),
moving_average AS (
    SELECT 
        date,
        order_count,
        daily_revenue,
        AVG(order_count) OVER (ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS avg_weekly_orders,
        AVG(daily_revenue) OVER (ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS avg_weekly_revenue
    FROM daily_orders
)
SELECT 
    strftime('%Y-%m', date) AS month,
    SUM(order_count) AS monthly_orders,
    SUM(daily_revenue) AS monthly_revenue,
    AVG(avg_weekly_orders) AS avg_weekly_orders,
    AVG(avg_weekly_revenue) AS avg_weekly_revenue
FROM moving_average
GROUP BY strftime('%Y-%m', date)
ORDER BY month;
        """,
        "lineage": "orders.order_id -> daily_orders.order_count, orders.total_amount -> daily_orders.daily_revenue, daily_orders.order_count -> moving_average.order_count, daily_orders.daily_revenue -> moving_average.daily_revenue, moving_average.order_count -> result, moving_average.daily_revenue -> result, moving_average.avg_weekly_orders -> result, moving_average.avg_weekly_revenue -> result"
    },
    {
        "sql": """
WITH customer_lifetime_value AS (
    SELECT 
        c.customer_id,
        c.customer_name,
        SUM(o.total_amount) AS total_spent,
        COUNT(DISTINCT o.order_id) AS total_orders,
        MIN(o.order_date) AS first_order_date,
        MAX(o.order_date) AS last_order_date,
        (JULIANDAY(MAX(o.order_date)) - JULIANDAY(MIN(o.order_date))) / 365.25 AS years_active
    FROM customers c
    JOIN orders o ON c.customer_id = o.customer_id
    GROUP BY c.customer_id, c.customer_name
),
customer_segments AS (
    SELECT 
        *,
        CASE 
            WHEN total_spent >= 10000 AND years_active >= 3 THEN 'VIP'
            WHEN total_spent >= 5000 OR (total_spent >= 3000 AND years_active >= 2) THEN 'Gold'
            WHEN total_spent >= 1000 OR (total_spent >= 500 AND years_active >= 1) THEN 'Silver'
            ELSE 'Bronze'
        END AS segment,
        total_spent / NULLIF(years_active, 0) AS annual_value
    FROM customer_lifetime_value
)
SELECT 
    cs.segment,
    COUNT(*) AS customer_count,
    AVG(cs.total_spent) AS avg_total_spent,
    AVG(cs.total_orders) AS avg_total_orders,
    AVG(cs.years_active) AS avg_years_active,
    AVG(cs.annual_value) AS avg_annual_value,
    SUM(cs.total_spent) / SUM(cs.years_active) AS segment_annual_value
FROM customer_segments cs
GROUP BY cs.segment
ORDER BY avg_annual_value DESC;
        """,
        "lineage": "customers.customer_id -> result, customers.customer_name -> customer_lifetime_value, orders.total_amount -> customer_lifetime_value.total_spent, orders.order_id -> customer_lifetime_value.total_orders, orders.order_date -> customer_lifetime_value.first_order_date, orders.order_date -> customer_lifetime_value.last_order_date, customer_lifetime_value.total_spent -> customer_segments.segment, customer_lifetime_value.years_active -> customer_segments.segment, customer_segments.segment -> result, customer_segments.total_spent -> result, customer_segments.total_orders -> result, customer_segments.years_active -> result, customer_segments.annual_value -> result"
    },
    {
        "sql": """
WITH RECURSIVE category_tree AS (
    SELECT category_id, parent_category_id, category_name, 0 AS level
    FROM categories
    WHERE parent_category_id IS NULL
    UNION ALL
    SELECT c.category_id, c.parent_category_id, c.category_name, ct.level + 1
    FROM categories c
    JOIN category_tree ct ON c.parent_category_id = ct.category_id
),
category_sales AS (
    SELECT 
        ct.category_id,
        ct.category_name,
        ct.level,
        SUM(oi.quantity * oi.unit_price) AS total_sales,
        COUNT(DISTINCT o.order_id) AS order_count
    FROM category_tree ct
    LEFT JOIN products p ON ct.category_id = p.category_id
    LEFT JOIN order_items oi ON p.product_id = oi.product_id
    LEFT JOIN orders o ON oi.order_id = o.order_id
    GROUP BY ct.category_id, ct.category_name, ct.level
),
category_hierarchy AS (
    SELECT 
        cs.category_id,
        cs.category_name,
        cs.level,
        cs.total_sales,
        cs.order_count,
        COALESCE(p.category_name, 'Root') AS parent_category
    FROM category_sales cs
    LEFT JOIN category_tree ct ON cs.category_id = ct.category_id
    LEFT JOIN categories p ON ct.parent_category_id = p.category_id
)
SELECT 
    ch.parent_category,
    ch.category_name,
    ch.level,
    ch.total_sales,
    ch.order_count,
    ch.total_sales / NULLIF(ch.order_count, 0) AS avg_order_value,
    RANK() OVER (PARTITION BY ch.parent_category ORDER BY ch.total_sales DESC) AS sales_rank
FROM category_hierarchy ch
ORDER BY ch.level, ch.total_sales DESC;
        """,
        "lineage": "categories.category_id -> category_tree, categories.parent_category_id -> category_tree, categories.category_name -> category_tree, products.category_id -> category_sales, order_items.quantity -> category_sales.total_sales, order_items.unit_price -> category_sales.total_sales, orders.order_id -> category_sales.order_count, category_sales.category_id -> result, category_sales.category_name -> result, category_sales.level -> result, category_sales.total_sales -> result, category_sales.order_count -> result, categories.category_name -> category_hierarchy.parent_category, category_hierarchy.parent_category -> result, category_hierarchy.category_name -> result, category_hierarchy.level -> result, category_hierarchy.total_sales -> result, category_hierarchy.order_count -> result"
    }
]

import os
from embed import prepare_data, tokenizer
from train import optimize_hyperparameters, cross_validation
import json
import datetime
from transformers import DataCollatorForSeq2Seq

# Complex evaluation data with CTEs
eval_data = [
    # ... (keep your existing eval_data here)
]

def process_subfolder(subfolder_path):
    sql_file = os.path.join(subfolder_path, "sql.txt")
    output_file = os.path.join(subfolder_path, "output.txt")
    
    if os.path.exists(sql_file) and os.path.exists(output_file):
        with open(sql_file, 'r') as f:
            sql_statement = f.read().strip()
        with open(output_file, 'r') as f:
            lineage = f.read().strip()
        return sql_statement, lineage
    return None, None

def main():
    input_folder = "Input"  # Change this to your input folder path
    all_datasets = []
    for subfolder in os.listdir(input_folder):
        subfolder_path = os.path.join(input_folder, subfolder)
        if os.path.isdir(subfolder_path):
            print(f"Processing subfolder: {subfolder}")
            sql_statement, lineage = process_subfolder(subfolder_path)
            
            if sql_statement is None or lineage is None:
                print(f"Skipping subfolder {subfolder}: Missing sql.txt or output.txt")
                continue
            
            print("Preparing data...")
            dataset = prepare_data([sql_statement], [lineage])
            all_datasets.extend(dataset)
            print("\nExample of input from this subfolder:")
            print("SQL statement:")
            print(sql_statement)
            print("\nLineage:")
            print(lineage)
            print("\nTokenized input:")
            print(dataset[0])
            print("=" * 50)

    if not all_datasets:
        print("No valid data found in any subfolder. Exiting.")
        return

    # Prepare evaluation dataset
    eval_dataset = prepare_data([item['sql'] for item in eval_data], [item['lineage'] for item in eval_data])

    # Create model save directory
    model_save_path = f"model/{datetime.datetime.now().strftime('%Y-%m-%d-%H-%M-%S')}"
    os.makedirs(model_save_path, exist_ok=True)
    
    # Create data collator
    data_collator = DataCollatorForSeq2Seq(tokenizer=tokenizer, model="t5-base")
    
    # Optimize hyperparameters
    print("Optimizing hyperparameters...")
    best_params = optimize_hyperparameters(all_datasets, eval_dataset, model_save_path, data_collator)
    print("Best parameters:", best_params)
    
    # Save best parameters
    with open(f"{model_save_path}/best_params.json", 'w') as f:
        json.dump(best_params, f)
    
    # Final training with best parameters
    print("Training final model with best parameters...")
    rouge1, rouge2, rougeL = cross_validation(all_datasets, eval_dataset, best_params, model_save_path, data_collator)
    
    print(f"Final results - ROUGE-1: {rouge1:.3f}, ROUGE-2: {rouge2:.3f}, ROUGE-L: {rougeL:.3f}")
    
    print("Training complete. Model, tokenizer, and best parameters saved.")
    print(f"Model save path: {model_save_path}")

if __name__ == "__main__":
    main()
