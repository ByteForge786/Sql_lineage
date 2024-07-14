import os

# Example SQL queries and lineage descriptions
examples = [
    {
        'sql': """
        WITH cte1 AS (
            SELECT customer_id, SUM(amount) AS total_spent
            FROM orders
            WHERE order_date >= '2023-01-01'
            GROUP BY customer_id
        ),
        cte2 AS (
            SELECT customer_id, COUNT(order_id) AS order_count
            FROM orders
            WHERE order_date >= '2023-01-01'
            GROUP BY customer_id
        ),
        cte3 AS (
            SELECT c.customer_id, c.customer_name, cte1.total_spent, cte2.order_count
            FROM customers c
            LEFT JOIN cte1 ON c.customer_id = cte1.customer_id
            LEFT JOIN cte2 ON c.customer_id = cte2.customer_id
        )
        SELECT cte3.customer_id, cte3.customer_name, cte3.total_spent, cte3.order_count, r.region_name
        FROM cte3
        JOIN regions r ON cte3.customer_id = r.customer_id
        WHERE cte3.total_spent > 500
        ORDER BY cte3.total_spent DESC;
        """,
        'lineage': """
        cte1.customer_id <- orders.customer_id
        cte1.total_spent <- orders.sum(amount)

        cte2.customer_id <- orders.customer_id
        cte2.order_count <- orders.count(order_id)

        cte3.customer_id <- customers.customer_id
        cte3.customer_name <- customers.customer_name
        cte3.total_spent <- cte1.total_spent
        cte3.order_count <- cte2.order_count

        final_result.customer_id <- cte3.customer_id
        final_result.customer_name <- cte3.customer_name
        final_result.total_spent <- cte3.total_spent
        final_result.order_count <- cte3.order_count
        final_result.region_name <- regions.region_name
        """
    },
    {
        'sql': """
        WITH cte_sales AS (
            SELECT product_id, EXTRACT(YEAR FROM sale_date) AS sale_year, SUM(amount) AS total_sales
            FROM sales
            GROUP BY product_id, sale_year
        ),
        cte_products AS (
            SELECT p.product_id, p.product_name, ps.total_sales, ps.sale_year
            FROM products p
            JOIN cte_sales ps ON p.product_id = ps.product_id
        )
        SELECT cte_products.product_id, cte_products.product_name, cte_products.total_sales, cte_products.sale_year, r.region_name
        FROM cte_products
        JOIN regions r ON cte_products.product_id = r.product_id
        WHERE cte_products.total_sales > 1000
        ORDER BY cte_products.total_sales DESC;
        """,
        'lineage': """
        cte_sales.product_id <- sales.product_id
        cte_sales.sale_year <- sales.extract(year)
        cte_sales.total_sales <- sales.sum(amount)

        cte_products.product_id <- products.product_id
        cte_products.product_name <- products.product_name
        cte_products.total_sales <- cte_sales.total_sales
        cte_products.sale_year <- cte_sales.sale_year

        final_result.product_id <- cte_products.product_id
        final_result.product_name <- cte_products.product_name
        final_result.total_sales <- cte_products.total_sales
        final_result.sale_year <- cte_products.sale_year
        final_result.region_name <- regions.region_name
        """
    },
    {
        'sql': """
        WITH recursive cte_hierarchy AS (
            SELECT employee_id, manager_id, employee_name, 1 AS level
            FROM employees
            WHERE manager_id IS NULL
            UNION ALL
            SELECT e.employee_id, e.manager_id, e.employee_name, h.level + 1
            FROM employees e
            JOIN cte_hierarchy h ON e.manager_id = h.employee_id
        )
        SELECT employee_id, employee_name, level
        FROM cte_hierarchy
        ORDER BY level, employee_id;
        """,
        'lineage': """
        cte_hierarchy.employee_id <- employees.employee_id
        cte_hierarchy.manager_id <- employees.manager_id
        cte_hierarchy.employee_name <- employees.employee_name
        cte_hierarchy.level <- 1 (initial value)

        final_result.employee_id <- cte_hierarchy.employee_id
        final_result.employee_name <- cte_hierarchy.employee_name
        final_result.level <- cte_hierarchy.level
        """
    },
    {
        'sql': """
        WITH cte_customer_orders AS (
            SELECT c.customer_id, c.customer_name, COUNT(o.order_id) AS order_count, SUM(o.total_amount) AS total_amount
            FROM customers c
            LEFT JOIN orders o ON c.customer_id = o.customer_id
            GROUP BY c.customer_id, c.customer_name
        )
        SELECT customer_id, customer_name, order_count, total_amount,
               CASE
                   WHEN total_amount > 5000 THEN 'High Value'
                   WHEN total_amount > 2000 THEN 'Medium Value'
                   ELSE 'Low Value'
               END AS customer_category
        FROM cte_customer_orders
        ORDER BY total_amount DESC;
        """,
        'lineage': """
        cte_customer_orders.customer_id <- customers.customer_id
        cte_customer_orders.customer_name <- customers.customer_name
        cte_customer_orders.order_count <- orders.count(order_id)
        cte_customer_orders.total_amount <- orders.sum(total_amount)

        final_result.customer_id <- cte_customer_orders.customer_id
        final_result.customer_name <- cte_customer_orders.customer_name
        final_result.order_count <- cte_customer_orders.order_count
        final_result.total_amount <- cte_customer_orders.total_amount
        final_result.customer_category <- (CASE expression)
        """
    },
    {
        'sql': """
        WITH cte_top_customers AS (
            SELECT customer_id, customer_name, SUM(total_amount) AS total_spent
            FROM orders
            JOIN customers ON orders.customer_id = customers.customer_id
            GROUP BY customer_id, customer_name
            HAVING SUM(total_amount) > 10000
        ),
        cte_avg_spending AS (
            SELECT AVG(total_amount) AS avg_spent
            FROM orders
        )
        SELECT c.customer_name, c.total_spent, a.avg_spent
        FROM cte_top_customers c
        CROSS JOIN cte_avg_spending a
        ORDER BY c.total_spent DESC;
        """,
        'lineage': """
        cte_top_customers.customer_id <- orders.customer_id
        cte_top_customers.customer_name <- customers.customer_name
        cte_top_customers.total_spent <- orders.sum(total_amount)

        cte_avg_spending.avg_spent <- orders.avg(total_amount)

        final_result.customer_name <- cte_top_customers.customer_name
        final_result.total_spent <- cte_top_customers.total_spent
        final_result.avg_spent <- cte_avg_spending.avg_spent
        """
    },
    {
        'sql': """
        WITH recursive cte_org_structure AS (
            SELECT employee_id, manager_id, employee_name, 1 AS level
            FROM employees
            WHERE manager_id IS NULL
            UNION ALL
            SELECT e.employee_id, e.manager_id, e.employee_name, h.level + 1
            FROM employees e
            JOIN cte_org_structure h ON e.manager_id = h.employee_id
        )
        SELECT employee_id, employee_name, level
        FROM cte_org_structure
        ORDER BY level, employee_id;
        """,
        'lineage': """
        cte_org_structure.employee_id <- employees.employee_id
        cte_org_structure.manager_id <- employees.manager_id
        cte_org_structure.employee_name <- employees.employee_name
        cte_org_structure.level <- 1 (initial value)

        final_result.employee_id <- cte_org_structure.employee_id
        final_result.employee_name <- cte_org_structure.employee_name
        final_result.level <- cte_org_structure.level
        """
    },
    {
        'sql': """
        WITH cte_orders AS (
            SELECT order_id, customer_id, order_date, total_amount
            FROM orders
            WHERE order_date >= '2023-01-01'
        ),
        cte_customers AS (
            SELECT customer_id, customer_name, city
            FROM customers
        )
        SELECT o.order_id, c.customer_name, c.city, o.total_amount
        FROM cte_orders o
        JOIN cte_customers c ON o.customer_id = c.customer_id
        WHERE o.total_amount > 1000
        ORDER BY o.total_amount DESC;
        """,
        'lineage': """
        cte_orders.order_id <- orders.order_id
        cte_orders.customer_id <- orders.customer_id
        cte_orders.order_date <- orders.order_date
        cte_orders.total_amount <- orders.total_amount

        cte_customers.customer_id <- customers.customer_id
        cte_customers.customer_name <- customers.customer_name
        cte_customers.city <- customers.city

        final_result.order_id <- cte_orders.order_id
        final_result.customer_name <- cte_customers.customer_name
        final_result.city <- cte_customers.city
        final_result.total_amount <- cte_orders.total_amount
        """
    },
    {
        'sql': """
        WITH cte_product_sales AS (
            SELECT p.product_id, p.product_name, SUM(s.amount) AS total_sales
            FROM products p
            JOIN sales s ON p.product_id = s.product_id
            GROUP BY p.product_id, p.product_name
            HAVING SUM(s.amount) > 5000
        ),
        cte_avg_sales AS (
            SELECT AVG(total_sales) AS avg_sales
            FROM cte_product_sales
        )
        SELECT c.product_name, c.total_sales, a.avg_sales
        FROM cte_product_sales c
        CROSS JOIN cte_avg_sales a
        ORDER BY c.total_sales DESC;
        """,
        'lineage': """
        cte_product_sales.product_id <- products.product_id
        cte_product_sales.product_name <- products.product_name
        cte_product_sales.total_sales <- sales.sum(amount)

        cte_avg_sales.avg_sales <- cte_product_sales.avg(total_sales)

        final_result.product_name <- cte_product_sales.product_name
        final_result.total_sales <- cte_product_sales.total_sales
        final_result.avg_sales <- cte_avg_sales.avg_sales
        """
    },
    {
        'sql': """
        WITH cte_orders AS (
            SELECT order_id, order_date, total_amount
            FROM orders
            WHERE order_date >= '2023-01-01'
        ),
        cte_top_orders AS (
            SELECT o.order_id, o.order_date, o.total_amount, c.customer_name
            FROM cte_orders o
            JOIN customers c ON o.customer_id = c.customer_id
            WHERE o.total_amount > 5000
            ORDER BY o.total_amount DESC
            LIMIT 10
        )
        SELECT t.order_id, t.order_date, t.total_amount, t.customer_name
        FROM cte_top_orders t;
        """,
        'lineage': """
        cte_orders.order_id <- orders.order_id
        cte_orders.order_date <- orders.order_date
        cte_orders.total_amount <- orders.total_amount

        cte_top_orders.order_id <- cte_orders.order_id
        cte_top_orders.order_date <- cte_orders.order_date
        cte_top_orders.total_amount <- cte_orders.total_amount
        cte_top_orders.customer_name <- customers.customer_name

        final_result.order_id <- cte_top_orders.order_id
        final_result.order_date <- cte_top_orders.order_date
        final_result.total_amount <- cte_top_orders.total_amount
        final_result.customer_name <- cte_top_orders.customer_name
        """
    },
    {
        'sql': """
        WITH recursive cte_hierarchy AS (
            SELECT employee_id, manager_id, employee_name, 1 AS level
            FROM employees
            WHERE manager_id IS NULL
            UNION ALL
            SELECT e.employee_id, e.manager_id, e.employee_name, h.level + 1
            FROM employees e
            JOIN cte_hierarchy h ON e.manager_id = h.employee_id
        )
        SELECT employee_id, employee_name, level
        FROM cte_hierarchy
        ORDER BY level, employee_id;
        """,
        'lineage': """
        cte_hierarchy.employee_id <- employees.employee_id
        cte_hierarchy.manager_id <- employees.manager_id
        cte_hierarchy.employee_name <- employees.employee_name
        cte_hierarchy.level <- 1 (initial value)

        final_result.employee_id <- cte_hierarchy.employee_id
        final_result.employee_name <- cte_hierarchy.employee_name
        final_result.level <- cte_hierarchy.level
        """
    },
    {
        'sql': """
        WITH cte_orders AS (
            SELECT order_id, customer_id, order_date, total_amount
            FROM orders
            WHERE order_date >= '2023-01-01'
        ),
        cte_customers AS (
            SELECT customer_id, customer_name, city
            FROM customers
        )
        SELECT o.order_id, c.customer_name, c.city, o.total_amount
        FROM cte_orders o
        JOIN cte_customers c ON o.customer_id = c.customer_id
        WHERE o.total_amount > 1000
        ORDER BY o.total_amount DESC;
        """,
        'lineage': """
        cte_orders.order_id <- orders.order_id
        cte_orders.customer_id <- orders.customer_id
        cte_orders.order_date <- orders.order_date
        cte_orders.total_amount <- orders.total_amount

        cte_customers.customer_id <- customers.customer_id
        cte_customers.customer_name <- customers.customer_name
        cte_customers.city <- customers.city

        final_result.order_id <- cte_orders.order_id
        final_result.customer_name <- cte_customers.customer_name
        final_result.city <- cte_customers.city
        final_result.total_amount <- cte_orders.total_amount
        """
    },
    {
        'sql': """
        WITH cte_product_sales AS (
            SELECT p.product_id, p.product_name, SUM(s.amount) AS total_sales
            FROM products p
            JOIN sales s ON p.product_id = s.product_id
            GROUP BY p.product_id, p.product_name
            HAVING SUM(s.amount) > 5000
        ),
        cte_avg_sales AS (
            SELECT AVG(total_sales) AS avg_sales
            FROM cte_product_sales
        )
        SELECT c.product_name, c.total_sales, a.avg_sales
        FROM cte_product_sales c
        CROSS JOIN cte_avg_sales a
        ORDER BY c.total_sales DESC;
        """,
        'lineage': """
        cte_product_sales.product_id <- products.product_id
        cte_product_sales.product_name <- products.product_name
        cte_product_sales.total_sales <- sales.sum(amount)

        cte_avg_sales.avg_sales <- cte_product_sales.avg(total_sales)

        final_result.product_name <- cte_product_sales.product_name
        final_result.total_sales <- cte_product_sales.total_sales
        final_result.avg_sales <- cte_avg_sales.avg_sales
        """
    },
    {
        'sql': """
        WITH cte_orders AS (
            SELECT order_id, order_date, total_amount
            FROM orders
            WHERE order_date >= '2023-01-01'
        ),
        cte_top_orders AS (
            SELECT o.order_id, o.order_date, o.total_amount, c.customer_name
            FROM cte_orders o
            JOIN customers c ON o.customer_id = c.customer_id
            WHERE o.total_amount > 5000
            ORDER BY o.total_amount DESC
            LIMIT 10
        )
        SELECT t.order_id, t.order_date, t.total_amount, t.customer_name
        FROM cte_top_orders t;
        """,
        'lineage': """
        cte_orders.order_id <- orders.order_id
        cte_orders.order_date <- orders.order_date
        cte_orders.total_amount <- orders.total_amount

        cte_top_orders.order_id <- cte_orders.order_id
        cte_top_orders.order_date <- cte_orders.order_date
        cte_top_orders.total_amount <- cte_orders.total_amount
        cte_top_orders.customer_name <- customers.customer_name

        final_result.order_id <- cte_top_orders.order_id
        final_result.order_date <- cte_top_orders.order_date
        final_result.total_amount <- cte_top_orders.total_amount
        final_result.customer_name <- cte_top_orders.customer_name
        """
    },
    {
        'sql': """
        WITH recursive cte_hierarchy AS (
            SELECT employee_id, manager_id, employee_name, 1 AS level
            FROM employees
            WHERE manager_id IS NULL
            UNION ALL
            SELECT e.employee_id, e.manager_id, e.employee_name, h.level + 1
            FROM employees e
            JOIN cte_hierarchy h ON e.manager_id = h.employee_id
        )
        SELECT employee_id, employee_name, level
        FROM cte_hierarchy
        ORDER BY level, employee_id;
        """,
        'lineage': """
        cte_hierarchy.employee_id <- employees.employee_id
        cte_hierarchy.manager_id <- employees.manager_id
        cte_hierarchy.employee_name <- employees.employee_name
        cte_hierarchy.level <- 1 (initial value)

        final_result.employee_id <- cte_hierarchy.employee_id
        final_result.employee_name <- cte_hierarchy.employee_name
        final_result.level <- cte_hierarchy.level
        """
    },
    {
        'sql': """
        WITH cte_orders AS (
            SELECT order_id, customer_id, order_date, total_amount
            FROM orders
            WHERE order_date >= '2023-01-01'
        ),
        cte_customers AS (
            SELECT customer_id, customer_name, city
            FROM customers
        )
        SELECT o.order_id, c.customer_name, c.city, o.total_amount
        FROM cte_orders o
        JOIN cte_customers c ON o.customer_id = c.customer_id
        WHERE o.total_amount > 1000
        ORDER BY o.total_amount DESC;
        """,
        'lineage': """
        cte_orders.order_id <- orders.order_id
        cte_orders.customer_id <- orders.customer_id
        cte_orders.order_date <- orders.order_date
        cte_orders.total_amount <- orders.total_amount

        cte_customers.customer_id <- customers.customer_id
        cte_customers.customer_name <- customers.customer_name
        cte_customers.city <- customers.city

        final_result.order_id <- cte_orders.order_id
        final_result.customer_name <- cte_customers.customer_name
        final_result.city <- cte_customers.city
        final_result.total_amount <- cte_orders.total_amount
        """
    }
]

# Function to create the Input folder and subfolders with sql.txt and output.txt
def create_input_folders(examples):
    input_folder = "Input"
    if not os.path.exists(input_folder):
        os.makedirs(input_folder)
    
    for i, example in enumerate(examples, start=1):
        subfolder_path = os.path.join(input_folder, str(i))
        if not os.path.exists(subfolder_path):
            os.makedirs(subfolder_path)
        
        # Write sql.txt
        with open(os.path.join(subfolder_path, 'sql.txt'), 'w') as f_sql:
            f_sql.write(example['sql'].strip() + '\n')
        
        # Write output.txt
        with open(os.path.join(subfolder_path, 'output.txt'), 'w') as f_output:
            f_output.write(example['lineage'].strip() + '\n')

# Create Input folder with subfolders and files
create_input_folders(examples)
print("Input folder with subfolders and files created successfully.")
