# 📘 Data Catalog – Gold Layer

This document describes the **Gold Layer** tables used in the Data Warehouse for sales analysis.  
It includes **dimensions** and **fact tables** along with their attributes and relationships.

---

## 🟨 Table: `gold.dim_customers`

**Description:** Contains customer master data.

| Column          | Data Type | Description                                      |
|-----------------|-----------|--------------------------------------------------|
| `customer_key`  | INT (PK)  | Surrogate key for the customer                   |
| `customer_id`   | STRING    | Business customer identifier (from source)       |
| `customer_number` | STRING  | Customer account number                          |
| `first_name`    | STRING    | Customer’s first name                            |
| `last_name`     | STRING    | Customer’s last name                             |
| `country`       | STRING    | Country of residence                             |
| `marital_status`| STRING    | Marital status (Single, Married, etc.)           |
| `gender`        | STRING    | Gender (M/F/Other)                               |
| `birthdate`     | DATE      | Date of birth                                    |

---

## 🟥 Table: `gold.dim_products`

**Description:** Contains product master data.

| Column          | Data Type | Description                                      |
|-----------------|-----------|--------------------------------------------------|
| `product_key`   | INT (PK)  | Surrogate key for the product                    |
| `product_id`    | STRING    | Business product identifier (from source)        |
| `product_number`| STRING    | Internal product number/code                     |
| `product_name`  | STRING    | Product name                                     |
| `category_id`   | STRING    | ID of the product category                       |
| `category`      | STRING    | Product category (e.g., Electronics, Apparel)    |
| `subcategory`   | STRING    | More specific product subcategory                |
| `maintenance`   | STRING    | Maintenance requirements / support level         |
| `cost`          | DECIMAL   | Cost of the product                              |
| `product_line`  | STRING    | Product line or series                           |
| `start_date`    | DATE      | Date product became available                    |

---

## 🟦 Table: `gold.fact_sales`

**Description:** Stores all sales transactions, linked to customers and products.

| Column          | Data Type | Description                                      |
|-----------------|-----------|--------------------------------------------------|
| `order_number`  | STRING    | Unique order number                              |
| `product_key`   | INT (FK1) | References `gold.dim_products.product_key`       |
| `customer_key`  | INT (FK2) | References `gold.dim_customers.customer_key`     |
| `order_date`    | DATE      | Date when the order was placed                   |
| `shipping_date` | DATE      | Date when the order was shipped                  |
| `due_date`      | DATE      | Date when the order is due                       |
| `quantity`      | INT       | Number of products sold                          |
| `price`         | DECIMAL   | Price per unit                                   |
| `sales_amount`  | DECIMAL   | Total sales amount (`quantity * price`)          |

---

## 🔗 Relationships

- `gold.fact_sales.customer_key` → `gold.dim_customers.customer_key`  
- `gold.fact_sales.product_key` → `gold.dim_products.product_key`

---

## 📊 Business Rule

**Sales Calculation: sales_amount = quantity * price **  
