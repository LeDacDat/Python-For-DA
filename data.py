import pandas as pd
import os
import mysql.connector
import re

def clean_order_id(order_id):
    # Loại bỏ các ký tự không hợp lệ
    cleaned_order_id = re.sub(r'[^\x00-\x7F]+', '', str(order_id)).strip()
    return cleaned_order_id

def import_excel_to_mysql():
    dataset_path = 'D:\\Python_Tutorial\\Test_INDA_3\\dataset'  # Thay đổi đường dẫn tới thư mục chứa file Excel/CSV
    excel_file = os.path.join(dataset_path, 'Modeling and DAX.xlsx')  # Thay đổi tên file Excel của bạn
    kpi_file = os.path.join(dataset_path, 'KPI Template.xlsx')  # Thay đổi tên file KPI của bạn

    # Kết nối tới MySQL
    connection = mysql.connector.connect(
        host='localhost',  # Thay đổi host nếu cần
        user='root',  # Thay đổi username của bạn
        password='Ledat@2002',  # Thay đổi password của bạn
        database='BAI_TEST'  # Thay đổi tên database của bạn
    )
    cursor = connection.cursor()

    # Đọc file Excel và import từng sheet vào MySQL
    df_customers = pd.read_excel(excel_file, sheet_name='Khách hàng')
    df_products = pd.read_excel(excel_file, sheet_name='Sản phẩm')
    df_employees = pd.read_excel(excel_file, sheet_name='Nhân viên')
    df_sales = pd.read_excel(excel_file, sheet_name='Dữ liệu bán hàng')
    df_branches = pd.read_excel(excel_file, sheet_name='Chi nhánh')
    df_kpi = pd.read_excel(kpi_file)

    # Kiểm tra độ dài tối đa của order_id
    max_length_order_id = df_sales['Đơn hàng'].apply(lambda x: len(clean_order_id(x))).max()
    print(f"Max length of order_id: {max_length_order_id}")

    # Tạo các bảng trong MySQL và import dữ liệu
    # Bảng Khách Hàng
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS customers (
            customer_id VARCHAR(10) PRIMARY KEY,
            customer_name VARCHAR(255)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    """)
    for _, row in df_customers.iterrows():
        cursor.execute("""
            INSERT INTO customers (customer_id, customer_name)
            VALUES (%s, %s)
            ON DUPLICATE KEY UPDATE customer_name=VALUES(customer_name)
        """, (row['Mã KH'], row['Khách hàng']))

    # Bảng Sản Phẩm
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS products (
            product_id VARCHAR(10) PRIMARY KEY,
            product_name VARCHAR(255),
            product_group VARCHAR(255)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    """)
    for _, row in df_products.iterrows():
        cursor.execute("""
            INSERT INTO products (product_id, product_name, product_group)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE product_name=VALUES(product_name), product_group=VALUES(product_group)
        """, (row['Mã Sản phẩm'], row['Sản phẩm'], row['Nhóm sản phẩm']))

    # Bảng Nhân Viên
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS employees (
            employee_id VARCHAR(10) PRIMARY KEY,
            employee_name VARCHAR(255)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    """)
    for _, row in df_employees.iterrows():
        cursor.execute("""
            INSERT INTO employees (employee_id, employee_name)
            VALUES (%s, %s)
            ON DUPLICATE KEY UPDATE employee_name=VALUES(employee_name)
        """, (row['Mã nhân viên bán'], row['Nhân viên bán']))

    # Bảng Bán Hàng
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS sales (
            sales_date DATE,
            order_id VARCHAR(255) PRIMARY KEY,
            customer_id VARCHAR(10),
            product_id VARCHAR(10),
            quantity INT,
            unit_price BIGINT,
            revenue DECIMAL(25,2),
            cost BIGINT,
            employee_id VARCHAR(10),
            branch_id VARCHAR(10)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    """)
    for _, row in df_sales.iterrows():
        try:
            order_id = clean_order_id(row['Đơn hàng'])
            if len(order_id) > 255:
                print(f"Skipping long order_id: {order_id}")
                continue
            cursor.execute("""
                INSERT INTO sales (sales_date, order_id, customer_id, product_id, quantity, unit_price, revenue, cost, employee_id, branch_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE customer_id=VALUES(customer_id), product_id=VALUES(product_id), quantity=VALUES(quantity),
                unit_price=VALUES(unit_price), revenue=VALUES(revenue), cost=VALUES(cost), employee_id=VALUES(employee_id), branch_id=VALUES(branch_id)
            """, (row['Ngày hạch toán'], order_id, row['Mã KH'], row['Mã Sản Phẩm'], row['Số lượng bán'], row['Đơn giá'], row['Doanh thu'], row['Giá vốn hàng hóa'], row['Mã nhân viên bán'], row['Chi nhánh']))
        except mysql.connector.errors.DataError as e:
            print(f"Data error for order_id {order_id}: {e}")
        except Exception as e:
            print(f"An error occurred for order_id {order_id}: {e}")

    # Bảng Chi Nhánh
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS branches (
            branch_id VARCHAR(10) PRIMARY KEY,
            branch_name VARCHAR(255),
            province VARCHAR(255)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    """)
    for _, row in df_branches.iterrows():
        cursor.execute("""
            INSERT INTO branches (branch_id, branch_name, province)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE branch_name=VALUES(branch_name), province=VALUES(province)
        """, (row['Mã chi nhánh'], row['Tên chi nhánh'], row['Tỉnh thành phố']))

    # Bảng KPI
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS kpi (
            year INT,
            branch_name VARCHAR(255),
            kpi_value DECIMAL(25,2)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    """)
    for _, row in df_kpi.iterrows():
        try:
            kpi_value = row['KPI'].replace(',', '')  # Chuyển dấu phẩy thành dấu chấm
            cursor.execute("""
                INSERT INTO kpi (year, branch_name, kpi_value)
                VALUES (%s, %s, %s)
                ON DUPLICATE KEY UPDATE kpi_value=VALUES(kpi_value)
            """, (row['Năm'], row['Chi nhánh'], kpi_value))
        except mysql.connector.errors.DataError as e:
            print(f"Data error: {e}")
        except Exception as e:
            print(f"An error occurred: {e}")

    connection.commit()
    cursor.close()
    connection.close()

if __name__ == "__main__":
    import_excel_to_mysql()
