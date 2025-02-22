import os
import pandas as pd
from datetime import datetime

# Loading the data
def parse_transaction(line):
    """
    Separates a transaction line into its components
    """
    try:
        parts = line.strip().split(",")
        if len(parts) != 4: #checks to see if the transaction line is in the right format
            return None
        sales_staff_id = int(parts[0])
        transaction_time = datetime.fromisoformat(parts[1])
        products_str = parts[2][1:-1] 
        products = products_str.split("|") #separates each product
        sales_amount = float(parts[3])
        return sales_staff_id, transaction_time, products, sales_amount
    except (ValueError, IndexError) as e:
        print(f"Skipping malformed line: {line.strip()} - Error: {e}")
        return None

def process_transaction_file(filename):
    """
    Processes the transaction file and returns a DataFrame
    """
    date = filename.split(".")[0]
    data = []
    with open(os.path.join(directory, filename), "r") as file:
        for line in file:
            transaction_data = parse_transaction(line) # separates each transaction line in their respective transaction file
            if not transaction_data:
                continue
            sales_staff_id, transaction_time, products, sales_amount = transaction_data
            for product in products:
                product_id, quantity = product.split(":") # separates each product from their quantity
                quantity = int(quantity)  # sets the quantity type to be an integer
                data.append([date, sales_staff_id, transaction_time, product_id, 
                             quantity, sales_amount])
    return pd.DataFrame(data, columns=["date", "sales_staff_id", "transaction_time", 
                                       "product_id", "quantity", "sales_amount"])

def main(directory):
    """
    Processes all the transaction files in the directory and returns a combined dataframe
    """
    file_list = sorted([title for title in os.listdir(directory) if title.endswith(".txt")])
    
    dataframes = [] # Empty list to store all the dataframes

    for file in file_list: 
        print(f"Processing file: {file}")
        df = process_transaction_file(file)
        print(f"DataFrame shape after processing: {df.shape}")
        dataframes.append(df)

    all_data = pd.concat(dataframes, ignore_index=True)
    return all_data

# Building the dataframe by executing main
directory = "/workspaces/Monieshop-hackathon/test-case2"
transaction_data = main(directory)
print(transaction_data.head())

def calculate_metrics(transaction_data):
    # Highest sales volume in a day
    volume_of_sales_day = transaction_data.groupby("date")["quantity"].sum()
    highest_sales_volume = int(volume_of_sales_day.max())
    day_of_highest_sales_volume = volume_of_sales_day.idxmax()

    # Highest sales value in a day
    sales_value_day = transaction_data.groupby("date")["sales_amount"].sum()
    highest_sales_value = float(sales_value_day.max())
    day_of_the_highest_sales_value = sales_value_day.idxmax()

    # Most sold product ID by volume
    productID_volume = transaction_data.groupby("product_id")["quantity"].sum()
    most_sold_productID = int(productID_volume.idxmax())
    most_sold_productID_value = int(productID_volume.max())

    # Highest sales staff ID for each month
    transaction_data["month"] = transaction_data["transaction_time"].dt.strftime("%m")
    staff_monthly_sales = transaction_data.groupby(["month", "sales_staff_id"])["sales_amount"].sum().reset_index()
    highest_sales_staff_by_month = staff_monthly_sales.loc[staff_monthly_sales.groupby("month")["sales_amount"].idxmax()]
    highest_sales_staff_by_month = dict(zip(highest_sales_staff_by_month["month"], highest_sales_staff_by_month["sales_staff_id"]))

    # Highest hour of the day by average transaction volume
    transaction_data["hour"] = transaction_data["transaction_time"].dt.hour
    hourly_avg_volume = transaction_data.groupby("hour")["quantity"].mean()
    highest_hourly_avg_volume = float(hourly_avg_volume.max())
    highest_avg_hour = int(hourly_avg_volume.idxmax())

    return{
        "highest sales volume" : (highest_sales_volume, day_of_highest_sales_volume),
        "highest sales value" : (highest_sales_value, day_of_the_highest_sales_value),
        "most sold productID" : (most_sold_productID_value, most_sold_productID),
        "highest sales staff ID" : highest_sales_staff_by_month,
        "highest hour avg" : (highest_hourly_avg_volume, highest_avg_hour)
    }

results = calculate_metrics(transaction_data)

print("1. Day of highest sales volume:", results["highest sales volume"][1], "with", results["highest sales volume"][0], "units")
print("2. Day of the highest sales value: ", results["highest sales value"][1], "with", round(results["highest sales value"][0], 2), "units" )
print("3. Most Product ID sold by volume: Product ",  results["most sold productID"][1], "with", results["most sold productID"][0], "units sold ")
print("4. Highest sales staff ID for each month: ", results["highest sales staff ID"])
print("5. Highest hour of the day:", f"{results["highest hour avg"][1]}:00", "with an average of ", round(results["highest hour avg"][0],0), "transactions per hour ")