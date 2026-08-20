import random
import pandas as pd
from datetime import datetime, timedelta

regions = {
    "Asia": ["Japan", "India", "China", "Singapore"],
    "Europe": ["Germany", "France", "Italy", "Spain"],
    "Africa": ["Libya", "Egypt", "Kenya", "Nigeria"],
    "Australia and Oceania": ["Fiji", "Australia", "New Zealand"],
    "North America": ["Canada", "United States", "Mexico"],
    "South America": ["Brazil", "Argentina", "Chile"],
}

item_types = {
    "Cosmetics": (437.20, 263.33),
    "Vegetables": (154.06, 90.93),
    "Baby Food": (255.28, 159.42),
    "Cereal": (205.70, 117.11),
    "Fruits": (9.33, 6.92),
    "Clothes": (109.28, 35.84),
    "Snacks": (152.58, 97.44),
    "Household": (668.27, 502.54),
    "Office Supplies": (651.21, 524.96),
    "Beverages": (47.45, 31.79),
    "Personal Care": (81.73, 56.67)
}   

rows = []

start_date = datetime(2010, 1, 1)
end_date = datetime(2017, 12, 31)

for i in range(1000):

    region = random.choice(list(regions.keys()))
    country = random.choice(regions[region])

    item_type = random.choice(list(item_types.keys()))
    unit_price, unit_cost = item_types[item_type]

    units_sold = random.randint(100, 10000)

    order_date = start_date + timedelta(
        days=random.randint(0, (end_date - start_date).days)
    )

    ship_date = order_date + timedelta(
        days=random.randint(1, 10)
    )

    total_revenue = units_sold * unit_price
    total_cost = units_sold * unit_cost
    total_profit = total_revenue - total_cost

    rows.append({
        "region": region,
        "country": country,
        "item_type": item_type,
        "sales_channel": random.choice(["Online", "Offline"]),
        "order_priority": random.choice(["C", "H", "M", "L"]),
        "order_date": order_date,
        "order_id": random.randint(100000000, 999999999),
        "ship_date": ship_date,
        "units_sold": units_sold,
        "unit_price": unit_price,
        "unit_cost": unit_cost,
        "total_revenue": round(total_revenue, 2),
        "total_cost": round(total_cost, 2),
        "total_profit": round(total_profit, 2)
    })

df = pd.DataFrame(rows)

df.to_csv("sales_data.csv", index=False)
print("synthetic data has been generated!!")