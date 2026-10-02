import csv
import random
from datetime import datetime, timezone
import os

def generate_seeds(num_records=1200, output_dir="seeds"):
    random.seed(42)
    os.makedirs(output_dir, exist_ok=True)
    sql_file = os.path.join(output_dir, "init.sql")
    csv_file = os.path.join(output_dir, "properties.csv")
    
    properties = []
    
    # 600 records for US, 600 records for EU
    half = num_records // 2
    
    for i in range(1, num_records + 1):
        region = "us" if i <= half else "eu"
        bedrooms = random.randint(1, 5)
        bathrooms = random.randint(1, 4)
        
        # Base price depends on bedrooms and bathrooms + regional variance
        base = 150000 + (bedrooms * 65000) + (bathrooms * 35000)
        variance = random.randint(-25000, 75000)
        price = round(max(85000.0, float(base + variance)), 2)
        
        properties.append({
            "id": i,
            "price": price,
            "bedrooms": bedrooms,
            "bathrooms": bathrooms,
            "region_origin": region,
            "version": 1,
            "updated_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        })

    # Write CSV
    with open(csv_file, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["id", "price", "bedrooms", "bathrooms", "region_origin", "version", "updated_at"])
        writer.writeheader()
        writer.writerows(properties)

    # Write SQL init script
    with open(sql_file, mode="w", encoding="utf-8") as f:
        f.write("-- Database schema initialization\n\n")
        f.write("CREATE TABLE IF NOT EXISTS properties (\n")
        f.write("    id BIGINT PRIMARY KEY,\n")
        f.write("    price DECIMAL(12, 2) NOT NULL,\n")
        f.write("    bedrooms INTEGER,\n")
        f.write("    bathrooms INTEGER,\n")
        f.write("    region_origin VARCHAR(2) NOT NULL,\n")
        f.write("    version INTEGER NOT NULL DEFAULT 1,\n")
        f.write("    updated_at TIMESTAMP NOT NULL DEFAULT NOW()\n")
        f.write(");\n\n")
        
        f.write("CREATE TABLE IF NOT EXISTS idempotency_keys (\n")
        f.write("    key VARCHAR(255) PRIMARY KEY,\n")
        f.write("    status_code INTEGER NOT NULL,\n")
        f.write("    response_body TEXT NOT NULL,\n")
        f.write("    created_at TIMESTAMP NOT NULL DEFAULT NOW()\n")
        f.write(");\n\n")
        
        f.write("CREATE INDEX IF NOT EXISTS idx_properties_region ON properties(region_origin);\n\n")
        
        f.write("-- Seed initial properties\n")
        f.write("INSERT INTO properties (id, price, bedrooms, bathrooms, region_origin, version, updated_at) VALUES\n")
        
        values = []
        for prop in properties:
            values.append(f"({prop['id']}, {prop['price']:.2f}, {prop['bedrooms']}, {prop['bathrooms']}, '{prop['region_origin']}', {prop['version']}, '{prop['updated_at']}')")
        
        f.write(",\n".join(values))
        f.write("\nON CONFLICT (id) DO NOTHING;\n")

    print(f"Generated {num_records} property records in {sql_file} and {csv_file}")

if __name__ == "__main__":
    generate_seeds()
