import csv
import sqlite3
from collections import Counter

con = sqlite3.connect("shipment_database.db")
cur = con.cursor()

"""
shipping_data_0
"""

name_dict = {}  # { product_name : id}
product_id = 1
shipment_id = 1
with open("data/shipping_data_0.csv", newline="") as csvfile:
    reader = csv.DictReader(csvfile)
    rows = list(reader)

    # populate product table
    for row in rows:
        name = str(row["product"].strip().replace(" ", "_"))

        if name not in name_dict:
            name_dict[name] = product_id

            cur.execute("SELECT id FROM product WHERE name = ?", (name,))
            row = cur.fetchone()

            if row is None:
                cur.execute(
                    "INSERT INTO product (id, name) VALUES (?, ?)",
                    (
                        product_id,
                        name,
                    ),
                )
                product_id = product_id + 1
            else:
                product_id = product_id + 1

    # populate shipment table
    for row in rows:
        # current row data
        existing_product_id = str(name_dict[row["product"].strip().replace(" ", "_")])
        quantity = row["product_quantity"]
        origin = row["origin_warehouse"]
        dest = row["destination_store"]

        cur.execute(
            "INSERT INTO shipment (id, product_id, quantity, origin, destination) VALUES (?, ?, ?, ?, ?)",
            (shipment_id, existing_product_id, quantity, origin, dest),
        )

        shipment_id = shipment_id + 1

"""
shipping_data_1 & shipping_data_2
"""

with open("data/shipping_data_1.csv", newline="") as csvfile1, open(
    "data/shipping_data_2.csv", newline=""
) as csvfile2:
    reader1 = csv.DictReader(csvfile1)
    reader2 = csv.DictReader(csvfile2)
    rows1 = list(reader1)
    rows2 = list(reader2)

    shipment_dict = {}  # {shipment_identifier : {product_name: []str, origin: str, dest: str}

    # handle product table first
    for row in rows1:
        name = str(row["product"].strip().replace(" ", "_"))
        if name not in name_dict:
            name_dict[name] = product_id

            cur.execute("SELECT id FROM product WHERE name = ?", (name,))
            row = cur.fetchone()

            if row is None:
                cur.execute(
                    "INSERT INTO product (id, name) VALUES (?, ?)",
                    (
                        product_id,
                        name,
                    ),
                )
                product_id = product_id + 1
            else:
                product_id = product_id + 1

    # extract product name from shipping_data_1
    for row in rows1:
        shipment_identifier = str(row["shipment_identifier"].strip())
        product = str(row["product"].strip().replace(" ", "_"))
        if shipment_identifier not in shipment_dict:
            shipment_dict[shipment_identifier] = {
                "shipment_identifier": shipment_identifier,
                "product_names": [product],
                "origin": "",
                "dest": "",
            }
        else:
            shipment_dict[shipment_identifier]['product_names'].append(product)
    # extract origin and dest from shipping_data_2
    for row in rows2:
        shipment_identifier = str(row["shipment_identifier"].strip())
        origin = str(row['origin_warehouse'])
        dest = str(row['destination_store'])
        if shipment_identifier in shipment_dict:
            shipment_dict[shipment_identifier]['origin'] = origin
            shipment_dict[shipment_identifier]['dest'] = dest

    # push new rows into shipment table
    for shipment_details in shipment_dict.values():    
        origin = shipment_details['origin']
        dest = shipment_details['dest']

        products_in_shipment = shipment_details['product_names']
        product_quantities = Counter(products_in_shipment)

        for product_name, quantity in product_quantities.items():            
            existing_product_id = name_dict[product_name]
            cur.execute(
                "INSERT INTO shipment (id, product_id, quantity, origin, destination) VALUES (?, ?, ?, ?, ?)",
                (int(shipment_id), int(existing_product_id), int(quantity), str(origin), str(dest)),
            )

            shipment_id = shipment_id + 1
    
con.commit()
con.close()
