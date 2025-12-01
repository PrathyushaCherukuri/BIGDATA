import mysql.connector

#  DB Connection 

con = mysql.connector.connect(
    host="localhost",
    user="root",               # change if required
    password="Prathyusha@011",  # CHANGE THIS
    database="shopping_cart"
)

cursor = con.cursor(dictionary=True)

#  Functions 

def load_items():
    cursor.execute("SELECT * FROM items")
    return cursor.fetchall()

def update_stock(item_id, new_qty):
    cursor.execute("UPDATE items SET qty=%s WHERE id=%s", (new_qty, item_id))
    con.commit()

def insert_customer(name, address, distance):
    cursor.execute(
        "INSERT INTO customers (name, address, distance) VALUES (%s,%s,%s)",
        (name, address, distance)
    )
    con.commit()
    return cursor.lastrowid

def insert_order(cust_id, items_total, delivery, final):
    cursor.execute(
        "INSERT INTO orders (cust_id, total_items_cost, delivery_charge, final_amount) "
        "VALUES (%s,%s,%s,%s)",
        (cust_id, items_total, delivery, final)
    )
    con.commit()
    return cursor.lastrowid

def insert_order_item(order_id, item):
    cursor.execute(
        "INSERT INTO order_items (order_id, item_name, qty, cost) VALUES (%s,%s,%s,%s)",
        (order_id, item["item"], item["qty"], item["cost"])
    )
    con.commit()

# main program

cart = []

while True:
    items = load_items()

    print("\n-----------------------------------------------")
    print("S.No   Item        Qty Available   Cost")
    print("-----------------------------------------------")
    for i in items:
        print(f"{i['id']}     {i['name']:12} {i['qty']:5}         {i['cost']}")

    try:
        choice = int(input("\nEnter item number to buy: "))
    except:
        print("Invalid input")
        continue

    selected = next((x for x in items if x["id"] == choice), None)
    if not selected:
        print("Item not found!")
        continue

    try:
        qty = int(input(f"How many {selected['name']}? "))
    except:
        print("Invalid quantity")
        continue

    if qty <= 0:
        print("Quantity must be positive!")
        continue

    if qty > selected["qty"]:
        print(f"Only {selected['qty']} in stock!")
        continue

    # Add to cart
    cart.append({
        "item": selected["name"],
        "qty": qty,
        "cost": selected["cost"],
        "id": selected["id"]
    })

    # Reduce stock in database
    update_stock(selected["id"], selected["qty"] - qty)

    cont = input("Add more items? (Y/N): ").lower()
    if cont == "n":
        break

#  Customer details

cust_name = input("Enter your name: ")
address = input("Enter your address: ")
distance = int(input("Enter distance from store (km): "))

# Delivery charges
if distance <= 15:
    delivery = 50
elif distance <= 30:
    delivery = 100
else:
    print("No delivery beyond 30 km")
    delivery = 0

total_items_cost = sum(c["qty"] * c["cost"] for c in cart)
final_amount = total_items_cost + delivery

# Insert into DB
cust_id = insert_customer(cust_name, address, distance)
order_id = insert_order(cust_id, total_items_cost, delivery, final_amount)

for item in cart:
    insert_order_item(order_id, item)

# Bill

print("\n------------------- FINAL BILL ----------------------")
print(f"Customer: {cust_name}")
print(f"Address : {address}")
print("------------------------------------------------------")

sr = 1
for c in cart:
    total = c["qty"] * c["cost"]
    print(f"{sr}. {c['item']:10} x {c['qty']}  = {total}")
    sr += 1

print("\nTotal Items Cost :", total_items_cost)
print("Delivery Charge  :", delivery)
print("Final Amount     :", final_amount)
print("------------------------------------------------------")
print("Thanks for shopping with us!")
