# Store items
items = [
    {"sr": 1, "name": "Biscuits", "qty": 5, "cost": 20.5},
    {"sr": 2, "name": "Cereals", "qty": 10, "cost": 90},
    {"sr": 3, "name": "Chicken", "qty": 20, "cost": 100}
]

cart = []

print("S.No   Item       Quantity   Cost/Item")
print("-----  --------   --------   ----------")
for i in items:
    print(f"{i['sr']}      {i['name']:10} {i['qty']:8}   {i['cost']}")

while True:
    try:
        choice = int(input("\nWhat do you want to purchase? "))
    except:
        print("Invalid")
        continue

    selected = next((x for x in items if x["sr"] == choice), None)
    if not selected:
        print("Invalid")
        continue

    name = selected["name"]
    stock = selected["qty"]

    try:
        qty = int(input(f"How many {name} packets you want to purchase: "))
    except:
        print("Invalid")
        continue

    if qty <= 0:
        print("Invalid")
        continue
    if qty > stock:
        print(f"Available quantity of {name} is {stock}:")
        cont = input("Do you want to continue shopping? Y/N ").lower()
        if cont == "n":
            break
        continue

    # Add to cart
    cart.append({"item": name, "qty": qty, "cost": selected["cost"]})
    selected["qty"] -= qty

    cont = input("Do you want to continue shopping? Y/N ").lower()
    if cont == "n":
        break

# Customer details
cust_name = input("Enter your name: ")
address = input("Enter your address: ")

try:
    distance = int(input("Enter the distance from store 5/10/15/30: "))
except:
    distance = 0

# Delivery charge logic
if distance <= 15:
    delivery = 50
elif 15 < distance <= 30:
    delivery = 100
else:
    delivery = None
    print("No delivery beyond 30 km")


#        FINAL BILL

print("\n--------------------Bill----------------------")
print(" S.No   Item       Qty    TotalCost")

total_items_cost = 0
sr = 1

for c in cart:
    tcost = c["qty"] * c["cost"]
    total_items_cost += tcost
    print(f"  {sr}     {c['item']:10} {c['qty']:3}     {tcost:.2f}")
    sr += 1

print(f"\nTotal items cost:   {total_items_cost}")

if delivery is not None:
    final_amount = total_items_cost + delivery
    print(f"\nTotal Bill Amount: Total items cost + Delivery Charge is:   {final_amount}")

print(f"Name:   {cust_name}")
print(f"Address:  {address}")
print("Have a nice day!!")


# REMAINING STOCK

print("\n----------------Remaining Quantity In Store-------------------")
print("S.No   Item       Qty   Cost/Item")

for i in items:
    print(f"{i['sr']}      {i['name']:10} {i['qty']:3}     {i['cost']:.2f}")


