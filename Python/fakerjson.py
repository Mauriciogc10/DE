import json
import datetime
from decimal import Decimal
from faker import Faker
import random

class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super(DecimalEncoder, self).default(obj)

def generate_data(num_items=100):
    """Generates a list of dictionaries with fake data, ensuring correct formatting for 'createDate'."""

    fake = Faker()
    data = []

    for _ in range(num_items):
        # Generate a datetime object for 'createDate'
        date_str = fake.date()  # Correctly creates a datetime object using Faker
        date_obj = datetime.datetime.strptime(date_str, '%Y-%m-%d')  # Convert string to datetime object

        item = {
            "Name": fake.name(),
            "type": random.choice(["gold", "silver", "bronze"]),  # Choose from the predefined options
            "price": round(fake.pydecimal(min_value=0.01, max_value=1000.00, positive=True), 2),
            "createDate": date_obj.strftime("%Y-%m-%d")  # Format the datetime object using strftime
        }
        data.append(item)

    return data

def write_to_json_file(data, filename):
    """Writes data to a JSON file."""
    with open(filename, 'w') as json_file:
        json.dump(data, json_file, cls=DecimalEncoder, indent=4)

# Generate the data
items = generate_data()

# Convert to JSON string with custom DecimalEncoder
json_data = json.dumps(items, cls=DecimalEncoder, indent=4)

# Write JSON data to a file
write_to_json_file(items, '/Users/mauriciogodinezcastro/Desktop/LNN/Python/fake_data.json')

# Print the JSON data
#print(json_data)
