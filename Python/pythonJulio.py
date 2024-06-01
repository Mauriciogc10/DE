import csv
import tkinter as tk

def search_by_id(csv_file, id_to_find):
    """Searches for a specific ID in a CSV file and returns matching rows."""
    with open(csv_file, 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        matching_rows = []
        for row in reader:
            if row['a'] == id_to_find:
                matching_rows.append(row)
    return matching_rows

def search_action():
    id_to_find = id_entry.get()
    results = search_by_id(csv_file, id_to_find)

    if results:
        output_label.config(text="Matching rows:\n" + "\n".join(str(row) for row in results))
    else:
        output_label.config(text="No matching rows found.")

csv_file = '/Users/mauriciogodinezcastro/Desktop/LNN/Python/Data.csv'  # Replace with your actual CSV file name

root = tk.Tk()
root.title("CSV ID Search")

id_label = tk.Label(root, text="Enter ID to search:")
id_label.pack()

id_entry = tk.Entry(root)
id_entry.pack()

search_button = tk.Button(root, text="Search", command=search_action)
search_button.pack()

output_label = tk.Label(root, text="")
output_label.pack()

root.mainloop()
