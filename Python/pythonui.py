import tkinter as tk
from tkinter import messagebox
import csv

# Function to write data to CSV
def write_to_csv(name, entry_type, amount):
    with open('library_data.csv', mode='a', newline='') as file:
        writer = csv.writer(file)
        writer.writerow([name, entry_type, amount])

# Function to get data from entries and write to CSV
def submit():
    name = entry_name.get()
    entry_type = entry_type_var.get()
    amount = entry_amount.get()

    if not name or not entry_type or not amount:
        messagebox.showwarning("Input Error", "All fields are required!")
        return
    
    try:
        amount = float(amount)  # Ensure amount is a number
    except ValueError:
        messagebox.showerror("Input Error", "Amount must be a number!")
        return

    write_to_csv(name, entry_type, amount)
    messagebox.showinfo("Success", "Data successfully saved to CSV file!")

    # Clear the entries after submission
    entry_name.delete(0, tk.END)
    entry_amount.delete(0, tk.END)

# Create the main window
root = tk.Tk()
root.title("Library Data Entry")

# Create and place labels and entry widgets for Name
label_name = tk.Label(root, text="Name:")
label_name.grid(row=0, column=0, padx=10, pady=5)
entry_name = tk.Entry(root)
entry_name.grid(row=0, column=1, padx=10, pady=5)

# Create and place labels and entry widgets for Type
label_type = tk.Label(root, text="Type:")
label_type.grid(row=1, column=0, padx=10, pady=5)
entry_type_var = tk.StringVar()
entry_type = tk.Entry(root, textvariable=entry_type_var)
entry_type.grid(row=1, column=1, padx=10, pady=5)

# Create and place labels and entry widgets for Amount
label_amount = tk.Label(root, text="Amount:")
label_amount.grid(row=2, column=0, padx=10, pady=5)
entry_amount = tk.Entry(root)
entry_amount.grid(row=2, column=1, padx=10, pady=5)

# Create and place Submit button
submit_button = tk.Button(root, text="Submit", command=submit)
submit_button.grid(row=3, column=0, columnspan=2, pady=10)

# Start the GUI event loop
root.mainloop()
