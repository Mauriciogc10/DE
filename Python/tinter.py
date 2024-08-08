import tkinter as tk
from tkinter import messagebox
import csv

def save_to_csv(department, product, product_id):
    # Define the CSV file name
    filename = "products.csv"
    
    # Open the CSV file in append mode
    with open(filename, mode='a', newline='') as file:
        writer = csv.writer(file)
        
        # Write the data to the CSV file
        writer.writerow([department, product, product_id])
    
    # Show a success message
    messagebox.showinfo("Success", "Data saved to CSV")

def submit():
    # Get the input data from the entry widgets
    department = department_entry.get()
    product = product_entry.get()
    product_id = product_id_entry.get()
    
    # Validate the input data
    if department and product and product_id:
        save_to_csv(department, product, product_id)
        a
        # Clear the entry widgets
        department_entry.delete(0, tk.END)
        product_entry.delete(0, tk.END)
        product_id_entry.delete(0, tk.END)
    else:
        messagebox.showerror("Error", "Please fill out all fields")

# Create the main window
root = tk.Tk()
root.title("Product Entry")

# Create and place the labels and entry widgets
tk.Label(root, text="Department:").grid(row=0, column=0, padx=10, pady=5)
department_entry = tk.Entry(root)
department_entry.grid(row=0, column=1, padx=10, pady=5)

tk.Label(root, text="Product:").grid(row=1, column=0, padx=10, pady=5)
product_entry = tk.Entry(root)
product_entry.grid(row=1, column=1, padx=10, pady=5)

tk.Label(root, text="Product ID:").grid(row=2, column=0, padx=10, pady=5)
product_id_entry = tk.Entry(root)
product_id_entry.grid(row=2, column=1, padx=10, pady=5)

# Create and place the submit button
submit_button = tk.Button(root, text="Submit", command=submit)
submit_button.grid(row=3, column=0, columnspan=2, pady=10)

# Run the application
root.mainloop()