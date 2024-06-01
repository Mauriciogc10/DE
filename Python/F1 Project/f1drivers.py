import pandas as pd
import tkinter as tk
from tkinter import messagebox
import webbrowser

class URLSearchApp:
    def __init__(self, master):
        self.master = master
        self.master.title("URL Search by Reference")
        
        # Load the CSV data
        self.data = pd.read_csv('/Users/mauriciogodinezcastro/Desktop/LNN/DE/Python/F1 Project/driversF1.csv')
        
        # Set up the UI
        self.label = tk.Label(master, text="Enter Reference:")
        self.label.pack(pady=10)
        
        self.entry = tk.Entry(master, width=50)
        self.entry.pack(pady=10)
        
        self.search_button = tk.Button(master, text="Search and Open URL", command=self.search_and_open_url)
        self.search_button.pack(pady=10)

    def search_and_open_url(self):
        reference = self.entry.get()
        if reference:
            try:
                url = self.data.loc[self.data['driverId'] == reference, 'url'].values[0]
                webbrowser.open(url)
            except IndexError:
                messagebox.showerror("Error", "Reference not found.")
        else:
            messagebox.showwarning("Input Error", "Please enter a reference.")

if __name__ == "__main__":
    root = tk.Tk()
    app = URLSearchApp(root)
    root.mainloop()
