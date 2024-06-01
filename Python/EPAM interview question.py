1. Write a Python code to print the factorial of any number, n.
(Factorial of n. n!=n∗(n−1)∗(n−2)∗.....2∗1)

Solution: 
def factorial(n):
 if n < 0:
 return "Factorial is not defined for negative numbers."
 elif n == 0 or n == 1:
 return 1
 else:
 answer = 1
 for i in range(1, n + 1):
 answer *= i   # answer = answer*i
 return answer

# Example usage:
try:
 n = int(input("Enter a number to calculate its factorial: "))
 print(f"The factorial of {n} is {factorial(n)}.")
except ValueError:
 print("Please enter a valid integer.")

follow up question.

Sure, here's a rephrased version:

2. Create a program in Python to output the number of trailing zeros in the factorial of a given number, n.

example 5! = 120 and trailing zero is 1. Like wise for 9! = 362880 and trailing zero is 1.

def trailing_zeroes_in_factorial(n):
 # Initialize the count of trailing zeroes
 count = 0
 
 # Initialize the current power of 5
 power_of_5 = 5
 
 # Loop to count the number of multiples of 5, 25, 125, etc. in n
 while n >= power_of_5:
 # Add the number of multiples of the current power of 5 to the count
 count += n // power_of_5 
 
 # Move to the next power of 5 (i.e., 5, 25, 125, ...)
 power_of_5 *= 5 
 
 # Return the total count of trailing zeroes
 return count

# Example usage:
# Take input from the user for which factorial trailing zeroes need to be found
n = int(input("Enter a number to find the number of trailing zeroes in its factorial: "))

# Print the result by calling the function and formatting the output
print(f"The number of trailing zeroes in {n}! is {trailing_zeroes_in_factorial(n)}.")