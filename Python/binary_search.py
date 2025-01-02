def binary_search(arr, low, high, target):
    # Base condition: If the range is invalid, the target is not found
    if low > high:
        return -1

    # Find the middle element
    mid = (low + high) // 2

    # If the target is found, return the index
    if arr[mid] == target:
        return mid

    # If the target is smaller, search in the left half
    elif arr[mid] > target:
        return binary_search(arr, low, mid - 1, target)

    # If the target is larger, search in the right half
    else:
        return binary_search(arr, mid + 1, high, target)

# Example usage:
arr = [1, 3, 5, 7, 9, 11, 13, 15]
target = 7
result = binary_search(arr, 0, len(arr) - 1, target)

if result != -1:
    print(f"Element {target} found at index {result}")
else:
    print(f"Element {target} not found in the array")