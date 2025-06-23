import string

def print_rangoli(n):
    alphabets = string.ascii_lowercase
    result = []

    for i in range(n):
        left = alphabets[n-1:i:-1]
        center = alphabets[i]
        right = alphabets[i+1:n]
        row = '-'.join(left + center + right)
        result.append(row.center(4*n - 3, '-'))

    for row in result[::-1] + result[1:]:
        print(row)

# Example
n = int(input("Enter size: "))
print_rangoli(n)
