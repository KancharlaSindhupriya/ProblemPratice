class AgeError(Exception):
    pass


try:
    age = int(input("Enter your age: "))

    if age < 18:
        raise AgeError("You must be 18 or older to vote.")
    else:
        print("You are eligible to vote!")

except AgeError as e:
    print("Custom Exception Caught:", e)





