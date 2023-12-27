import functools as f

# In this question, You need to remove items from a list while iterating but without creating a different copy of a
# list. Remove numbers greater than 50

number_list = [10, 20, 30, 40, 50, 60, 70, 80, 90, 100]

number_list = list(filter(lambda x: x <= 50, number_list))
print(number_list)

# using higher order functions calculate the values of list in even

Input = [[1, 2, 3], [2, 4, 1]]
I = f.reduce(lambda x, y: x + y, Input)
print(I)
sorted_list = sorted(I)
print(sorted_list)

add_val = list(map(lambda i: sorted_list[i],
                   filter(lambda i: i % 2 == 0 and sorted_list[i] != 1, range(len(sorted_list)))
                   ))

print(add_val)

result = f.reduce(lambda x, y: x + y, add_val)

print(result)
