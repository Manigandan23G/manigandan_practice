from functools import reduce
from pyspark.sql.functions import *
from pyspark.sql.types import *

# If 5 is repeating in the list then get the 1st and last position
inputlist = [1, 2, 4, 5, 5, 5, 5, 6, 5, 5, 6, 7, 7, 8, 9]
search_element = 5
output = []

for i in range(0,len(inputlist)):
    if inputlist[i] == search_element and inputlist[i-1] != inputlist[i]:
            output.append(i)
    if inputlist[i] == search_element and inputlist[i+1] != inputlist[i]:
            output.append(i)
            break
print(output)
print(output[0],output[-1])


# List of list values need to convert it to single dimension list and then get the even positioned values and calcualte those values.

Input = [ [1, 2, 3], [2, 4, 1]]
I = Input[0] + Input[1]
sorted_list = sorted(I)
print(sorted_list)

add_val = list(map(lambda i: sorted_list[i], 
                        filter(lambda i: i % 2 == 0 and sorted_list[i] != 1, range(len(sorted_list)))
                   ))

print(add_val)

result = f.reduce(lambda x,y: x+y,add_val)

print(result)

#Python code to find prime number
numbers = [2, 3, 5, 7, 8, 11, 13, 14, 17, 19, 23, 29]

prime_numbers = list(filter(lambda n: all(n % i != 0 for i in range(2, n)) and n > 1, numbers))

print("Prime numbers:", prime_numbers)

#factorial program without using if-else, for, and ternary operators
number = 5
fact = 1
values = list(range(1,number+1))
def factorial(n1, n2):
    return n1*n2
l = f.reduce(factorial,values)
print(l)

#Array squared
list_value = [1,2,4,5,6,7,8]
result = list(map(lambda x: x*x,list_value))
print(result)

# Sum of every positive element
list_value = [1,-2,3,4,-6]

result = list(map(lambda x: x if x > 0 else x*0, list_value))
final_list = f.reduce(lambda x,y: x+y,result)
print(result)
print(final_list)

# Get mean of the list values
list_value = [1,2,3,4,5,6,7,8,9,10]
l = len(list_value)
# mean = sum(list_value)/l
# print(mean)
a = f.reduce(lambda x,y: x+y,list_value)
print(a)
final_result = a/l
print(round(final_result))

#The given input is a string of multiple words with a single space between each of them. Abbreviate the name and return the name initials.

a = "George Raymond Richard Martin"
b = a.split()

result = list(map(lambda x: x[0],b))
o = f.reduce(lambda x,y: x+y,result)

print(o)


#Find the difference in age between the oldest and youngest family members, and return their respective ages and the age difference.

details = [{"name": "John","age": 13,},{"name": "Mark","age": 56,},{"name": "Rachel","age": 45,},{'name': "Nate","age": 67,},{"name": "Jennifer","age": 65,},]

ages = list(map(lambda x: x["age"] ,input))

def min_max(a):
    min_val = min(a)
    max_val = max(a)
    diff_val = max_val - min_val
    l = [min_val,max_val,diff_val]
    return l
result = list(min_max(ages))
print(result)

# numeronymns 
input = "Every developer likes to mix kubernetes and javascript"

words = input.split()
r = []
result = list(map(lambda x:(x[0]+(str(len(x)-2))+x[-1]),words))
final = f.reduce(lambda x,y:x+" "+y,result)
print(final)

#Count the occurrences of distinct elements in the given 2D array. The given input is an array, the elements of which are arrays of strings. The result is an object whose property names are the values from the arrays and their value is the number of their occurrences.

input = [
  ["a", "b", "c"],
  ["c", "d", "f"],
  ["d", "f", "g"],
]
d1array = f.reduce(lambda x,y:x+y,input)
d1array = sorted(d1array)
count_array = sorted(set(list(map(lambda x:x+":"+str(d1array.count(x)),d1array))))

print(count_array)


