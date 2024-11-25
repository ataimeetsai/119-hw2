# Hints

This file is optional.
Feel free to take a look at these hints if you get stuck!

## Part 1

1. Use .flatMap:

https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.RDD.flatMap.html

2. Use .reduceByKey:

https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.RDD.reduceByKey.html

5. To compute an average, you need the sum and the count.
It will be helpful to keep track of both at once
after the map stage and when reducing.
You won't need the key k2, so you can set all keys to 1 (for example).

If you get stuck on Q5, try going back to Q4 and doing it using general_map and general_reduce. Also, try reviewing the examples
in the unit tests in Q1 and Q2.

6. The general_reduce is partitioned by the key k2.
You can use this to easily add up values for each digit.
(There should be 10 keys, one for each digit)

7. This may be the hardest question, mainly because of the
helper function!

For the helper function, to get the digits of an integer, one way to do it is just convert the integer to a string.
You may find it helpful to have a dictinoary like
DIGIT_NAMES = { "0": zero, "1": one, "2": two, ... } etc.

Once you have the helper function, the logic for q7() should
be similar to q6().

8. load_input_bigger() should have similar logic to load_input()

11. Use set(l) to get a set from a list l.

The question is a bit of a trick question! The answer should
be very simple, if you have implemented general_reduce correctly.

13.
In the description of the general_reduce function, the key
part is "will be combined (in some order) until there
are no values of that key left".

14.
Reduce is a function that basically takes two integers and returns an integer, for example
    x, y ==> x + y
If the function is like addition or multiplication, then it will
be commutative/associative, so it doesn't matter what order things are reduced in.

To get a different example, you need something that is either not commutative or not associative.

16.
You can set the parallelism to whatever you want, say,
1, 2, and 10.
reminder: the syntax is
sc.parallelize(data, parallelism)
