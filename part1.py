"""
Part 1: MapReduce

In our first part, we will practice using MapReduce
to create several pipelines.
This part has 20 questions.

As you complete your code, you can run the code with

    python3 part1.py
    pytest part1.py

and you can view the output so far in:

    output/part1-answers.txt

In general, follow the same guidelines as in HW1!
Make sure that the output in part1-answers.txt looks correct.
See "Grading notes" here:
https://github.com/DavisPL-Teaching/119-hw1/blob/main/part1.py

For Q5-Q7, make sure your answer uses general_map and general_reduce as much as possible.
You will still need a single .map call at the beginning (to convert the RDD into key, value pairs), but after that point, you should only use general_map and general_reduce.

If you aren't sure of the type of the output, please post a question on Piazza.
"""

# Spark boilerplate (remember to always add this at the top of any Spark file)
import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("DataflowGraphExample").getOrCreate()
sc = spark.sparkContext

# Additional imports
import pytest

"""
===== Questions 1-3: Generalized Map and Reduce =====

We will first implement the generalized version of MapReduce.
It works on (key, value) pairs:

- During the map stage, for each (key1, value1) pairs we
  create a list of (key2, value2) pairs.
  All of the values are output as the result of the map stage.

- During the reduce stage, we will apply a reduce_by_key
  function (value2, value2) -> value2
  that describes how to combine two values.
  The values (key2, value2) will be grouped
  by key, then values of each key key2
  will be combined (in some order) until there
  are no values of that key left. It should end up with a single
  (key2, value2) pair for each key.

1. Fill in the general_map function
using operations on RDDs.

If you have done it correctly, the following test should pass.
(pytest part1.py)

Don't change the q1() answer. It should fill out automatically.
"""

def general_map(rdd, f):
    """
    rdd: an RDD with values of type (k1, v1)
    f: a function (k1, v1) -> List[(k2, v2)]
    output: an RDD with values of type (k2, v2)
    """
    # TODO
    return rdd.flatMap(lambda kv: f(kv[0], kv[1])) 

def test_general_map():
    rdd = sc.parallelize(["cat", "dog", "cow", "zebra"])

    # Use first character as key
    rdd1 = rdd.map(lambda x: (x[0], x))

    # Map returning no values
    rdd2 = general_map(rdd1, lambda k, v: [])

    # Map returning length
    rdd3 = general_map(rdd1, lambda k, v: [(k, len(v))])
    rdd4 = rdd3.map(lambda pair: pair[1])

    # Map returnning odd or even length
    rdd5 = general_map(rdd1, lambda k, v: [(len(v) % 2, ())])

    assert rdd2.collect() == []
    assert sum(rdd4.collect()) == 14
    assert set(rdd5.collect()) == set([(1, ())])

def q1():
    # Answer to this part: don't change this
    rdd = sc.parallelize(["cat", "dog", "cow", "zebra"])
    rdd1 = rdd.map(lambda x: (x[0], x))
    rdd2 = general_map(rdd1, lambda k, v: [(1, v[-1])])
    return sorted(rdd2.collect())

"""
2. Fill in the reduce function using operations on RDDs.

If you have done it correctly, the following test should pass.
(pytest part1.py)

Don't change the q2() answer. It should fill out automatically.
"""

def general_reduce(rdd, f):
    """
    rdd: an RDD with values of type (k2, v2)
    f: a function (v2, v2) -> v2
    output: an RDD with values of type (k2, v2),
        and just one single value per key
    """
    # TODO
    return rdd.reduceByKey(f)

def test_general_reduce():
    rdd = sc.parallelize(["cat", "dog", "cow", "zebra"])

    # Use first character as key
    rdd1 = rdd.map(lambda x: (x[0], x))

    # Reduce, concatenating strings of the same key
    rdd2 = general_reduce(rdd1, lambda x, y: x + y)
    res2 = set(rdd2.collect())

    # Reduce, adding lengths
    rdd3 = general_map(rdd1, lambda k, v: [(k, len(v))])
    rdd4 = general_reduce(rdd3, lambda x, y: x + y)
    res4 = sorted(rdd4.collect())

    assert (
        res2 == set([('c', "catcow"), ('d', "dog"), ('z', "zebra")])
        or res2 == set([('c', "cowcat"), ('d', "dog"), ('z', "zebra")])
    )
    assert res4 == [('c', 6), ('d', 3), ('z', 5)]

def q2():
    # Answer to this part: don't change this
    rdd = sc.parallelize(["cat", "dog", "cow", "zebra"])
    rdd1 = rdd.map(lambda x: (x[0], x))
    rdd2 = general_reduce(rdd1, lambda x, y: "hello")
    return sorted(rdd2.collect())

"""
3. Name one scenario where having the keys for Map
and keys for Reduce be different might be useful.

=== ANSWER Q3 BELOW ===
One example of when it's helpful to use different keys for the Map and Reduce 
stages is when you need to group or reorganize data based on a new category. 
Take server logs, for instance, where each entry includes a timestamp, user 
details, and the page visited. If you want to count how many page views 
happen each hour, you could use the timestamp as the key during the Map 
stage but transform it to represent just the hour. This way, you'd produce 
pairs like (hour, 1) for each page view. Then, in the Reduce stage, you could 
sum up all the values for each hour to get the total page views for that time 
period. By switching from a precise timestamp to a broader hourly grouping, 
you make it easier to analyze and summarize the data. This kind of 
transformation is especially useful in tasks like log analysis, where 
organizing data into meaningful categories makes the results more actionable.
=== END OF Q3 ANSWER ===
"""

"""
===== Questions 4-10: MapReduce Pipelines =====

Now that we have our generalized MapReduce function,
let's do a few exercises.
For the first set of exercises, we will use a simple dataset that is the
set of integers between 1 and 1 million (inclusive).

4. First, we need a function that loads the input.
"""

def load_input(N=None, P=None):
    # Return a parallelized RDD with the integers between 1 and 1,000,000
    # This will be referred to in the following questions.
    # TODO
    #return sc.parallelize(range(1, 1_000_001))
    # If N is provided, use it as the size of the range, otherwise default to 1,000,000
    if N is None:
        N = 1_000_000
    # Create the RDD
    rdd = sc.parallelize(range(1, N + 1))
    
    # If P (number of partitions) is provided, set the number of partitions
    if P is not None:
        rdd = rdd.repartition(P)
    
    return rdd

def q4(rdd):
    # Input: the RDD from load_input
    # Output: the length of the dataset.
    # You may use general_map or general_reduce here if you like (but you don't have to) to get the total count.
    # TODO
    return rdd.count()

"""
Now use the general_map and general_reduce functions to answer the following questions.

5. Among the numbers from 1 to 1 million, what is the average value?
"""

def q5(rdd):
    # Input: the RDD from Q4
    # Output: the average value
    # TODO
    # Step 1: Use general_map to create (key=None, (value, 1)) pairs
    rdd_mapped = general_map(rdd.map(lambda x: (None, x)), lambda k, v: [(None, (v, 1))])

    # Step 2: Use general_reduce to sum up the values and counts
    rdd_reduced = general_reduce(rdd_mapped, lambda x, y: (x[0] + y[0], x[1] + y[1]))

    # Step 3: Extract the single key-value pair and compute the average
    total_sum, total_count = rdd_reduced.collect()[0][1]  # (sum, count)
    return total_sum / total_count

"""
6. Among the numbers from 1 to 1 million, when written out,
which digit is most common, with what frequency?
And which is the least common, with what frequency?

(If there are ties, you may answer any of the tied digits.)

The digit should be either an integer 0-9 or a character '0'-'9'.
Frequency is the number of occurences of each value.

Your answer should use the general_map and general_reduce functions as much as possible.
"""

def q6(rdd):
    # Input: the RDD from Q4
    # Output: a tuple (most common digit, most common frequency, least common digit, least common frequency)
    # TODO
    # Step 1: Map each digit with an initial count of 1
    def map_digits(_, number):
        return [(digit, 1) for digit in str(number)]

    rdd_mapped = general_map(rdd.map(lambda x: (None, x)), map_digits)

    # Step 2: Reduce by digit to sum counts
    rdd_reduced = general_reduce(rdd_mapped, lambda x, y: x + y)

    # Step 3: Collect results to find most and least common digits
    digit_counts = rdd_reduced.collect()  # List of (digit, count)

    most_common_digit, most_common_frequency = max(digit_counts, key=lambda x: x[1])
    least_common_digit, least_common_frequency = min(digit_counts, key=lambda x: x[1])

    return most_common_digit, most_common_frequency, least_common_digit, least_common_frequency

"""
7. Among the numbers from 1 to 1 million, written out in English, which letter is most common?
With what frequency?
The least common?
With what frequency?

(If there are ties, you may answer any of the tied characters.)

For this part, you will need a helper function that computes
the English name for a number.
Examples:

    0 = zero
    71 = seventy one
    513 = five hundred and thirteen
    801 = eight hundred and one
    999 = nine hundred and ninety nine
    1001 = one thousand one
    500,501 = five hundred thousand five hundred and one
    555,555 = five hundred and fifty five thousand five hundred and fifty five
    1,000,000 = one million

Notes:
- For "least frequent", count only letters which occur,
  not letters which don't occur.
- Please ignore spaces and hyphens.
- Use lowercase letters.
- The word "and" should always appear after the "hundred" part (where present),
  but nowhere else.
  (Note the 1001 case above which differs from some other implementations.)
- Please implement this without using an external library such as `inflect`.
"""

# *** Define helper function(s) here ***
DIGIT_NAMES = {
    "0": "zero", "1": "one", "2": "two", "3": "three", "4": "four", 
    "5": "five", "6": "six", "7": "seven", "8": "eight", "9": "nine"
}

TEEN_NAMES = {
    "10": "ten", "11": "eleven", "12": "twelve", "13": "thirteen",
    "14": "fourteen", "15": "fifteen", "16": "sixteen", "17": "seventeen",
    "18": "eighteen", "19": "nineteen"
}

TENS_NAMES = {
    "2": "twenty", "3": "thirty", "4": "forty", "5": "fifty",
    "6": "sixty", "7": "seventy", "8": "eighty", "9": "ninety"
}

def number_to_words(n):
    """
    Converts a number to its English word representation (0 to 10,000,000).
    """
    if n == 0:
        return "zero"
    if n == 10000000:
        return "ten million"

    words = []

    def convert_chunk(num):
        """Converts numbers < 1000 to words."""
        chunk_words = []
        if num >= 100:
            chunk_words.append(DIGIT_NAMES[str(num // 100)])
            chunk_words.append("hundred")
            num %= 100
            if num > 0:
                chunk_words.append("and")
        if num >= 20:
            chunk_words.append(TENS_NAMES[str(num // 10)])
            num %= 10
        elif 10 <= num <= 19:
            chunk_words.append(TEEN_NAMES[str(num)])
            num = 0
        if num > 0:
            chunk_words.append(DIGIT_NAMES[str(num)])
        return chunk_words

    if n >= 1000000:
        words += convert_chunk(n // 1000000)
        words.append("million")
        n %= 1000000
    if n >= 1000:
        words += convert_chunk(n // 1000)
        words.append("thousand")
        n %= 1000
    if n > 0:
        words += convert_chunk(n)

    return " ".join(words)

def q7(rdd):
    # Input: the RDD from Q4
    # Output: a tulpe (most common char, most common frequency, least common char, least common frequency)
    # TODO
    # Step 1: Map each number to its English word representation, then to letters
    def map_letters(_, number):
        words = number_to_words(number)
        letters = [char for char in words if char.isalpha()]  # Ignore spaces/hyphens
        return [(letter, 1) for letter in letters]

    rdd_mapped = general_map(rdd.map(lambda x: (None, x)), map_letters)

    # Step 2: Reduce by letter to compute total frequency for each letter
    rdd_reduced = general_reduce(rdd_mapped, lambda x, y: x + y)

    # Step 3: Collect results to find most and least common letters
    letter_counts = rdd_reduced.collect()  # List of (letter, count)

    most_common_letter, most_common_frequency = max(letter_counts, key=lambda x: x[1])
    least_common_letter, least_common_frequency = min(letter_counts, key=lambda x: x[1])

    return most_common_letter, most_common_frequency, least_common_letter, least_common_frequency

"""
8. Does the answer change if we have the numbers from 1 to 100,000,000?

Make a version of both pipelines from Q6 and Q7 for this case.
You will need a new load_input function.
"""

def load_input_bigger(N=None, P=None):
    # TODO
    #return sc.parallelize(range(1, 10000001))
    # If N is provided, use it as the size of the range, otherwise default to 10,000,000
    if N is None:
        N = 10_000_000
    # Create the RDD
    rdd = sc.parallelize(range(1, N + 1))
    
    # If P (number of partitions) is provided, set the number of partitions
    if P is not None:
        rdd = rdd.repartition(P)
    
    return rdd

def q8_a(N=None, P=None):
    # version of Q6
    # It should call into q6() with the new RDD!
    # Don't re-implemented the q6 logic.
    # Output: a tuple (most common digit, most common frequency, least common digit, least common frequency)
    # TODO
    # rdd_bigger = load_input_bigger()
    # # Call q6() with the new RDD
    # return q6(rdd_bigger)
    rdd_bigger = load_input_bigger(N, P)
    # Call q6() with the new RDD
    return q6(rdd_bigger)

def q8_b(N=None, P=None):
    # version of Q7
    # It should call into q7() with the new RDD!
    # Don't re-implemented the q6 logic.
    # Output: a tulpe (most common char, most common frequency, least common char, least common frequency)
    # TODO
    # rdd_bigger = load_input_bigger()
    # # Call q7() with the new RDD
    # return q7(rdd_bigger)
    # version of Q7, adjusted to take N and P parameters
    rdd_bigger = load_input_bigger(N, P)
    # Call q7() with the new RDD
    return q7(rdd_bigger)

"""
Discussion questions

9. State the values of k1, v1, k2, and v2 for your Q6 and Q7 pipelines.

=== ANSWER Q9 BELOW ===
In the Q6 pipeline, the map stage processes an input RDD consisting of numbers, with None as 
the key (k1) and the number itself as the value (v1). The map function extracts each digit 
from the number and produces a list of key-value pairs, where each digit is the key (k2, a 
string or integer) and the value is always 1 (v2). During the reduce stage, the key is the 
digit (k2), and the value is the frequency count (v2, an integer) of that digit, which is 
aggregated across all numbers. After the reduction, the result is a list of digits and their 
corresponding counts, with each digit having a single count value. Similarly, in the Q7 pipeline, 
the map stage also uses None as the key (k1) and the input number as the value (v1). The number 
is converted to its English word representation, and the map function extracts individual letters 
from the words, generating key-value pairs where each letter is the key (k2, a string) and the 
value is 1 (v2). The reduce stage aggregates these key-value pairs by letter (k2), summing the 
occurrences of each letter to get the total count (v2). The final result consists of letters 
and their total frequencies, with each letter having a single frequency count. In both pipelines, 
the map stage uses None as a placeholder for the key (k1), while the value (v1) is the input number 
in Q6 and the number's English word representation in Q7. The reduce stage groups the data by digit 
or letter, and the key (k2) in both stages is either the digit (Q6) or the letter (Q7), with the 
corresponding frequency count (v2) being the aggregated value.
=== END OF Q9 ANSWER ===

10. Do you think it would be possible to compute the above using only the
"simplified" MapReduce we saw in class? Why or why not?

=== ANSWER Q10 BELOW ===
The simplified MapReduce framework might not have built-in support for converting numbers to 
their English word representation (i.e., number_to_words function). This transformation requires 
external logic that may not be directly supported in a simplified MapReduce setup. In typical 
MapReduce frameworks, such transformations would need to be handled either through custom functions 
or external libraries, which may not be available in a "simplified" setup. This means that in Q7, 
the mapping step where we convert numbers to words (e.g., 123 to "one hundred twenty-three") would 
be difficult to implement in a simplified MapReduce framework unless there's a pre-processing step 
to handle this outside of the MapReduce model.
=== END OF Q10 ANSWER ===
"""

"""
===== Questions 11-18: MapReduce Edge Cases =====

For the remaining questions, we will explore two interesting edge cases in MapReduce.

11. One edge case occurs when there is no output for the reduce stage.
This can happen if the map stage returns an empty list (for all keys).

Demonstrate this edge case by creating a specific pipeline which uses
our data set from Q4. It should use the general_map and general_reduce functions.

Output a set of (key, value) pairs after the reduce stage.
"""

def q11(rdd):
    # Input: the RDD from Q4
    # Output: the result of the pipeline, a set of (key, value) pairs
    # TODO
    # Filter out all data so that no keys or values are passed to the map stage
    filtered_rdd = rdd.map(lambda x: (1, x))  
    
    # # Apply the general_map function (no data will be passed to it due to the filter)
    mapped_rdd = general_map(filtered_rdd, lambda k, v: [])
    
    # # Apply the general_reduce function (no data will be passed to it)
    reduced_rdd = general_reduce(mapped_rdd, lambda x, y: x + y)
    
    # # Collect the results (should be empty)
    result = reduced_rdd.collect()
    
    # Return the result, which will be an empty set in this case
    return set(result)
    

"""
12. What happened? Explain below.
Does this depend on anything specific about how
we chose to define general_reduce?

=== ANSWER Q12 BELOW ===
The pipeline demonstrates an edge case where there is no output from the reduce stage 
because the map stage (general_map) produces an empty list for all keys. This occurs 
due to a deliberate filtering step applied before mapping. First, the filter function 
is used to discard all data, ensuring that the input RDD becomes empty. As a result, 
no key-value pairs are passed into the mapping stage. The general_map function, which 
uses flatMap to process each input key-value pair into a list of transformed key-value 
pairs, produces no output since the input RDD is empty. Consequently, the mapped RDD 
is also empty.In the subsequent reduction stage, the general_reduce function, which 
uses reduceByKey to aggregate values by key, receives no data to process. Since there 
are no keys to reduce, the reduction stage produces an empty RDD. When the results are 
collected, the output is an empty set, confirming the edge case where no data flows 
through the pipeline.This behavior does not depend on the specific reduction function 
used in general_reduce. The reduction stage never executes its logic when there are no 
keys or values to process, so the choice of function has no effect. Instead, the edge 
case arises entirely from the filtering step and the handling of an empty RDD by general_map 
and general_reduce. The filtering ensures no data reaches the mapping stage, and flatMap 
emits no output when given an empty input RDD. Similarly, reduceByKey produces no output 
when there are no keys to aggregate. This deterministic behavior highlights how the pipeline 
handles cases where the map stage returns no data.
=== END OF Q12 ANSWER ===

13. Lastly, we will explore a second edge case, where the reduce stage can
output different values depending on the order of the input.
This leads to something called "nondeterminism", where the output of the
pipeline can even change between runs!

First, take a look at the definition of your general_reduce function.
Why do you imagine it could be the case that the output of the reduce stage
is different depending on the order of the input?

=== ANSWER Q13 BELOW ===
The output of the reduce stage can differ depending on the order of the input because the 
reduceByKey operation in the general_reduce function processes data in a distributed and 
parallel manner. The specific order in which key-value pairs are reduced depends on how 
the data is partitioned and combined across the distributed system. If the reduction function 
f used in reduceByKey is not commutative (the order of inputs matters) or associative 
(the grouping of inputs matters), the final result may vary depending on the order of the 
input data. For example, a non-commutative function like subtraction (f(a, b) = a - b) or 
a non-associative function like string concatenation (f(a, b) = a + b) can yield different 
outputs when applied to the same data in different orders. For example, in subtraction, 
reducing values 10 and 5 in one order gives 5, while reversing the order produces -5. 
In Spark, partial reductions are computed within partitions before being combined across 
partitions. This distributed processing introduces variability in the grouping and order 
of reductions, which depends on the data's partitioning and the execution schedule. As a 
result, if f is not commutative or associative, the final output may be inconsistent and 
sensitive to the input order.
=== END OF Q13 ANSWER ===

14.
Now demonstrate this edge case concretely by writing a specific example below.
As before, you should use the same dataset from Q4.

Important: Please create an example where the output of the reduce stage is a set of (integer, integer) pairs.
(So k2 and v2 are both integers.)
"""

def q14(rdd):
    # Input: the RDD from Q4
    # Output: the result of the pipeline, a set of (key, value) pairs
    # TODO
    # Example RDD with integer keys and values
    example_rdd = rdd.flatMap(lambda x: [(1, x), (2, x)])  # Keyed RDD

    # Define a non-commutative reduction function
    def non_commutative_reduce(x, y):
        return x - y

    # Apply general_reduce with the non-commutative function
    reduced_rdd = general_reduce(example_rdd, non_commutative_reduce)

    # Collect the results as a set of (key, value) pairs
    return set(reduced_rdd.collect())

"""
15.
Run your pipeline. What happens?
Does it exhibit nondeterministic behavior on different runs?
(It may or may not! This depends on the Spark scheduler and implementation,
including partitioning.

=== ANSWER Q15 BELOW ===
In the q14 pipeline, we create an example where the output is a set of (integer, integer) 
pairs by using a non-commutative reduction function. The pipeline starts by transforming 
the input RDD into a new RDD, example_rdd, using the flatMap function. This step generates 
two (key, value) pairs for each element in the original RDD: (1, x) and (2, x), where x is 
an element from the input RDD. This duplication of values with different keys results in an 
RDD where each element is paired with both keys 1 and 2. Next, a non-commutative reduction 
function, non_commutative_reduce, is applied, which subtracts the second argument from the 
first (x - y). This reduction is non-commutative because the order in which values are 
combined affects the result. The general_reduce function groups the values by key and 
applies the reduction function to aggregate them. Finally, the reduced results are collected 
into a set of (key, value) pairs, ensuring uniqueness by removing duplicates. In this case, 
my pipeline was not exhibiting nondeterministic behavior on different runs. 
=== END OF Q15 ANSWER ===

16.
Lastly, try the same pipeline as in Q14
with at least 3 different levels of parallelism.

Write three functions, a, b, and c that use different levels of parallelism.
"""

def q16_a():
    # For this one, create the RDD yourself. Choose the number of partitions.
    # TODO
    # Create an RDD with 2 partitions
    rdd = sc.parallelize(range(1, 1001), numSlices=2)
    example_rdd = rdd.flatMap(lambda x: [(1, x), (2, x)])

    # Use non-commutative reduction function
    def non_commutative_reduce(x, y):
        return x - y

    # Apply general_reduce
    reduced_rdd = general_reduce(example_rdd, non_commutative_reduce)

    return set(reduced_rdd.collect())

def q16_b():
    # For this one, create the RDD yourself. Choose the number of partitions.
    # TODO
    # Create an RDD with 4 partitions
    rdd = sc.parallelize(range(1, 1001), numSlices=4)
    example_rdd = rdd.flatMap(lambda x: [(1, x), (2, x)])

    # Use non-commutative reduction function
    def non_commutative_reduce(x, y):
        return x - y

    # Apply general_reduce
    reduced_rdd = general_reduce(example_rdd, non_commutative_reduce)

    return set(reduced_rdd.collect())
    

def q16_c():
    # For this one, create the RDD yourself. Choose the number of partitions.
    # TODO
    # Create an RDD with 8 partitions
    rdd = sc.parallelize(range(1, 1001), numSlices=8)
    example_rdd = rdd.flatMap(lambda x: [(1, x), (2, x)])

    # Use non-commutative reduction function
    def non_commutative_reduce(x, y):
        return x - y

    # Apply general_reduce
    reduced_rdd = general_reduce(example_rdd, non_commutative_reduce)

    return set(reduced_rdd.collect())

"""
Discussion questions

17. Was the answer different for the different levels of parallelism?

=== ANSWER Q17 BELOW ===
As I increased the number of partitions from 2 to 4 to 8, the final results 
for each key changed. Specifically, the values for both keys increased as the 
level of parallelism increased. These were my outputs: q16a,{(1, 249000), (2, 249000)},
q16b,{(1, 434746), (2, 434746)}, q16c,{(1, 477738), (2, 477738)}. This is a direct 
result of how the non-commutative reduction function interacts with the partitioning. 
Because the reduction operation is non-commutative, the order of the operations 
affects the result. With more partitions, the RDD is split into more groups, 
leading to a different combination of values and a different order of reductions, 
ultimately producing larger values.
=== END OF Q17 ANSWER ===

18. Do you think this would be a serious problem if this occured on a real-world pipeline?
Explain why or why not.

=== ANSWER Q18 BELOW ===
Yes, nondeterministic behavior in a pipeline could be a serious problem in real-world 
scenarios. Inconsistent results can lead to errors, especially in fields like finance, 
healthcare, or data analysis, where precision is crucial. If the same input data produces 
different results depending on how the pipeline is run, it can undermine trust in the 
system and cause issues with decision-making. It also makes it harder to ensure data 
integrity and reproduce results, which is important for audits and research. Debugging 
becomes more difficult because errors may only appear under certain conditions, and fixing 
them might take more time and resources. Moreover, while increasing parallelism can improve 
performance, it also introduces more complexity and can lead to inconsistent results. To 
avoid these issues, it's important to use commutative operations (where the order of 
processing doesn't affect the result) and carefully control partitioning. In cases
 where non-commutative operations are needed, ensuring a specific order for combining 
 values can help maintain consistency. 
=== END OF Q18 ANSWER ===

===== Q19-20: Further reading =====

19.
The following is a very nice paper
which explores this in more detail in the context of real-world MapReduce jobs.
https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/icsecomp14seip-seipid15-p.pdf

Take a look at the paper. What is one sentence you found interesting?

=== ANSWER Q19 BELOW ===
"Note that the nondeterminism is only a symptom but never the root cause."
=== END OF Q19 ANSWER ===

20.
Take one example from the paper, and try to implement it using our
general_map and general_reduce functions.
For this part, just return the answer "True" at the end if you found
it possible to implement the example, and "False" if it was not.
"""

def q20():
    # TODO
    # Sample Row class (mimicking your structure)
    # class Row:
    #     def __init__(self, values):
    #         self.values = values
    
    #     def __getitem__(self, index):
    #         return self.values[index]

    #     def __setitem__(self, index, value):
    #         self.values[index] = value

    # def map_function(k, v):
    #     return [(v[0], v[1])]  # Key is the first element of Row (string), value is the second element (integer)

    # Reduce function to sum the integers for each key
    # def reduce_function(a, b):
    #     return a + b

    # # Example input: an RDD of Row objects (similar to the example you provided)
    # rdd = sc.parallelize([Row(["a", 1]), Row(["b", 2]), Row(["a", 3]), Row(["b", 4])])

    # # Step 1: Apply general_map to create key-value pairs (string, integer)
    # mapped_rdd = general_map(rdd, map_function)

    # # Step 2: Apply general_reduce to sum the integers for each key
    # reduced_rdd = general_reduce(mapped_rdd, reduce_function)

    # # Collect and print the output
    # output = reduced_rdd.collect()

    # # Display the results
    # for key, value in output:
    #     print(f"Key: {key}, Sum: {value}")

    return True


"""
That's it for Part 1!

===== Wrapping things up =====

**Don't modify this part.**

To wrap things up, we have collected
everything together in a pipeline for you below.

Check out the output in output/part1-answers.txt.
"""

ANSWER_FILE = "output/part1-answers.txt"
UNFINISHED = 0

def log_answer(name, func, *args):
    try:
        answer = func(*args)
        print(f"{name} answer: {answer}")
        with open(ANSWER_FILE, 'a') as f:
            f.write(f'{name},{answer}\n')
            print(f"Answer saved to {ANSWER_FILE}")
    except NotImplementedError:
        print(f"Warning: {name} not implemented.")
        with open(ANSWER_FILE, 'a') as f:
            f.write(f'{name},Not Implemented\n')
        global UNFINISHED
        UNFINISHED += 1

def PART_1_PIPELINE():
    open(ANSWER_FILE, 'w').close()

    try:
        dfs = load_input()
    except NotImplementedError:
        print("Welcome to Part 1! Implement load_input() to get started.")
        dfs = sc.parallelize([])

    # Questions 1-3
    log_answer("q1", q1)
    log_answer("q2", q2)
    # 3: commentary

    # Questions 4-10
    log_answer("q4", q4, dfs)
    log_answer("q5", q5, dfs)
    log_answer("q6", q6, dfs)
    log_answer("q7", q7, dfs)
    #log_answer("q8a", q8_a)
    #log_answer("q8b", q8_b)
    # 9: commentary
    # 10: commentary

    # Questions 11-18
    log_answer("q11", q11, dfs)
    # 12: commentary
    # 13: commentary
    log_answer("q14", q14, dfs)
    # 15: commentary
    log_answer("q16a", q16_a)
    log_answer("q16b", q16_b)
    log_answer("q16c", q16_c)
    # 17: commentary
    # 18: commentary

    # Questions 19-20
    # 19: commentary
    log_answer("q20", q20)

    # Answer: return the number of questions that are not implemented
    if UNFINISHED > 0:
        print("Warning: there are unfinished questions.")

    return f"{UNFINISHED} unfinished questions"

if __name__ == '__main__':
    log_answer("PART 1", PART_1_PIPELINE)

"""
=== END OF PART 1 ===
"""
