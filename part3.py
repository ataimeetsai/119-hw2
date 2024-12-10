"""
Part 3: Measuring Performance

Now that you have drawn the dataflow graph in part 2,
this part will explore how performance of real pipelines
can differ from the theoretical model of dataflow graphs.

We will measure the performance of your pipeline
using your ThroughputHelper and LatencyHelper from HW1.

=== Coding part 1: making the input size and partitioning configurable ===

We would like to measure the throughput and latency of your PART1_PIPELINE,
but first, we need to make it configurable in:
(i) the input size
(ii) the level of parallelism.

Currently, your pipeline should have two inputs, load_input() and load_input_bigger().
You will need to change part1 by making the following additions:

- Make load_input and load_input_bigger take arguments that can be None, like this:

    def load_input(N=None, P=None)

    def load_input_bigger(N=None, P=None)

You will also need to do the same thing to q8_a and q8_b:

    def q8_a(N=None, P=None)

    def q8_b(N=None, P=None)

Here, the argument N = None is an optional parameter that, if specified, gives the size of the input
to be considered, and P = None is an optional parameter that, if specifed, gives the level of parallelism
(number of partitions) in the RDD.

You will need to make both functions work with the new signatures.
Be careful to check that the above changes should preserve the existing functionality of part1
(so python3 part1.py should still give the same output as before!)

Don't make any other changes to the function sigatures.

Once this is done, define a *new* version of the PART_1_PIPELINE, below,
that takes as input the parameters N and P.
(This time, you don't have to consider the None case.)
You should not modify the existing PART_1_PIPELINE.

You may either delete the parts of the code that save the output file, or change these to a different output file like part1-answers-temp.txt.
"""
# Spark boilerplate (remember to always add this at the top of any Spark file)
import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("DataflowGraphExample").getOrCreate()
sc = spark.sparkContext
from part1 import q1, q2, q4, q5, q6, q14, q16_a, q16_b, q16_c, q20, general_map, general_reduce

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
    

# def PART_1_PIPELINE_PARAMETRIC(N, P):
#     """
#     TODO: Follow the same logic as PART_1_PIPELINE
#     N = number of inputs
#     P = parallelism (number of partitions)
#     (You can copy the code here), but make the following changes:
#     - load_input should use an input of size N.
#     - load_input_bigger (including q8_a and q8_b) should use an input of size N.
#     - both of these should return an RDD with level of parallelism P (number of partitions = P).
#     """

def PART_1_PIPELINE_PARAMETRIC(N, P):
    dfs = load_input(N=N, P=P)

    result_q1 = q1
    result_q2 = q2
    result_q4 = q4(dfs)
    result_q5 = q5(dfs)
    result_q6 = q6(dfs)
    result_q7 = q7(dfs)
    result_q8a = q8_a(N, P)
    result_q8b = q8_b(N, P)
    result_q11 = q11(dfs)
    result_q14 = q14(dfs)
    result_q16a = q16_a
    result_q16b = q16_b
    result_q16c = q16_c
    result_q20 = q20


"""
=== Coding part 2: measuring the throughput and latency ===

Now we are ready to measure the throughput and latency.

To start, copy the code for ThroughputHelper and LatencyHelper from HW1 into this file.

Then, please measure the performance of PART1_PIPELINE as a whole
using five levels of parallelism:
- parallelism 1
- parallelism 2
- parallelism 4
- parallelism 8
- parallelism 16

For each level of parallelism, you should measure the throughput and latency as the number of input
items increases, using the following input sizes:
- N = 1, 10, 100, 1000, 10_000, 100_000, 1_000_000.

- Note that the larger sizes may take a while to run (for example, up to 30 minutes). You can try with smaller sizes to test your code first.

You can generate any plots you like (for example, a bar chart or an x-y plot on a log scale,)
but store them in the following 10 files,
where the file name corresponds to the level of parallelism:

output/part3-throughput-1.png
output/part3-throughput-2.png
output/part3-throughput-4.png
output/part3-throughput-8.png
output/part3-throughput-16.png
output/part3-latency-1.png
output/part3-latency-2.png
output/part3-latency-4.png
output/part3-latency-8.png
output/part3-latency-16.png

Clarifying notes:

- To control the level of parallelism, use the N, P parameters in your PART_1_PIPELINE_PARAMETRIC above.

- Make sure you sanity check the output to see if it matches what you would expect! The pipeline should run slower
  for larger input sizes N (in general) and for fewer number of partitions P (in general).

- For throughput, the "number of input items" should be 2 * N -- that is, N input items for load_input, and N for load_input_bigger.

- For latency, please measure the performance of the code on the entire input dataset
(rather than a single input row as we did on HW1).
MapReduce is batch processing, so all input rows are processed as a batch
and the latency of any individual input row is the same as the latency of the entire dataset.
That is why we are assuming the latency will just be the running time of the entire dataset.

- Set `NUM_RUNS` to `1` if you haven't already. Note that this will make the values for low numbers (like `N=1`, `N=10`, and `N=100`) vary quite unpredictably.
"""

# Copy in ThroughputHelper and LatencyHelper

import timeit 
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns

NUM_RUNS = 1

class ThroughputHelper:
    def __init__(self):
        # Initialize the object.
        # Pipelines: a list of functions, where each function
        # can be run on no arguments.
        # (like: def f(): ... )
        self.pipelines = []

        # Pipeline names
        # A list of names for each pipeline
        self.names = []

        # Pipeline input sizes
        self.sizes = []

        # Pipeline throughputs
        # This is set to None, but will be set to a list after throughputs
        # are calculated.
        self.throughputs = None

    def add_pipeline(self, name, size, func):
        self.pipelines.append(func)  # Add the function to the list
        self.names.append(name)       # Add the name to the list
        self.sizes.append(size)  

    def compare_throughput(self):
        # Measure the throughput of all pipelines
        # and store it in a list in self.throughputs.
        # Make sure to use the NUM_RUNS variable.
        # Also, return the resulting list of throughputs,
        # in **number of items per second.**
        self.throughputs = []  # Reset the throughput list
        
        for size, pipeline, name in zip(self.sizes, self.pipelines, self.names):
            # Get the number of items processed by the pipeline
            num_items = size  # Assuming size is the number of items in the dataset
            # Define a function to run the pipeline
            def f():
                pipeline()  # Run the pipeline

            # Measure execution time using timeit
            execution_time = timeit.timeit(f, number=NUM_RUNS)

            # Calculate throughput as number of items processed per second
            throughput = num_items * NUM_RUNS / execution_time

            # Append throughput to the list
            self.throughputs.append(throughput)

            # Print throughput for this pipeline
            print(f"Throughput for {name}: {int(throughput)} items/second")
        return self.throughputs



class LatencyHelper:
    def __init__(self):
        # Initialize the object.
        # Pipelines: a list of functions, where each function
        # can be run on no arguments.
        # (like: def f(): ... )
        self.pipelines = []

        # Pipeline names
        # A list of names for each pipeline
        self.names = []

        # Pipeline latencies
        # This is set to None, but will be set to a list after latencies
        # are calculated.
        self.latencies = None

    def add_pipeline(self, name, func):
        self.pipelines.append(func)
        self.names.append(name)

    def compare_latency(self):
        # Measure the latency of all pipelines
        # and store it in a list in self.latencies.
        # Also, return the resulting list of latencies,
        # in **milliseconds.**
        latencies = []
        for pipeline in self.pipelines:
        # Run the pipeline multiple times and measure the time taken
            execution_time = timeit.timeit(lambda: pipeline(), number=NUM_RUNS)  # Use a lambda to call the pipeline
        
            # Calculate average latency in milliseconds
            latency = (execution_time / NUM_RUNS) * 1000  # Convert to milliseconds
            latencies.append(latency)
        
        self.latencies = latencies
        return latencies  # Return list of latencies in milliseconds

"""
=== Reflection part ===

Once this is done, write a reflection and save it in
a text file, output/part3-reflection.txt.

I would like you to think about and answer the following questions:

1. What would we expect from the throughput and latency
of the pipeline, given only the dataflow graph?

Use the information we have seen in class. In particular,
how should throughput and latency change when we double the amount of parallelism?

Please ignore pipeline and task parallelism for this question.
The parallelism we care about here is data parallelism.

2. In practice, does the expectation from question 1
match the performance you see on the actual measurements? Why or why not?

State specific numbers! What was the expected throughput and what was the observed?
What was the expected latency and what was the observed?

3. Finally, use your answers to Q1-Q2 to form a conjecture
as to what differences there are (large or small) between
the theoretical model and the actual runtime.
Name some overheads that might be present in the pipeline
that are not accounted for by our theoretical model of
dataflow graphs that could affect performance.

You should list an explicit conjecture in your reflection, like this:

    Conjecture: I conjecture that ....

You may have different conjectures for different parallelism cases.
For example, for the parallelism=4 case vs the parallelism=16 case,
if you believe that different overheads are relevant for these different scenarios.

=== Grading notes ===

Don't forget to fill out the entrypoint below before submitting your code!
Running python3 part3.py should work and should re-generate all of your plots in output/.

In the reflection, please write at least a paragraph for each question. (5 sentences each)

Please include specific numbers in your reflection (particularly for Q2).

=== Entrypoint ===
"""

def measure_performance():
    NUM_RUNS = 1
    input_sizes = [1, 10, 100, 1000, 10000, 100000, 1000000]
    parallelism_levels = [1, 2, 4, 8, 16]
    
    for parallelism in parallelism_levels:
        throughput_data = []
        latency_data = []
        for N in input_sizes:
            # Measure throughput for current parallelism and input size
            def throughput_func():
                PART_1_PIPELINE_PARAMETRIC(N, parallelism)
            execution_time = timeit.timeit(throughput_func, number=NUM_RUNS)
            throughput = N * NUM_RUNS / execution_time
            throughput_data.append([parallelism, N, throughput])
            
            # Measure latency for current parallelism and input size
            def latency_func():
                PART_1_PIPELINE_PARAMETRIC(N, parallelism)
            execution_time = timeit.timeit(latency_func, number=NUM_RUNS)
            latency = (execution_time / NUM_RUNS) * 1000  # Convert to milliseconds
            latency_data.append([parallelism, N, latency])
    
    # Convert data to DataFrames
        throughput_df = pd.DataFrame(throughput_data, columns=['Parallelism', 'Input Size', 'Throughput'])
        latency_df = pd.DataFrame(latency_data, columns=['Parallelism', 'Input Size', 'Latency'])

    # Plotting the Throughput data using Seaborn
        sns.set(style="whitegrid")
        throughput_plot = sns.lineplot(data=throughput_df, x='Input Size', y='Throughput', hue='Parallelism', marker='o')
        throughput_plot.set(xscale='log', yscale='log', title="Throughput vs Input Size for Different Parallelism Levels")
        throughput_plot.set(xlabel='Input Size (N)', ylabel='Throughput (items/sec)')
        throughput_plot.get_figure().savefig(f'output/part3-throughput-{parallelism}.png', bbox_inches='tight')
        throughput_plot.get_figure().clear()  # Clear figure to avoid memory issues

    # Plotting the Latency data using Seaborn
        latency_plot = sns.lineplot(data=latency_df, x='Input Size', y='Latency', hue='Parallelism', marker='o')
        latency_plot.set(xscale='log', yscale='log', title="Latency vs Input Size for Different Parallelism Levels")
        latency_plot.set(xlabel='Input Size (N)', ylabel='Latency (ms)')
        latency_plot.get_figure().savefig(f'output/part3-latency-{parallelism}.png', bbox_inches='tight')
        latency_plot.get_figure().clear()  # Clear figure to avoid memory issues

# Run the performance measurement
if __name__ == '__main__':
    measure_performance()

