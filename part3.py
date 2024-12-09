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
from part1 import load_input, load_input_bigger, q1, q2, q4, q5, q6, q7, q8_a, q8_b, q11, q14, q16_a, q16_b, q16_c, q20

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

ANSWER_FILE = "output/part1-answers-temp.txt"
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

def PART_1_PIPELINE_PARAMETRIC(N, P):
    """
    TODO: Follow the same logic as PART_1_PIPELINE
    N = number of inputs
    P = parallelism (number of partitions)
    (You can copy the code here), but make the following changes:
    - load_input should use an input of size N.
    - load_input_bigger (including q8_a and q8_b) should use an input of size N.
    - both of these should return an RDD with level of parallelism P (number of partitions = P).
    """
    # Initialize the output file
    open(ANSWER_FILE, 'w').close()

    # Load the input with the specified N (size) and P (parallelism)
    try:
        dfs = load_input(N=N, P=P)
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
    log_answer("q8a", q8_a, N=N, P=P)  # Use N and P for q8a
    log_answer("q8b", q8_b, N=N, P=P)  # Use N and P for q8b
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

    def generate_plot(self, filename):
        # Generate a plot for throughput using matplotlib.
        # You can use any plot you like, but a bar chart probably makes
        # the most sense.
        # Make sure you include a legend.
        # Save the result in the filename provided.
        plt.figure(figsize=(10, 6))
        plt.bar(self.names, self.throughputs, color='skyblue')
        plt.xlabel('Pipelines')
        plt.ylabel('Throughput (items/sec)')
        plt.title('Throughput of Different Pipelines')
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.legend(['Throughput'])
        plt.savefig(filename)  # Save the plot
        plt.close()  # Close the plot to free up memory


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

    def generate_plot(self, filename):
        # Generate a plot for latency using matplotlib.
        # You can use any plot you like, but a bar chart probably makes
        # the most sense.
        # Make sure you include a legend.
        # Save the result in the filename provided.
        plt.figure(figsize=(10, 6))
        plt.bar(self.names, self.latencies, color='skyblue')
        plt.xlabel('Pipeline')
        plt.ylabel('Latency (ms)')
        plt.title('Pipeline Latency Comparison')
        plt.savefig(filename)  # Save the plot to the provided filename
        plt.close()  # Close the plot



# Insert code to generate plots here as needed

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

if __name__ == '__main__':
    print("Complete part 3. Please use the main function below to generate your plots so that they are regenerated whenever the code is run:")

    print("[add code here]")
    # TODO: add code here
