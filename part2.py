"""
Part 2: Dataflow Graph

For this part, draw a dataflow graph for your pipeline from Part 1.

Save your graph in output/part2.png.

=== Instructions for Drawing the Graph ===

Your dataflow graph should have one node for:

- Each different data source (load_input function), labeled with the source name

- Each intermediate transformation or action on an RDD -- in particular,
   you should include one node for each call to general_map and general_reduce.
   Consider each general_map and general_reduce to be separate operators (each one their own node).

   There should be multiple nodes for each question, which you can label for example as q5-map, q5-reduce (for question 5).

   Depending on your solutions, there may be other intermediate nodes if you have
   other steps besides general_map and general_reduce (such as a .map, .filter, etc.).

- The final output for each question.
   Each of these "output nodes" should be labeled with the question number and output
   (e.g., q1-out, q2-out, q4-out, q5-out, q6-out, etc.)

- You don't need to draw nodes for the unit tests (test_general_map and
   test_general_reduce) or for the final pipeline (PART_1_PIPELINE).

- Important: if you have multiple pipelines that share the same intermediate computations on the same intermediate data,
   you should draw these as only one node/task (for the shared parts), not multiple
   nodes/tasks.
   (For example, q4 and q5 both use the same input, so they should share at least the
   load_input node.)

=== Grading Notes ===

Please follow the following additional grading notes:

- Include your answer in a single image, output/part2.png.

- You can draw the dataflow graph however you like, but it must
be a PNG image. Feel free to take a screenshot to get to a PNG (as long as the writing is legible).

- Some software and online websites may be helpful, in particular, you could use:
   Google Slides
   draw.io
   Microsoft PowerPoint

- A hand-drawn graph is OK as well, as long as it is legible.

- To assign your score, we will manually look at your graph image to see if it is correct and well-labeled. The exact set of nodes may differ slightly between submissions, but there are some important general features/general structures that we will look for.
"""

if __name__ == '__main__':
    print("This part has no code. Please draw a dataflow graph and save it in output/part2.png.")
