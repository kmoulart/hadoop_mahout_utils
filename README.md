# Hadoop & Mahout Utils
===================

Here are the classes I developed to run tests and benchmarks with hadoop and mahout.

As far as I'm writing this, it contains :

## makesf
Transforms a CSV into a SequenceFile that can be used to traint and test a Naive Bayes model with mahout.
This process is sequential and thus will run on one node only.

## makesfmr
Same process as above but using MapReduce to improve performances.

## randforest
Builds a random forest using a CSV and then tests its performances (Currently under construction).

Feel free to use/improve/comment.
