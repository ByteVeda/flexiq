"""Measurement plumbing shared by every runner.

The runners know how to drive one queue each; everything that has to be
identical across all five — the scenario, the completion sink, the percentile
maths, the machine fingerprint, the results schema — lives here, so that a
difference between two numbers is a difference between two queues.
"""
