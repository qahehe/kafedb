# 1
In TPCHBench Spark doesn't analyze its database.  So it's not likely to generate optimized query plan for DEX.  So both Spark and DEX's results are based on unoptimized join order.

# 2
Spark analyzes its database.  So both Spark and DEX see improvement by having optimized join order.
