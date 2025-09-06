# **Example Commands**



#### **Meta / quality**



dataset shape

list columns

show dtypes

missing values

duplicate rows

head 5



#### **Distributions \& breakdowns**



value counts for state\_abb top 10

value counts for category\_short top 10





#### **Aggregates**



sum scheduled\_quantity by state\_abb

average scheduled\_quantity by year

median scheduled\_quantity by state\_abb

std scheduled\_quantity by year

min scheduled\_quantity by state\_abb

max scheduled\_quantity by state\_abb where year = 2023





#### **Filters**



sum scheduled\_quantity where state\_abb = TX

average scheduled\_quantity by state\_abb where category\_short contains ldc

sum scheduled\_quantity by state\_abb where year = 2024





#### **Sorting / Top-N**



top 10 rows by scheduled\_quantity

top 10 rows by scheduled\_quantity where state\_abb = tx

top 10 rows by scheduled\_quantity where category\_short contains interconnect



#### **Correlations**



correlations





#### **Anomalies**



outliers in scheduled\_quantity

find multivariate anomalies using isolation forest

find multivariate anomalies across scheduled\_quantity and rec\_del\_sign











