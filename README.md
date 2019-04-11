## Summary

My solutions to Jornaya's [Software Engineer Code Test](https://leadid-dev.gitlab.io/scratch/software-engineering-code-test/).


# 1. Recency Calculation

* Source code: `code_test/recency_calc.py`
* Event data file (can be used to feed larger event sets): 
  `code_test/data/events.csv`

### Example Run and Output

```bash
(jornaya) kevin@kpm:~/development/python-projects/kmcdermott_jornaya/code_test$ python recency_calc.py
Reading inputs from ./data/events.csv
[2, 6, 10]
```

# 2. A Customer Question

* SQL query & sample python code: `code_test/client_query.py`
* CSV file used as the source for the data: 
  `code_test/data/lead_daily_sum_sample_data.csv`
* SQLite database used to test the query against: `code_test/data/sqlite.db`

### Example Run and Output

```bash
(jornaya) kevin@kpm:~/development/python-projects/kmcdermott_jornaya/code_test$ python client_query.py
Reading source data from ./data/lead_daily_sum_sample_data.csv
Customers who generated the most leads in 5/2018.
Client ID: 1234
Client ID: 9012
```

### Editorial Notes

1. In the example problem for Recency Calculation, the bullets should say:

> - Are ~~at least~~ no more than 1 second old?
> - Are ~~at least~~ no more than 5 seconds old?
> - Are ~~at least~~ no more than 8 seconds old?

2. In A Customer Question, the source table doesn't match the DML (dates differ for client 9012).
