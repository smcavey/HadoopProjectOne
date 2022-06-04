transactions = load 'Transactions.csv' using PigStorage(',') as (TransID, CustID, TransTotal, TransNumItems, TransDesc);
groups = group transactions by CustID;
custAndTransTotals = foreach groups generate group as CustID, SUM(transactions.TransTotal) as sumTrans;

customers = load 'Customers.csv' using PigStorage(',') as (ID, Name, Age, Gender, CountryCode, Salary);
idAndName = foreach customers generate ID, Name;

joined = join idAndName by ID, custAndTransTotals by CustID using 'replicated';

nameTrans = foreach joined generate Name, sumTrans;

store nameTrans into 'PigOneOut.csv' using PigStorage(',');
