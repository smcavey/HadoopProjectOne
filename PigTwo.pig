customers = load 'Customers.csv' using PigStorage(',') as (ID, Name, Age, Gender, CountryCode, Salary);
groups = group customers by CountryCode;
countryCustomerCount = foreach groups generate group as CountryCode, COUNT(customers.ID) as numCusts;
countriesOver5kLess2k = filter countryCustomerCount by (numCusts > 5000) or (numCusts < 2000);

store countriesOver5kLess2k into 'PigTwoOut.csv' using PigStorage(',');
