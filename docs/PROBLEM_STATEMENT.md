\#Problem Statement — Bitcoin Blockchain ETL Pipeline



\# Problem Statement



\## The Problem



Sending Bitcoin sounds simple — but the cost of doing it is not.



Bitcoin transaction fees change constantly. Some days they are low.

Other days, especially when the network is busy, fees can eat up

a significant portion of what you are sending. Most people have

no idea when the cheap times are, or why fees spike the way they do.



Between 2013 and 2017, Bitcoin went from a small experiment to a

global financial phenomenon. During that time, average fees jumped

from around 1% to over 3% — a 3x increase. For someone sending

large amounts, that difference is real money.



The problem is simple:



> \*\*There is no easy way for everyday Bitcoin users to understand

> fee trends and decide the best time to transact.\*\*



Raw blockchain data exists — but it is massive, messy, and

impossible to read without the right tools.



\---



\## Who Does This Affect



\- \*\*Investors\*\* moving large amounts of Bitcoin between wallets

\- \*\*Traders\*\* making time-sensitive transactions

\- \*\*Businesses\*\* accepting or sending Bitcoin payments

\- \*\*Researchers\*\* studying Bitcoin network behavior over time



\---



\## The Solution



This project builds an automated data pipeline that:



1\. Pulls real Bitcoin transaction data from AWS public datasets

2\. Cleans and organizes 8.7 million transactions (2013–2017)

3\. Runs statistical analysis to find fee patterns

4\. Pulls today's live Bitcoin data automatically every day

5\. Displays everything in a simple, readable Power BI dashboard



The dashboard answers one question:



> \*\*When were Bitcoin fees lowest — and what caused them to spike?\*\*



\---



\## Historical Data + Live Application



The 5-year historical dataset (2013–2017) reveals clear patterns

in Bitcoin fee behavior:



\- Fees spike when transaction volume is high

\- Network congestion directly causes fee increases

\- Whale activity correlates with fee movements



These patterns do not change. The same forces that drove fees in

2017 drive fees today.



Our pipeline automatically pulls today's Bitcoin transaction data

daily via Apache Airflow — so the dashboard always shows the most

recent activity alongside historical context.



A user can look at today's transaction volume and — based on

historical patterns — make an informed decision about whether

fees are likely to be high or low right now.



Think of it like a weather forecast. Meteorologists study decades

of historical weather data to find patterns — then apply those

patterns to predict today's weather. This project does the same

thing with Bitcoin fees.



\---



\## What the User Gets



After looking at the dashboard, a user can answer:



\- Which years had the lowest average fees?

\- What caused fees to jump in 2017?

\- How did network activity relate to fee increases?

\- Were whale transactions affecting fees?

\- How does today's activity compare to historical patterns?



\---



\## Why This Matters



The 2017 Bitcoin fee spike was not random. It happened because

too many people were transacting at the same time. The network

got congested. Fees went up because miners prioritized higher

paying transactions.



Understanding this pattern helps users make smarter decisions

about when to send Bitcoin — and how much to expect in fees.



\---



\## What This Project Is Not



This is not a price prediction tool. It does not tell you whether

to buy or sell Bitcoin. It focuses purely on transaction behavior

and network fees — things that are measurable, historical, and factual.



\---



\## Data Source



\- \*\*Source:\*\* AWS Public Blockchain Dataset

\- \*\*Coverage:\*\* January 2013 – December 2017 + daily live updates

\- \*\*Size:\*\* 8.7 million transactions (growing daily)

\- \*\*Format:\*\* Parquet files

\- \*\*Updated:\*\* Every day automatically via Apache Airflow

