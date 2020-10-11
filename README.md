## Overview
This code allocates observers who have volunteered to polling station based on its priority.

The order in which they are allocated is as follows with polling stations with higher priority being filled first:
1. Observers who **have legal** experience/expertise and are willing to be observers **inside all day.**

2. Observers who **have legal** experience/expertise and are willing to be observers **outside** for the **AM** shift.

3. Observers who **have legal** experience/expertise and are willing to be observers **outside** for the **PM** shift.

4. Observers who **don't have legal** experience/expertise and are willing to be observers **inside all day.**

5. Observers who **don't have legal** experience/expertise and are willing to be observers **outside** for the **AM** shift.

6. Observers who **don't have legal** experience/expertise and are willing to be observers **outside** for the **PM** shift.

   

## To run

1. Clone this repo
2. From the base directory:
   1. Run basic assigment using `python -m src.basic_assignment`
   2. Run optimised assignment using `python -m src.optimal_assignment`

Note that you must have access to the google sheet with observers details.



## Details

Optimisation runs the basic assignments and then [House Exchange Algorithm](https://en.wikipedia.org/wiki/Top_trading_cycle). It's not globally optimal but is sufficient for our purpose.





