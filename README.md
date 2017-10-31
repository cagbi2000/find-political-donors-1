Before submitting your solution you should summarize your approach, dependencies and run instructions (if any) in your `README`.

# Approach
In order to keep a running median of the information, I needed to store the list of numbers received up till that point in time using a sorted list.

First, I used a dictionary of dictionaries to quickly access group data by CMTE_ID and zipCode/TRANSACTION_DT. As numbers would stream in, I would determine what group they belonged to and update the list of numbers associated with those keys. I created a data structure called sortedList to sort the data on the fly using a priority queue. This also speeds up how quickly the median is found. 

Second, to ensure the data for the dates was returned in alphabetical and chronological order, I used a priority queue to sort the keys and then read the keys in order to write the data to file

#Dependencies
The only library imported was heapq. Otherwise, this file has no external or exotic dependencies

#Run Instructions
Use ./run.sh to run the script





