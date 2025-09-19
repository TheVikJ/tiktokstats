## Setup ##
Begin by cloning the repository and creating a virtual environment. Then run  ```pip install -r requirements.txt``` 

## Random Sampling ##
Collecting samples is done with ```randomsample.py```, with additional specifications passed in as arguments.   

```-s``` specifies how many IDs are collected per second of tiktok videos   

```-t``` is the number of threads allocated to running the script. This is set by default to 15, but can be set lower or higher depending on machine specifications

```-b``` for the unix timestamp from which to _begin_ the scrape, with ```-e``` specifying the end timestamp

```-i``` sets the incrementer for ID sampling, which should be set to 10  

