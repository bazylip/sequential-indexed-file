# Sequential indexed file implementation
Python implementation of sequential indexed files. Provides functionalities of adding, deleting and updating records for big files and stores them ordered by key. Uses index file for quick record lookup, needing no more than 5 disk accesses (in pessimistic scenarios). Useful in big database structures, where you cannot afford storing the whole file in operative memory.

Written as a part of "Database Structures" course taken at Gdańsk University of Technology, 2020.