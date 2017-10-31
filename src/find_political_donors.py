#!/usr/bin/python
# -*- coding: utf-8 -*-

import heapq

def StreamData( inputFile, output1File, output2File ):

    # Initialize parameters
    #aa = 0
    keyList = []
    headerList = [ "CMTE_ID", "AMNDT_IND", "RPT_TP", "TRANSACTION_PGI",
                  "IMAGE_NUM", "TRANSACTION_TP", "ENTITY_TP","NAME",
                  "CITY", "STATE", "ZIP_CODE", "EMPLOYER", "OCCUPATION", 
                  "TRANSACTION_DT", "TRANSACTION_AMT", "OTHER_ID", 
                  "TRAN_ID", "FILE_NUM", "MEMO_CD", "MEMO_TEXT", "SUB_ID" ]
    
    # Initialize output files
    output1 = Processor( output1File )
    output2 = Processor( output2File )

    # Open input and output files
    print("Streaming data from %s" % inputFile)
    for inputStr in open( inputFile, "r"):

        # Clean up string line and create dictionary
        inputStr  = inputStr.rstrip()
        inputDict = dict( zip(headerList, inputStr.split("|")) )

        # Skip certain records
        if   len( inputDict["CMTE_ID"] ) == 0: continue          # Ignore if CMTE_ID is empty
        elif len( inputDict["OTHER_ID"] ) > 0: continue          # Ignore if OTHER_ID is not empty
        elif len( inputDict["TRANSACTION_AMT"] ) == 0: continue  # Ignore if TRANSACTION_AMT is empty

        # Determine key/value pairs
        key1  = inputDict["CMTE_ID"]
        key2  = inputDict["ZIP_CODE"][:5]
        key3  = inputDict["TRANSACTION_DT"][:8]
        value = float( inputDict["TRANSACTION_AMT"] )

        # Calculate zip code calculations for valid zip codes
        if len( key2 ) == 5: 
            output1.add_value( key1, key2, value )
            output1.write_value( key1, key2 )

        # Calculate the date calculations for valid dates
        if len( key3 ) == 8:
            keyDate = key3[4:] + key3[:4]
            output2.add_value( key1, key3, value )
            heapq.heappush( keyList, (key1, keyDate) )
          
        # Print at every 100,000th record
        #aa += 1
        #if aa % 100000 == 0: print( int(aa/100000), end = " " )

    # Write to date file
    print("Writing to " + output2.fileobj.name )
    prevKeyA = ""
    prevKeyB = ""
    while keyList:
        keys = heapq.heappop( keyList )
        if ( prevKeyA != keys[0] ) | ( prevKeyB != keys[1] ):
            prevKeyA = keys[0]
            prevKeyB = keys[1]
            output2.write_value( keys[0], keys[1][4:] + keys[1][:4] )

    # Close files
    output1.close() 
    output2.close()

    print("Done!")
    

# Class sorts values and provides summary statistics
class sortedList:
    
    def __init__(self):
        self.Low = []
        self.High = []
        self.Count = 0
        self.Total = 0

    def add(self, value):
        
        # Add value to one of two lists
        if   len( self.Low ) == 0: heapq.heappush( self.Low, -value )
        elif len( self.High )== 0: heapq.heappush( self.High, value )
        elif value < -self.Low[0]: heapq.heappush( self.Low, -value )
        elif value > self.High[0]: heapq.heappush( self.High, value )
        else: heapq.heappush( self.Low, -value )
        
        # Update running counts
        self.Count += 1
        self.Total += value
        
        # Rebalance priority queues
        if len(self.Low) > ( len(self.High) + 1 ): 
            maxVal = -heapq.heappop( self.Low )
            heapq.heappush( self.High, maxVal )
            
        elif len(self.High) > ( len(self.Low) + 1 ):
            minVal = heapq.heappop( self.High )
            heapq.heappush( self.Low, -minVal )

    def median(self):
        
        if   len( self.Low ) > len( self.High ): med_val = -self.Low[0]
        elif len( self.Low ) < len( self.High ): med_val = self.High[0]
        else: med_val = ( -self.Low[0] + self.High[0] )/2
        
        return round( med_val )


# Class processes the streaming input data and outputs it in given files
class Processor:
        
    def __init__(self, fileName):
        self.memory = {}
        self.fileobj = open( fileName, "w+")

    # Define custom functions
    def add_value( self, key1, key2, value ):

        # Create a sorted list if it doesn't exist
        if key1 not in self.memory.keys(): 
            self.memory[ key1 ] = { key2: sortedList() }
        elif key2 not in self.memory[ key1 ].keys():
            self.memory[ key1 ][ key2 ] = sortedList()

        # Add value
        self.memory[ key1 ][ key2 ].add( value )


    def write_value( self, key1, key2 ):

        # Get values
        sum_val = self.memory[key1][key2].Total
        len_val = self.memory[key1][key2].Count
        med_val = self.memory[key1][key2].median()

        # Format output
        outputStr = "%s|%s|%i|%i|%i\n" % ( key1, key2, med_val, len_val, sum_val )
    
        # Write to output file
        self.fileobj.write( outputStr )

        
    def close( self ): self.fileobj.close()
        
        
if __name__ == "__main__":
    
    import sys
    StreamData( sys.argv[1], sys.argv[2], sys.argv[3] )
    