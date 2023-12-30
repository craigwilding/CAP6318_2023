import os
import shutil
import pandas
import numpy as np

################################################
# Pandas Transforms
# This file contains common data transformations 
# used to transform data before loading to a database.
#################################################

TAB = "\t"
EOL = '\n'
COMMA = ','
QUOTE = '"'
SINGLE_QUOTE = "'"
BIN_START = "b'"
# indicates whether to add a column to beginning or end
COL_BEGIN = 0
COL_END = -1



class PandasTransform :
    ###########################################################
    # PandasTransform.Init()
    # Assign the file name, delimiter and whether the file has column headers or not
    # This allows the transforms to adjust to the type of file
    # rather than having to pass in these paramaters to each function
    #
    # fileNameIn - name of the file.  All transforms will be written back to this file
    # delim - The file delimiter, COMMA is the default.  It assumes CSV files
    #         Set to TAB for tab delimited text file
    # headers - True/False indicates whether the file contains headers or not
    ###########################################################
    def __init__(self, fileNameIn, delim=COMMA, headers=True, float_precisionIn=6):
        #print("create PTX: " + fileNameIn)
        self.fileNameIn = fileNameIn
        self.delim = delim
        self.headers = headers
        self.headerRead=0
        if (self.headers != True) :
            self.headerRead = None
        # end self.headerRead
        self.float_precision = float_precisionIn

        self.float_formatOut='%.' + str(float_precisionIn) + 'f'
        pandas.set_option("display.precision", self.float_precision)

        #print("read PTX: " + fileNameIn)
        self.df = self.read()
        #print("read success PTX: " + fileNameIn)
        
    # end init

    ############################################
    # Read as pandas data frame
    ############################################
    def read(self) :
        dtypeKeys={"precinct": "string", "prc_grp": "string", "prc_split": "string"}
        # dtype='unicode'  or engine='python'
        df = pandas.read_csv(self.fileNameIn,sep=self.delim,header=self.headerRead,index_col=False,low_memory=False, float_precision="round_trip")
        return df
    # end read()

    def read_from(self, tempFileIn) :
        self.df = pandas.read_csv(tempFileIn,sep=self.delim,header=self.headerRead,index_col=False,low_memory=False, float_precision="round_trip")
        return self.df
    # end read()

    ############################################
    # Write df to out file
    ############################################
    def write(self) :
        self.df.to_csv(self.fileNameIn, encoding='utf-8', index=False, sep=self.delim, header=self.headers, float_format=self.float_formatOut)
        return self.df
    # end write()

    def write_to_file(self, df, fileNameOut) :
        df.to_csv(fileNameOut, encoding='utf-8', index=False, sep=self.delim, header=self.headers, float_format=self.float_formatOut)
        return df
    # end write()

    ############################################
    # Get Data Frame
    ############################################
    def getDataFrame(self) :
        return self.df
    # end

    ############################################
    # Remove Non-Ascii 
    # Remove non utf8 characters that break loading into the database
    ############################################
    def removeNonAscii(self) :

        for col in self.df.columns:
            # convert non-english characters.
            self.df[col].replace({r'[^\x00-\x7F]+':''}, regex=True, inplace=True)
            # self.df[col] = self.df[col].str.strip()
        # end for columns
    # end removeNonAscii

    def stripSpace(self, listTextColumns) :

        for col in listTextColumns:
            #print(col)
            # *** Throws error if there are no values in the column
            empty = self.df[col].replace(r'^\s*$', np.nan, regex=True).isna().all()
            if (not empty) :
                # strip leading and trailing white space
                self.df[col] = self.df[col].str.strip()
            # end if not empty
        # end for columns
    # end removeNonAscii

    ############################################
    # Remove Binary 
    # Example:
    # b'Candidate/Committee',b'Date',b'Amount'
    ############################################

    def removeBinary(self) :

        if (self.headers == True) :
            # convert headers
            listColumns = list(self.df.columns)
            dictRenameCols = {}
            for colName in listColumns :
                if (colName.startswith(BIN_START))  and (colName.endswith(SINGLE_QUOTE)):
                    # print(colName)
                    newName = colName.replace(BIN_START, "")
                    newName = newName[:len(newName)-1]
                    dictRenameCols[colName] = newName
                # end if binary characters
            # end for column names

            self.renameColumn(dictRenameCols)
            del listColumns
            del dictRenameCols

            # re-read since renameColumn method writes back to the file.
            # self.df = pandas.read_csv(self.fileNameIn,delimiter=self.delim, header=self.headerRead)
        # end if column headers

        # convert values
        for row, rowVal in self.df.iterrows():
            for col in self.df.columns:
                # convert non-english characters.
                value = str(self.df.loc[row,col])
                if (value.startswith(BIN_START))  and (value.endswith(SINGLE_QUOTE)):
                    # print(value)
                    value = value.replace(BIN_START, "")
                    value = value[:len(value)-1]
                    self.df.loc[row,col] = value
                # end if binary characters
            # end for columns
        # end for row

    # end removeBinary

    ############################################
    # Add Column Names 
    # Add column Names to the file
    # This replaces the fileNameIn with the converted file.
    ############################################

    def addColumnNames(self, listColNames) :

        fileNameOut = self.fileNameIn.replace(".", "_addColumnNames" + ".")
           
        #Assign column names
        self.df.columns = listColNames
        self.headers = True
        self.headerRead=0

        # write to temp file
        #self.write(df, fileNameOut)
        self.df.to_csv(fileNameOut, encoding='utf-8', index=False, sep=self.delim, header=True)
        # re-read df so it picks up headers.
        self.df = pandas.read_csv(fileNameOut,sep=self.delim,header=self.headerRead,index_col=False)

        # remove temp file
        os.remove(fileNameOut)

    # end addColumnNames

    ############################################
    # Add Column 
    # Add a column to the file
    ############################################

    def addColumn(self, newColName, defaultValue, colLocation=COL_END) :
            
        #check if has column headers already
        listColNames = list(self.df.columns)
        if (newColName not in listColNames) or (self.headers == False) :

            # add the column with default value.
            if (self.headers == True) :
                if (COL_BEGIN == colLocation) :
                    self.df.insert(0,newColName,'')
                # end insert at beginning
                self.df[newColName] = defaultValue
            else :
                if (COL_BEGIN == colLocation) :
                    self.df.insert(0,'',defaultValue)
                else :
                    self.df[len(self.df.columns)] = defaultValue
                # end no header
            # end if header
        else :
            print("column already exists: " + newColName)
        # end if office

        del listColNames
    # end addColumn

    ############################################
    # Remove Column 
    # Remove a column from the file
    # If headers=False, then colName must be the column index to remove
    ############################################
    def removeColumn(self, colName) :

        #check if has column exists already
        listColNames = list(self.df.columns)
        if (colName in listColNames) or (self.headers == False) :

            # Remove the column
            if (self.headers == True) :
                self.df.pop(colName)
            else :
                ix = int(colName)
                self.df.drop(self.df.columns[ix], axis=1,inplace=True)
            # end if header
        else :
            print("column did not exists: " + colName)
            print(listColNames)
        # end if office

        del listColNames
    # end removeColumn

    ############################################
    # Remove Rows 
    # Remove rows where it matches a column value
    # If headers=False, then colName must be the column index to remove
    ############################################
    def removeRows(self, colName, colValue) :
            
        #check if has column exists already
        listColNames = list(self.df.columns)
        if (colName  in listColNames) or (self.headers == False) :

            listValues = []
            listValues.append(colValue)

            # Remove the rows matching the value
            if (self.headers == True) :
                self.df = self.df[~self.df[colName].isin(listValues)]
            else :
                ix = int(colName)   
                self.df = self.df[~self.df[ix].isin(listValues)]
            # end if header
            del listValues
        else :
            print("column did not exists: " + colName)
        # end if office

        del listColNames
    # end removeRows

    ############################################
    # Select Rows 
    # Select rows where column matches one of a list of values
    # If headers=False, then colName must be the column index to remove
    # This returns a seperate data frame than the current dataframe.
    ############################################
    def selectRows(self, colName, listValues) :
        
        dfSelect = pandas.DataFrame()
        #check if has column exists already
        listColNames = list(self.df.columns)
        if (colName  in listColNames) or (self.headers == False) :

            # Select the rows matching the value
            if (self.headers == True) :
                dfSelect = self.df[self.df[colName].isin(listValues)]
            else :
                ix = int(colName)   
                dfSelect = self.df[self.df[ix].isin(listValues)]
            # end if header
        else :
            print("column did not exists: " + colName)
        # end if office

        del listColNames
        return dfSelect
    # end selectRows

    def replaceColumnValue(self, colName, curValue, newValue) :
        
        #check if has column exists already
        listColNames = list(self.df.columns)
        if (colName  in listColNames) or (self.headers == False) :
            # Select the rows matching the value
            if (self.headers == True) :
                
                self.df.loc[self.df[colName] == curValue, colName] = newValue
            else :
                ix = int(colName)
                self.df.loc[self.df[ix] == curValue, ix] = newValue
            # end if header
        else :
            print("column did not exists: " + colName)
        # end if office

        del listColNames
        
    # end replaceColumnValue

    ############################################
    # Rename Column 
    # Rename a column from the file
    # The column(s) to be changed must be passed in as a dictionary
    # dictRenameColumns - (key = oldName, value = new name)
    ############################################
    def renameColumn(self, dictRenameColumns) :
           
        #check if has column exists already
        self.df.rename(columns=dictRenameColumns, inplace=True)

    # end renameColumn

    ############################################
    # Rename Column 
    # Rename a column from the file
    # The column(s) to be changed must be passed in as a dictionary
    # dictRenameColumns - (key = oldName, value = new name)
    ############################################
    def renameColumns(self, dictRenameColumns) :
           
        #check if has column exists already
        self.df.rename(columns=dictRenameColumns, inplace=True)

    # end renameColumn
    ############################################
    # SetNullValues() 
    # Set all missing values on a column to a given value
    # Params 
    # listColumns - a list of columns to check for null values
    # nullValue - the value to set the columns to.
    #
    # Notes:  This can be used in multiple ways:
    # + Set all integer or float files to 0 instead of blank
    # + Set 'NULL' as the common null value, then use that in the DB Load command NULL as 'NULL'
    ############################################

    def setNullValuesOnColumns(self, listColumns, nullValue="") :
            
        #check if has column headers already
        for colName in listColumns :

            self.df[colName].fillna(nullValue, inplace=True)
        # end for
    # end SetNullValues

    def setNullValues(self, colName, nullValue="") :
            
        self.df[colName].fillna(nullValue, inplace=True)
        
    # end SetNullValues
    
    def SetNullValuesAsInt(self, listColumns, nullValue=0) :
            
        #check if has column headers already
        for colName in listColumns :

            self.df[colName].fillna(nullValue, inplace=True).astype(int)
        # end for
    # end SetNullValuesAsInt

    # Fixes integers columns from being saved as decimal.  I.e 2, not 2.0
    def SetColumnsAsInt(self, listColumns) :
        for colName in listColumns :
            # pandas is converting int values to float with .0 if null values in column.
            self.setNullValues(colName, "0")
            self.df[colName] = self.df[colName].astype(str)
            self.df[colName] = self.df[colName].str.replace('.0','', regex=False)
            self.df[colName] = self.df[colName].astype(np.int64)
        # end for
    # end SetColumnsAsInt

    # Fixes integers columns from being saved as decimal.  I.e 2, not 2.0
    def SetColumnAsInt(self, colName) :
        # pandas is converting int values to float with .0 if null values in column.
        self.setNullValues(colName, "0")
        #self.replaceColumnValue(colName, '', "0")
        self.df[colName] = self.df[colName].astype(str)
        self.df[colName] = self.df[colName].str.replace('.0','', regex=False)
        self.replaceColumnValue(colName, ' ', "0")
        self.df[colName] = self.df[colName].astype(np.int64)
        # end for
    # end SetColumnsAsInt

    def SetColumnsAsFloat(self, colName, precision) :
        self.df[colName] = self.df[colName].astype(np.float64)
        self.df[colName].round(decimals = precision)
    # end SetColumnsAsFloat

    def SetColumnsAsStr(self, listColumns) :
        for colName in listColumns :

            self.df[colName] = self.df[colName].astype(str)
        # end for
    # end SetColumnsAsStr

    def zfill(self, colName, numDigits) :
            
        self.df[colName] = self.df[colName].str.zfill(numDigits)
        
    # end SetNullValues

    def FormatDate(self, colName, dtFormat='%Y-%m-%d') :
        self.df[colName] = pandas.to_datetime(self.df[colName]).dt.strftime(dtFormat)
    # end FormatDate


    def FormatDateColumns(self, listColumns, dtFormat='%Y-%m-%d') :
        for colName in listColumns :
            self.replaceColumnValue(colName, " ", "") # replace space with null
            empty = self.df[colName].replace(r'^\s*$', np.nan, regex=True).isna().all()
            if (not empty) :
                self.df[colName] = pandas.to_datetime(self.df[colName]).dt.strftime(dtFormat)
        # end for
    # end FormatDate

    def FormatDateTimeStamp(self, colName, dtFormat='%Y-%m-%d %H:%M:%S') :
        self.df[colName] = pandas.to_datetime(self.df[colName]).dt.strftime(dtFormat)
    # end FormatDate

    def TestRowColumn(self, rowNum, colNum) :
        # this is used to test the results
        # it counts the header as a row number if there is one.
        self.write()
        testVal = ""
        fileIn = open(self.fileNameIn, encoding='utf-8')
        rowCount = 0
        for line in fileIn :
            rowCount += 1
            if (rowNum != rowCount) :
                continue

            columns = line.replace(EOL,"").split(self.delim)
            colCount = 0
            for col in columns :
                colCount += 1
                if (colNum == colCount) :
                    testVal = col
                # end if
            # end for columns
        # end for each line
        fileIn.close()
        del fileIn

        return testVal
    # end TestRowColumn

    def PrintRowColumns(self) :
        # this is used to print the testresults
        fileIn = open(self.fileNameIn, encoding='utf-8')
        rowCount = 0
        for line in fileIn :
            rowCount += 1

            columns = line.replace(EOL,"").split(self.delim)
            colCount = 0
            for col in columns :
                colCount += 1
                print("Row: " + str(rowCount) + " Col: " + str(colCount) + " value: " + str(col))
            # end for columns
        # end for each line
        fileIn.close()
        del fileIn

    # end PrintRowColumns

# end class PandasTransform
