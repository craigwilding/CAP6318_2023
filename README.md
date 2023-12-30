# Tourism Co-authrship Network

This project collects author and publication information from Google Scholar on Tourism authors  
and builds a coauthorship network graph that has a node for each author and links representing authors that have coauthored a publication together.

You select a topic and the number of authors.  This then uses selenium webscraper to pull the list of top authors (by citation count) from Google Scholar, collect the last Y years of publications for each author, then match the authors and coauthors to form the nodes and edges in the coauthorship network.  



## Configuration  
The code supports the following configuration parameters set in [MyEnvVars.py](/MyEnvVars.py)

- N_AUTHORS = 100      # Number of authors to collect
- TOPIC = "Tourism"    # Topic to search for.  Use "ALL" to use the extended TOPIC_ALL list.  This is also used as a short name in file names
- TOPIC_ALL = "label:tourism OR label:tourism_marketing OR label:tourism_management OR label:tourism_economics OR label:destination_marketing OR label:Hotel_and_Tourism_management OR label:tourist_behavior OR label:tourism_impacts OR label:sustainable_tourism"
- GRAPH_FOLDER = "GraphALLTourMgmt5Year"  # folder name to store gephi node and edge files in.
- YEARS_PRIOR = 5  # of years of publications to pull
- INCLUDE_NONTOPIC_COAUTH=False  # Add nodes for coauthors not found in original authors list.  Default is false.

### Path setup
The **MyEnvVars.py** also saves path locations that are used by the program
chromedriver_path = r"C:\MyTools\Python\chromedriver-win64\chromedriver.exe"
workspace = r"D:\UCF\Data Engineering\CNT_5805_Networks\Network Data\Tourism" # top project folder

## Web Scraper Setup
See the file: [Web Scraper Setup](/00_1_WebScraper_Setup.ipynb)
For instructions on downloading and setting up Selenium's Chrome Web Scraper


## Processing Steps
Run the python script [00_ProcessAuthors.py](/00_ProcessAuthors.py) to run the scripts in their processing order.  
You can run the script seperately for each process to repeat or redo a step.

- Step 1: [Get Author List](/01_GetAuthorList.py)  
This opens Google Scholar, enters the topic to search on and pulls the list of top N authors.  
This saves an ID number for each author found that will be used in all references to the author later.  The ID is the author's order or rank in the list, so the ID will change if you rerun the script requiring all future steps to be rerun as well.  
This saves the author id and links to [GS_Topic_Authors_N.csv](/Data/GS_ALL_Authors_2000.csv)  

- Step 2: [Get Author Data](/02_GetAuthorData.py)  
This opens the author page for each author from step one and retrieves information on the author such as name and university.
This saves the author id and data to [GS_Topic_AuthorData_N.csv](/Data/GS_ALL_AuthorData_2000.csv)  

- Step 3: [Get Publications](/03_GetPublications.py)  
This opens the author page for each author from step one and retrieves the last N years of publications for each author.  It saves the link to the publication page on Google Scholar
This saves the publication id and links to [GS_Topic_Publications_N.csv](/Data/GS_ALL_Publications_2000.csv)  

- Step 4: [Get Publication Authors](/04_GetPubAuthors.py)  
This opens the publication page for each publication from step three and retrieves the list of coauthors
This saves the publication id and coauthor names to [GS_Topic_PublAuthors_N.csv](/Data/GS_ALL_PubAuthors_2000.csv)  

- Step 5: [Get Author Alias](/05_GetAuthAlias.py)  
This goes through the author list and publication list for each author and builds an alias list for each author.  
This is because the author's name may appear differently between publications, such as including a middle initial or not.
This alias list is then used to identify authors and coauthors for linking them.
This saves the author alias names to [GS_Topic_AuthAlias_N.csv](/Data/GS_ALL_AuthAlias_2000.csv)  

- Step 6: [Set Publication Author Status](/06_SetPubAuth_Status.py)  
For each author - coauthor pair in [GS_Topic_PublAuthors_N.csv](/Data/GS_ALL_PubAuthors_2000.csv), 
This uses the author alias list to identify the author and coauthor and find the author id number for both.
It then parses the list to add a **Status** column to indicate if it found a co-authorsip relationship between the authors.  
   
	**Status:**
	- ONLY : CoAuthor is Original Author and NO other co-authors
	- SELF : Coauthor is the author, but there are other co-authors
	- DUP  : Same Title, different pubids.  Take pub id where Author is 1st co-author
	- MAUTH : Coauthor is listed as 6th author or more.  Some have large # of authors.  (The cutoff can be changed in the code)
	- NOAUTH : Author not listed as an author or coauthor
	- COAUTH : This is a valid co-author with the original author.  Id of Co-author must have been found  

This updates the [GS_Topic_PublAuthors_N.csv](/Data/GS_ALL_PubAuthors_2000.csv)  file with the **Status** and **coauthid** columns  

- Step 7: [Get CoAuth Alias](/07_GetCoAuthAlias.py)  
This steps adds any coauthors that were found listed in publications but not in the original author list to the author list.
This step is currently being skipped to limit the scope to the original N authors selected.

 - Step 8: [Build Nodes and Edges](/08_BuildNodesAndEdges.py)  
This step reads the author - coauthor data from [GS_Topic_PublAuthors_N.csv](/Data/GS_ALL_PubAuthors_2000.csv)  
and coverts it to **Gephi** Node and Edge ccv files.  
This Node file is saved to Gephi\GRAPH_FOLDER  [GS_Topic_Nodes.csv](/Gephi/GraphALLTourMgmt5Year/GS_ALL_Nodes.csv)  
This Edge file is saved to Gephi\GRAPH_FOLDER  [GS_Topic_Edges.csv](/Gephi/GraphALLTourMgmt5Year/GS_ALL_Edges.csv)  
There are additional edge files save that include a **Weight** value indicating what the size of the edge indicates:  
	- Count (# of coauthored publications)  [GS_Topic_Edges_count.csv](/Gephi/GraphALLTourMgmt5Year/GS_ALL_Edges_count.csv)  
	- Year (1st year of coauthored publications)  [GS_Topic_Edges_year.csv](/Gephi/GraphALLTourMgmt5Year/GS_ALL_Edges_year.csv) 
	- Order (Order of coauthorship)  [GS_Topic_Edges_order.csv](/Gephi/GraphALLTourMgmt5Year/GS_ALL_Edges_order.csv) 

- Step 9: [Build Adjacency Matrix](/09_BuildAdjacencyMatrix.py)  
This steps converts the Nodes and Edges files into an adjacency matrix for use in other network graph programs.
This step is currently not part of the process but can be run seperately
