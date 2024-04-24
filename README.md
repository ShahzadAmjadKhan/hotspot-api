# Helium Hotspot Data Extractor
- A python script to extract hotspot data available via APIs
- Extracted data is stored in CSV files.
- Step1: Extract list of hotspots data by network; mobile and iot
- Step2: Extract list of organizations registered on the helium network
- Step3: For each hotspot entry extracted in step1, get the hotspot details

# Solution
- More than million records are extracted in step 3. To improve performance a multi-thread apporach was used.
- Chunks of 1000 were created and added in list where each chunk was processed in parallel by threads
- Number of threads were based on number of CPU cores available.
- Each chunk writes data in CSV file.
- When processing is completed, CSVs are merged

# Language and Libraries
- Python
- Requests
- Pandas
