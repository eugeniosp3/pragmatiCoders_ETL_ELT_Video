# splitter - pragmaticoders.com

import pandas as pd
import warnings
from datetime import datetime
import plotly.express as px
warnings.filterwarnings('ignore', category=pd.errors.DtypeWarning)

def whichChunkSize(chunkSize, chunkTR):
    chunkMemoryList = []
    for i,chunk in enumerate(chunkTR):
        if i < 11:
            chunk_memory_mb = chunk.memory_usage(deep=True).sum() / (1024 ** 2)
            chunkMemoryList.append(chunk_memory_mb)
        else:
            break

    checkStats = pd.Series(chunkMemoryList)
    print("Current Chunk {:,} Median Memory {:.2f}MB".format(chunkSize, checkStats.median()))
    return chunkSize, checkStats.median()


# read in the csv file with a chunk size of 50,000
fileName = '311_Service_Requests_from_2010_to_Present_20231102.csv'
chunkToMemory = {}
for chunkSize in range(1000, 5000000, 100000):
    chunkTR = pd.read_csv(fileName, chunksize=chunkSize, low_memory=False)
    chunkSize, statsMedian = whichChunkSize(chunkSize, chunkTR)
    if chunkSize != 1000:
        chunkToMemory[chunkSize] = statsMedian
    if (statsMedian > (2000 * .90)) and (statsMedian < 2000):
        break



chunkToMemoryDF = pd.DataFrame.from_dict(chunkToMemory, orient='index', columns=['Memory']).reset_index().rename(columns={'index':'Chunk Size'})

# create plotly express line plot showing trend x axis should be the chunk size y axis should be the median memory
# size, i want it to show a label on the line for each chunk size. a small circle where the chunk size is
fig = px.line(chunkToMemoryDF, x='Chunk Size', y='Memory', title='Chunk Size vs. Median Memory')
fig.update_traces(mode='markers+lines')
fig.show()


fileName = '311_Service_Requests_from_2010_to_Present_20231102.csv'
selectedChunkSize = 901000
currentDate = datetime.now().strftime("%d%m%Y")

def writeChunkOut(selectedChunkSize, fileName):
    chunkTR = pd.read_csv(fileName, chunksize=selectedChunkSize, low_memory=False)
    for i,chunk in enumerate(chunkTR):
        # calculate the row numbers range for this chunk and add that to the file name
        startRow = (i * selectedChunkSize) + 1
        endRow = startRow + selectedChunkSize - 1
        chunk.to_csv(f'partitions/NYC311ServiceRequestsDate{currentDate}Start{startRow}toEnd{endRow}.csv', index=False)


writeChunkOut(selectedChunkSize, fileName)

    
