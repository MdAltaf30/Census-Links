#!/usr/bin/env python
# coding: utf-8

# In[1]:



import sys, os
from datetime import datetime
import DWM10_Parms
import DWM16_BuildTokenFreqDict
import DWM45_Block_Cleaning ## added to perform block level token replacement

def buildBlockPairs(refDict, linkIndex, tokenFreqDict):
    logFile = DWM10_Parms.logFile
    print('\n>>Starting DWM42')
    print('\n>>Starting DWM42', file=logFile)
    blockByPairs = DWM10_Parms.blockByPairs
    blockList = []
    stopCnt = 0
    beta = DWM10_Parms.beta
    print('beta =',beta)
    print('beta =',beta, file=logFile)
    minBlkTokenLen = DWM10_Parms.minBlkTokenLen
    excludeNumericBlocks = DWM10_Parms.excludeNumericBlocks
    removeExcludedBlkTokens = DWM10_Parms.removeExcludedBlkTokens
    print('min blocking token length =', minBlkTokenLen)
    print('min blocking token length =', minBlkTokenLen, file=logFile)
    print('exclude numeric blocking tokens =', excludeNumericBlocks)
    print('exclude numeric blocking tokens =', excludeNumericBlocks, file=logFile)
    print('block by pairs of tokens =', blockByPairs)
    print('block by pairs of tokens =', blockByPairs, file=logFile)    
    # blockList is a list of ordered pairs (blockingValue, RefID) where blockingValue
    # is a single blocking token when "blockByPairs is False"
    # or concatenated pairs of blocking tokens when "blockByPairs is True"
    blockList =[]
    selectCnt = 0
    # First extract the blocking tokens from each reference into blockTokenList
    for key in linkIndex:
        if len(linkIndex[key])>0:
            continue
        selectCnt +=1
        tokenList = refDict[key]
        blockTokenList = []
        for token in tokenList:
            # Decide if token is going to be a Blocking Token
            isBlkToken = True
            if len(token)<minBlkTokenLen:
                isBlkToken = False
            if excludeNumericBlocks and token.isdigit():
                isBlkToken = False
            freq = tokenFreqDict[token]
            if freq < 2 or freq > beta:
                isBlkToken = False
            if isBlkToken:
                blockTokenList.append(token)
        tokenCnt = len(blockTokenList)
        # If there were no blocking tokens, then nothing to do
        if tokenCnt < 1:
            continue
        # When "blockByPairs==True" form all pairs from list
        if blockByPairs:
            if tokenCnt < 2:
                continue
            for j in range(0, tokenCnt-1):
                for k in range(j+1, tokenCnt):
                    tokenJ = blockTokenList[j]
                    tokenK = blockTokenList[k]
                    # Always concatenate pairs in ascending order
                    if tokenJ < tokenK:
                        blockList.append((tokenJ+tokenK, key))
                    else:
                        blockList.append((tokenK+tokenJ, key))
        else:
            for j in range(0, tokenCnt):
                tokenJ = blockTokenList[j]
                blockList.append((tokenJ, key))
    # End of iteration of refDict
    print('Total Records Selected for Reprocessing', selectCnt)
    print('Total Records Selected for Reprocessing', selectCnt, file=logFile)    
    # Sort blockList 
    blockList.sort()
    blockListLen = len(blockList)
    print('Total Blocking Records Created', blockListLen)
    print('Total Blocking Records Created', blockListLen, file=logFile)
    # Phase 2, generate blocks and pairs of refs in each block
    # start by appending a caboose to the end of the blockList
    blockList.append(('XXXXX','X'))
    block = []
    blockPairList = []
    blockCnt = 0
    for j in range(0,blockListLen-1):
        currPair = blockList[j]
        currBlockToken = currPair[0]
        block.append(currPair)
        nextPair = blockList[j+1]
        nextBlockToken = nextPair[0]
        if currBlockToken != nextBlockToken:
            blockLen = len(block)
            if blockLen > 1:
                blockCnt +=1
                for m in range(0,blockLen-1):
                    pairM = block[m]
                    refIDm = pairM[1]                    
                    for n in range(m+1,blockLen):
                        pairN = block[n]
                        refIDn = pairN[1]
                        if refIDm < refIDn:
                            blockPairList.append(refIDm+'|'+refIDn)
                        else:
                            blockPairList.append(refIDn+'|'+refIDm)
            # End of pair generation loop            
            block.clear()   
    # End of j loop
    print('Total Blocks Size>1 Created', blockCnt)
    print('Total Blocks Size>1 Created', blockCnt, file=logFile)    
    # Deduplicate and sort pair list
    print('Total Pairs Generated by Blocks=', len(blockPairList))
    print('Total Pairs Generated by Blocks=', len(blockPairList), file=logFile)
    blockPairList = list(set(blockPairList))
    blockPairList.sort()
    print('Total Unduplicated Pairs =', len(blockPairList))
    print('Total Unduplicated Pairs =', len(blockPairList), file=logFile)     
    return blockPairList

