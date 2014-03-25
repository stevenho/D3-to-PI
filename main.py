import os
from pandas.io.excel import ExcelWriter
from pandas.io.excel import ExcelFile
import pandas as pd
import csv


def selectMDL(file, logdir):
    logfiles = sorted([f for f in os.listdir(logdir) if f.startswith(file)])
    return logfiles[-1]


def processBlock(blockType, outDataFrame):
    if blockType == 'AI':
        df = xls.parse(blockType)

        df['Tag'] = df['EPN']
        df['descriptor'] = df['DESC'].apply(lambda x: ' '.join(x.replace('"', ' ').split()).title())
        df['instrumenttag'] = df['EPN'] + ':0:AI_MEAS'
        df['engunits'] = df['EGUTAG'].apply(lambda x: str(x).title())
        df = df.join(df['EGU'].apply(lambda x: pd.Series(x.split(), dtype='float32', index=['zero', 'span'])))
        df['span'] = df['span'] - df['zero']

        df = generateDefaultDF(df, 'float32', dataQualityChecking=True)
        outDataFrame = outDataFrame.append(df)
    elif blockType == 'DIN' or blockType == 'DOT':
        df = xls.parse(blockType)

        df['Tag'] = df['EPN']
        df['descriptor'] = df['DESC'].apply(lambda x: ' '.join(x.replace('"', ' ').split()).title())
        df['instrumenttag'] = df['EPN'] + ':0:' + blockType + '_VAL'
        df['digitalset'] = df['DIGTAG'].apply(
            lambda x: blockType + '_' + '/'.join(x.split('" "')).replace('"', '').replace(' ', ''))

        df = generateDefaultDF(df, 'digital', dataQualityChecking=True)
        outDataFrame = outDataFrame.append(df)
    elif blockType == 'DEV':
        df = xls.parse(blockType)

        df['Tag'] = df['EPN']
        df['descriptor'] = df['DESC'].apply(lambda x: ' '.join(x.replace('"', ' ').split()).title())
        df['instrumenttag'] = df['EPN'] + ':0:DEV_STAT'
        df['digitalset'] = 'MDV_' + df['MDV'] + '_STAT'

        df = generateDefaultDF(df, 'digital', dataQualityChecking=True, bitMaskAndReturnFirstBit=True)
        outDataFrame = outDataFrame.append(df)

        df['Tag'] = df['EPN'] + '.command'
        df['instrumenttag'] = df['EPN'] + ':0:DEV_CMD'
        df['digitalset'] = 'MDV_' + df['MDV'] + '_CMND'

        outDataFrame = outDataFrame.append(df)
    elif blockType == 'OUT':
        ##
        # Example of how to rapidly populate PI tags for secondary D/3 processing blocks.
        ##
        df = xls.parse(blockType)
 
        df['BLOCK'] = df['BLOCK'].apply(str)
        df['Tag'] = df['EPN'] + '.output' + df['BLOCK']
        df['descriptor'] = 'df['EPN'] + %Output'
        df['instrumenttag'] = df['EPN'] + ':' + df['BLOCK'] + ':CB_OTVL'
        df['engunits'] = '%'
        df['zero'] = df['MIN%']
        df['span'] = df['MAX%'] - df['zero']
 
        df = generateDefaultDF(df, 'float32')
        outDataFrame = outDataFrame.append(df)
    else:
        pass

    return outDataFrame


def generateDefaultDF(df, pointtype, dataQualityChecking=False, bitMaskAndReturnFirstBit=False):
    df['location1'] = interfaceID
    df['location2'] = 0

    if dataQualityChecking:
        ###
        # Data quality checking only applies for AI, DEV, DIN, DOT, and DGR
        # location3 == 2, block scan setting and ALMCT field is used for quality check
        # location3 == 1, block scan setting is used for quality check
        # location3 == 0, no quality checks
        ###
        df['location3'] = 2
    else:
        df['location3'] = 1
    df['location4'] = scanClass

    if bitMaskAndReturnFirstBit:
        ###
        # location5 == -1, pick the position of the first bit. See interface documention for details as other numeric values will mask bits.
        #
        # Value of -1 for applies if DEV status/command is expected to have only active state at any given moment.
        # Will not work when short int datatype will have multiple active bits (for example block maintanance or fault flag).
        ###
        df['location5'] = -1
    else:
        df['location5'] = 0

    if pointtype == 'float32':
        df['pointtype'] = 'float32'
        df['compressing'] = 1
        df['step'] = 0

        df['digitalset'] = ''
    else:
        df['pointtype'] = 'digital'
        df['compressing'] = 0
        df['engunits'] = 'STATE'
        df['step'] = 1

        df['zero'] = 0
        df['span'] = 128

    df['Select (x)'] = ''
    df['archiving'] = '1'
    df['changedate'] = ''
    df['changer'] = 'piadmin'
    df['compdev'] = ''
    df['compdevpercent'] = .2
    df['compmax'] = 28800
    df['compmin'] = 0
    df['convers'] = 1
    df['creationdate'] = ''
    df['creator'] = df['changer']
    df['datasecurity'] = 'piadmin: A(r,w) | piadmins: A(r,w) | PIWorld: A(r)'
    df['displaydigits'] = -5
    df['exdesc'] = 'D3C'
    df['ptclassname'] = 'classic'
    df['pointsource'] = 'D3'
    df['excdev'] = ''
    df['excdevpercent'] = .1
    df['excmax'] = 300
    df['excmin'] = 0
    df['filtercode'] = 0
    df['ptsecurity'] = df['datasecurity']
    df['scan'] = 1
    df['shutdown'] = 0
    df['sourcetag'] = ''
    df['squareroot'] = 0
    df['srcptid'] = 0
    df['totalcode'] = 0
    df['typicalvalue'] = df['zero']
    df['userint1'] = 0
    df['userint2'] = 0
    df['userreal1'] = 0
    df['userreal2'] = 0

    df['pointid'] = ''
    df['recno'] = ''

    return df


def processDigitalSet(digitalSetType, outDataFrame):
    if digitalSetType == 'DIN' or digitalSetType == 'DOT':
        df = xls.parse(digitalSetType)
        df['Digital State Set'] = df['DIGTAG'].apply(
            lambda x: digitalSetType + '_' + '/'.join(x.split('" "')).replace('"', '').replace(' ', ''))
        df['Digital States'] = df['DIGTAG'].apply(lambda x: x[1:-1].split('" "'))
        outDataFrame = outDataFrame.append(df[['Digital State Set', 'Digital States']])
    elif digitalSetType == 'MDV':
        df = xls.parse(digitalSetType)
        df['Digital State Set'] = 'MDV_' + df['EPN'] + '_STAT'
        df['Digital States'] = df['STATS'].fillna('').apply(lambda x: filter(lambda y: y.strip(), x[1:-1].split('" "')))
        outDataFrame = outDataFrame.append(df[['Digital State Set', 'Digital States']])

        df['Digital State Set'] = 'MDV_' + df['EPN'] + '_CMND'
        df['Digital States'] = df['CMNDS'].fillna('').apply(lambda x: filter(lambda y: y.strip(), x[1:-1].split('" "')))
        outDataFrame = outDataFrame.append(df[['Digital State Set', 'Digital States']])
    else:
        pass

    return outDataFrame


def finalizeDigitalSet(digitalSetsDF):
    digitalSetsDF['lengthCheck'] = digitalSetsDF['Digital States'].apply(lambda x: len(x))
    digitalSetsDF = digitalSetsDF[digitalSetsDF['lengthCheck'] > 0]
    digitalSetsDF['Digital States'] = digitalSetsDF['Digital States'].apply(lambda x: [s.strip() for s in x])
    digitalSetsDF['Digital States'] = digitalSetsDF['Digital States'].apply(lambda x: ','.join(x))
    digitalSetsDF = digitalSetsDF.drop_duplicates('Digital State Set', 'Digital States')

    digitalSetsDF.to_csv('ds_temp.csv', index=False, cols=['Digital State Set', 'Digital States'])

    reader = csv.reader(open('ds_temp.csv'))
    writer = open('DigitalSets.csv', 'w')

    for line in reader:
        if line[1].startswith('MDV_'):
            line.insert(2, "No_State")
        writer.write(','.join(line) + '\n')
    writer.close()

    return digitalSetsDF


if __name__ == '__main__':
    files = ['pcm0']  # [ ..., 'pcm1', 'pcm2', ... ]
    logdir = 'log'

    blockTypes = ['AI', 'DIN', 'DEV', 'DOT']
    digitalSetTypes = ['DIN', 'MDV', 'DOT']

    #constants go here
    interfaceID = 2
    scanClass = 1

    tagConfig = pd.DataFrame()
    digitalSetsDF = pd.DataFrame()

    for f in files:
        sourceFile = selectMDL(f, logdir)
        print "Selected: " + sourceFile
        xls = ExcelFile(os.getcwd() + '/' + logdir + '/' + sourceFile)

        for block in blockTypes:
            tagConfig = processBlock(block, tagConfig)

        for digitalSet in digitalSetTypes:
            digitalSetsDF = processDigitalSet(digitalSet, digitalSetsDF)

    finalizeDigitalSet(digitalSetsDF)

    print "Done"
    tagConfig.to_csv('TagConfig.csv', index=False,
                     cols=['Select (x)', 'Tag', 'archiving', 'changedate', 'changer', 'compdev', 'compdevpercent',
                           'compmax', 'compmin', 'compressing', 'convers', 'creationdate', 'creator', 'datasecurity',
                           'descriptor', 'digitalset', 'displaydigits', 'engunits', 'excdev', 'excdevpercent', 'excmax',
                           'excmin', 'exdesc', 'filtercode', 'instrumenttag', 'location1', 'location2', 'location3',
                           'location4', 'location5', 'pointid', 'pointsource', 'pointtype', 'ptclassname', 'ptsecurity',
                           'recno', 'scan', 'shutdown', 'sourcetag', 'span', 'squareroot', 'srcptid', 'step',
                           'totalcode', 'typicalvalue', 'userint1', 'userint2', 'userreal1', 'userreal2', 'zero'])
