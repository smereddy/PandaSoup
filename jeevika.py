import urllib.request

import pandas as pd
from bs4 import BeautifulSoup


def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        pass
    
    try:
        import unicodedata
        unicodedata.numeric(s)
        return True
    except (TypeError, ValueError):
        pass
    return False


def get_districts():
    print('Getting all Districts')
    
    resp = urllib.request.urlopen('http://223.30.251.84:9090/dashboard/')
    
    page = BeautifulSoup(resp, 'html.parser')
    
    districts = {}
    for x in page.find(id="district-selector").find_all('option'):
        if x.attrs:
            districts[x.text] = x.attrs['value']
    
    del districts['All']
    
    return districts


def get_blocks(district):
    print('Fetching all blocks for district: %s' % district)
    blocks = {}
    resp = urllib.request.urlopen(
        'http://223.30.251.84:9090/dashboard/reports/blockCboDetails/cbomapping.html?districtId=' + district)
    page = BeautifulSoup(resp, 'html.parser')
    for link in page.findAll('a', href=True):
        if not is_number(link.contents[0].string):
            block = link.get('href')
            blockid = block.split('blockId=')[1]
            blocks[link.contents[0].string] = blockid
    
    return blocks


def get_panchayats(block):
    print('Fetching all panchayats for block: %s' % block)
    panchayats = []
    resp = urllib.request.urlopen(
        'http://223.30.251.84:9090/dashboard/reports/blockCboDetails/cbomappanchayatformedshg.html?reportType=html&blockId=' + block)
    page = BeautifulSoup(resp, 'html.parser')
    for link in page.findAll('a', href=True):
        panchayat = link.get('href')
        if 'panchayatnameformedshg' in panchayat:
            panchayatid = panchayat.split('panchayatId=')[1]
            panchayats.append(panchayatid)
    
    return panchayats


def fetch_block_village(debug):
    if debug:
        districts = {'ARARIA': '188'}
    else:
        districts = get_districts()
    
    jeevika = pd.DataFrame()
    for district, distID in districts.items():
        if debug:
            blocks = {'Narpatganj': '1112'}
        else:
            blocks = get_blocks(distID)
        
        for block, blockid in blocks.items():
            if debug:
                panchayats = ['93760', '97959']
            else:
                panchayats = get_panchayats(blockid)
            
            for panchayatID in panchayats:
                print('Fetching Villahe SGH info for panchayat: %s' % block)
                html = "http://223.30.251.84:9090/dashboard/reports/blockCboDetails/panchayatnameformedshg.html?reportType=html&panchayatId=" + panchayatID
                lis = pd.read_html(html)
                length = len(lis)
                for i in range(length):
                    if i == 0:
                        continue
                    else:
                        try:
                            df_v1 = lis[i]
                            df_v2 = df_v1.iloc[4:, 2:16]
                            jeevika = jeevika.append(df_v2)
                        except:
                            continue
    
    jeevika.columns = ['District Name',
                       'BLOCK NAME',
                       'PANCHAYAT NAME',
                       'VILLAGE NAME',
                       'MOHALLA NAME',
                       'SHG NAME',
                       'FORMATION DATE',
                       'Total Member',
                       'SC',
                       'ST',
                       'EBC / SC',
                       'OTHERS',
                       'SAVING ACCOUNTS',
                       'LOAN ACCOUNTS']
    jeevika = jeevika[jeevika['District Name'].notna()]
    print('Dumping data into CSV')
    filename = 'village.csv'
    jeevika.to_csv(filename)


def fetch_panchayat_SHG():
    if debug:
        districts = {'ARARIA': '188'}
    else:
        districts = get_districts()
    
    jeevika = pd.DataFrame()
    for district, distID in districts.items():
        if debug:
            blocks = {'Narpatganj': '1112'}
        else:
            blocks = get_blocks(distID)
        for block, blockid in blocks.items():
            print('Fetching SGH info for block: %s' % block)
            html = "http://223.30.251.84:9090/dashboard/reports/blockCboDetails/cbomappanchayatformedshg.html?reportType=html&blockId=" + blockid
            lis = pd.read_html(html)
            try:
                df_v1 = lis[1]
                df_v2 = df_v1.iloc[4:, 1:3]
                df_v2 = df_v2.iloc[:-2, :]
                df_v2['district'] = district
                df_v2['block'] = block
                jeevika = jeevika.append(df_v2)
            except:
                continue
    
    jeevika.columns = ['Panchayat', 'SHG Formed', 'District', 'Block']
    print('Dumping data into CSV')
    filename = 'panchayat.csv'
    jeevika.to_csv(filename)


def fetch_block_SHG():
    if debug:
        districts = {'ARARIA': '188'}
    else:
        districts = get_districts()
    jeevika = pd.DataFrame()
    for district, distID in districts.items():
        html = "http://223.30.251.84:9090/dashboard/reports/blockCboDetails/cbomapping.html?districtId=" + distID
        print('Fetching SGH info for district: %s' % district)
        lis = pd.read_html(html)
        try:
            df_v1 = lis[1]
            df_v2 = df_v1.iloc[4:, 1:4]
            df_v2 = df_v2.iloc[:-1, :]
            df_v2['district'] = district
            jeevika = jeevika.append(df_v2)
        except:
            continue
    
    jeevika.columns = ['Block', 'SHG Formed', 'Member Under', 'District']
    print('Dumping data into CSV')
    filename = 'jeevikablock.csv'
    jeevika.to_csv(filename)


def specific_blocks():
    blocks = ['1200', '1082', '1278', '1096', '1130', '1270', '1210', '1131', '1135', '1272', '1089', '1088', '1302',
              '1209', '1048', '1288', '1274', '1276', '1279', '1199', '1322', '1283', '1268', '1092', '1051', '1187',
              '1299', '1428', '1083', '1094', '1225', '1208', '1099']
    jeevika = pd.DataFrame()
    
    for blockid in blocks:
        print('Fetching panchayats Blocks: %s' % blockid)
        
        panchayats = get_panchayats(blockid)
        
        for panchayatID in panchayats:
            print('Fetching Villahe SGH info for panchayat: %s' % panchayatID)
            html = "http://223.30.251.84:9090/dashboard/reports/blockCboDetails/panchayatnameformedshg.html?reportType=html&panchayatId=" + panchayatID
            lis = pd.read_html(html)
            length = len(lis)
            for i in range(length):
                if i == 0:
                    continue
                else:
                    try:
                        df_v1 = lis[i]
                        df_v2 = df_v1.iloc[4:, 2:16]
                        jeevika = jeevika.append(df_v2)
                    except:
                        continue
    
    jeevika.columns = ['District Name',
                       'BLOCK NAME',
                       'PANCHAYAT NAME',
                       'VILLAGE NAME',
                       'MOHALLA NAME',
                       'SHG NAME',
                       'FORMATION DATE',
                       'Total Member',
                       'SC',
                       'ST',
                       'EBC / SC',
                       'OTHERS',
                       'SAVING ACCOUNTS',
                       'LOAN ACCOUNTS']
    jeevika = jeevika[jeevika['District Name'].notna()]
    print('Dumping data into CSV')
    filename = 'village.csv'
    jeevika.to_csv(filename)


if __name__ == "__main__":
    '''
    Set Debug to "True" is you want to test this script
    Else to "False" to run normally
    '''
    debug = True
    # fetch_block_SHG(debug)
    # fetch_panchayat_SHG(debug)
    # fetch_block_village(debug)
    specific_blocks()
