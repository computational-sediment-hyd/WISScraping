#!/usr/bin/env python
# coding: utf-8

# # WIS Scraping Tools
# 
# [水文水質データベース](http://www1.river.go.jp/)のダウンロード支援のツール群です。

# ## License 

# ## Source code

# In[ ]:


import pandas as pd
import geopandas as gpd
import numpy as np
from bs4 import BeautifulSoup
from urllib import request, error
from dateutil.relativedelta import relativedelta
import datetime
from shapely.geometry import Point
import requests


# In[ ]:


__version__ = '0.6.0'


# ### 全ての水系のIDを取得

# In[ ]:


def getAllSuikeis():
    """
    Get all suikei informations
    
    Returns
    -------
    df : pandas.DataFrame
        pandas.DataFrame of all suikei infomations
    
    """
    
    url = 'http://www1.river.go.jp/cgi-bin/SrchSite.exe?KOMOKU=02&NAME=&SUIKEI=&KASEN=&KEN=-1&CITY=&PAGE=0'
    response = request.urlopen(url)
    soup = BeautifulSoup(response, features="lxml")
    response.close()
    
    r1 = soup.findAll("td")[10]
    name  = []
    number = []
    for r2 in r1.findAll("option"):
        name.append(r2.text.replace('\n','').strip() )
        number.append( r2['value'] )
    
    return pd.DataFrame({'name':name, 'id':number})


# ### 観測所の詳細を取得

# In[ ]:


def _getVerbose(i):
    
    """
    Get station detailed informations in suikei
    
    Parameters
    ----------
    i : str
        station identifier
    
    Returns
    -------
    lon : float
        longitude
    lat : float
        latiitude
    zero : str
        elevation of zero point
    """
    
    url = 'http://www1.river.go.jp/cgi-bin/SiteInfo.exe?ID=' + i
    # dfs = pd.read_html(url, index_col=[0]) #error:pandas 2.2.2
    dfs = pd.read_html(requests.get(url).content, index_col=[0]) 
    v = dfs[0].loc['緯度経度'].values[0]
    
    if '度' in str(v):
        lat = float(v[3:5])   + float(v[6:8])/float(60)   + float(v[9:11])/float(3600) 
        lon = float(v[16:19]) + float(v[20:22])/float(60) + float(v[23:25])/float(3600)
    else:
        lon = ''
        lat = ''
        
    if '最新の零点高' in dfs[0].index:
        zero = dfs[0].loc['最新の零点高'].values[0]
    else:
        zero = ''
    
    return lon, lat, zero


# ### pandas dfより数値以外の値を取得

# In[ ]:


def _getNonnumericValue(df):
    p = r'[-+]?(\d+\.?\d*|\.\d+)([eE][-+]?\d+)?'
    
    df = df.astype(str)
    out = np.empty(0)
    for n in df.columns:
    #数字以外の文字を抽出
        a = df[df[n].str.fullmatch(p)==False][n].values
        out = np.append( out, np.unique(a) )
        out = np.unique(out)

    return out


# ### 数値以外の値を置換

# In[ ]:


def _replaceNonnumericValue(df, lnonnum, nonnumVal):
    
    df = df.astype(str)
    
    for l in lnonnum:
        df = df.replace(l, nonnumVal)
        
#     df = df.astype(float)
    
    return df


# ### 任意水系の観測所情報を取得

# In[ ]:


def getAllStationsInSuikei(suikeiid='', kind='水位流量', verbose=False, GeoDataFrame=False, epsg=int(6668)):
    
    """
    Get all stations informations in suikei
    
    Parameters
    ----------
    suikeid : str , default ''
        suikei identifier
    kind : str , default '水位流量'
        kind of data
    verbose : boolean, default False
        Detailed information output or not
    GeoDataFrame : boolean, default False
        to GeoDataFrame or not
    epsg : int, default 6668
        epsg number   
    
    Returns
    -------
    df : pandas.DataFrame or geopandas.GeoDataFrame
        pandas.DataFrame or geopandas.GeoDataFrame of all river infomations
    """
    
    if GeoDataFrame & (verbose==False) : print('error : Be sure to set verbose=True if GeoDataFrame=True.')
    
    v0 = [] #観測項目
    v1 = [] #水系名
    v2 = [] #河川名
    v3 = [] #観測所名
    v4 = [] #所在地
    v5 = [] #観測所記号
    v6 = [] #緯度
    v7 = [] #経度
    v8 = [] #最新の零点高
    
    if kind == '水位流量':
        kindt = '02'
    elif kind == '雨量':
        kindt = '01'
    elif kind == 'ダム':
        kindt = '05'
    else:
        return 'error'
        
    pp = 0
    isloop = True
    while isloop :
        url = 'http://www1.river.go.jp/cgi-bin/SrchSite.exe?KOMOKU=' + kindt + '&NAME=&SUIKEI=' \
              + suikeiid + '&KASEN=&KEN=-1&CITY=&PAGE=' + str(pp)
        
        response = request.urlopen(url)
        soup = BeautifulSoup(response, features="lxml")
        response.close()
        
        table = soup.findAll("table")
        
        for trsp in table[2].findAll("tr")[1:]:
            tds = trsp.findAll("td")
            
            if len(tds[2].text) == 1 : 
                isloop = False
                break 
            
            v0.append( tds[1].text )
            v1.append( tds[2].text )
            v2.append( tds[3].text )
            v3.append( tds[4].text )
            v4.append( tds[5].text )
            val = tds[4].find('a').get('href').split('\'')[1]
            v5.append( val )
            
            if verbose:
                lon, lat, zero = _getVerbose(val)
                v6.append(lon) 
                v7.append(lat)
                if kind == '水位流量' : v8.append(zero)
                
        pp += 1
    
    
    if verbose:
        df = pd.DataFrame( {'観測項目'  :v0
                          , '水系名'    :v1
                          , '河川名'    :v2
                          , '観測所名'  :v3
                          , '所在地'    :v4
                          , '観測所記号':v5
                          , 'lon':v6
                          , 'lat':v7
                          })
        
        if kind == '水位流量' : df['最新の零点高'] = v8
            
        if GeoDataFrame:
            geo = []
            for i, d in df.iterrows():
                if (type(d['lon'])==str) or (type(d['lat'])==str):
                    geo.append(Point())
                else:
                    geo.append(Point(d['lon'],d['lat']))
            
            df = gpd.GeoDataFrame(df, geometry=geo, crs=f'epsg:{str(epsg)}')        
    else:
        df = pd.DataFrame( {'観測項目'  :v0
                          , '水系名'    :v1
                          , '河川名'    :v2
                          , '観測所名'  :v3
                          , '所在地'    :v4
                          , '観測所記号':v5} )
    return df


# ### 観測所の観測データを取得（水位or流量or雨量、1時間間隔）

# In[ ]:


def getRiverVariable(stationid, startTime, endTime, kind, verbose=False, nonumericdata=False, returnednonumericdata=False):
# 0.6.0 update 
# 出力をdictionary型に変更

    """
    Get river variables
    
    Parameters
    ----------
    stationid : str
        station identifier
    startTime : str
        Start time of data acquisition : %Y/%m/%d'
    endTime : str
        End time of data acquisition : %Y/%m/%d'
    kind : str
        "水位" or "流量" or "雨量"
    verbose : boolean, default False
        Detailed information output or not
    nonumericdata : default False
        value replaced with nonumeric value
    returnednonumericdata : boolean, default False
        List of nonumeric value output or not
    
    Returns
    -------
    dout : Dictionary
    
    """

    if kind == '水位':
        DSP = 'DspWaterData.exe'
        ikind = str(2) 
    elif kind == '流量':
        DSP = 'DspWaterData.exe'
        ikind = str(6) 
    elif kind == '雨量':
        DSP = 'DspRainData.exe'
        ikind = str(2) 
    else:
        print('error')
        return
        
    st0 = datetime.datetime.strptime(startTime, '%Y/%m/%d')
    st = st0
    et = datetime.datetime.strptime(endTime, '%Y/%m/%d')
    
    vs = []
    while (et+relativedelta(months=1)) > st :
        
        strtime = str(st.year) +  str(st.month).zfill(2) + '01'
        
        url = 'http://www1.river.go.jp/cgi-bin/' + DSP + '?KIND=' + ikind + '&ID=' \
            + stationid + '&BGNDATE=' + strtime + '&ENDDATE=21001231&KAWABOU=NO'
        
        # dfs = pd.read_html(url, skiprows=2, index_col=[0]) #error:pandas 2.2.2
        dfs = pd.read_html(requests.get(url).content, skiprows=2, index_col=[0])         
        
        dfsp = dfs[0]
        if kind == '水位': dfsp = dfsp.drop(dfsp.index[-1])
        vs.append( dfsp.values.flatten() )
        
        st += relativedelta(months=1)
    
    val = np.hstack(vs)
    stind = datetime.datetime(st0.year, st0.month, 1, 1, 0)
    df = pd.DataFrame({kind:val}
                 , index= pd.date_range(stind, periods=len(val), freq='H') )
    
    df = df[st0:et]
    
    dout = {}
    
    if (nonumericdata!=False) or returnednonumericdata :
        lnonnum = _getNonnumericValue(df)
        
        if returnednonumericdata : dout['NonNumericValue'] = lnonnum
            
        if nonumericdata!=False : df = _replaceNonnumericValue(df, lnonnum, nonumericdata)
    
    dout['dataframe'] = df
    
    if verbose:
        lon, lat, zero = _getVerbose(stationid)
        dout['lon'] = lon
        dout['lat'] = lat
        dout['最新の零点高'] = zero
        
    return dout


# ### 全てのダムのIDを取得

# In[ ]:


def getAllDams(verbose=False, GeoDataFrame=False, epsg=int(6668)):
    """
    Get all dam informations
    
    Parameters
    ----------
    verbose : boolean, default False
        Detailed information output or not
    GeoDataFrame : boolean, default False
        to GeoDataFrame or not
    epsg : int, default 6668
        epsg number   
    
    Returns
    -------
    df : pandas.DataFrame or geopandas.GeoDataFrame
        pandas.DataFrame or geopandas.GeoDataFrame of all dam infomations
    """
    
    v1 = [] #水系名
    v2 = [] #河川名
    v3 = [] #観測所名
    v4 = [] #所在地
    v5 = [] #観測所記号
    v6 = [] #緯度
    v7 = [] #経度

    pp = 0
    isloop = True
    while isloop :
        url = 'http://www1.river.go.jp/cgi-bin/SrchSite.exe?KOMOKU=05&NAME=&SUIKEI=&KASEN=&KEN=-1&CITY=&PAGE=' + str(pp)
        
        response = request.urlopen(url)
        soup = BeautifulSoup(response, features="lxml")
        response.close()
        
        table = soup.findAll("table")
        
        for trsp in table[2].findAll("tr")[1:]:
            tds = trsp.findAll("td")
            
            if len(tds[2].text) == 1 : 
                isloop = False
                break 
            
            v1.append( tds[2].text )
            v2.append( tds[3].text )
            v3.append( tds[4].text )
            v4.append( tds[5].text )
            tp = tds[4].findAll("a")[0]
            num = tp.get('href').split("'")[1]
            v5.append( num )
            
            if verbose:
                lon, lat, _ = _getVerbose(num)
                v6.append(lon) 
                v7.append(lat)
            
        pp += 1
    
    if verbose:
        df = pd.DataFrame( {
                            '水系名'    :v1
                          , '河川名'    :v2
                          , '観測所名'  :v3
                          , '所在地'    :v4
                          , '観測所記号':v5
                          , 'lon':v6
                          , 'lat':v7 } )
        
        if GeoDataFrame:
            geo = []
            for i, d in df.iterrows():
                if (type(d['lon'])==str) or (type(d['lat'])==str):
                    geo.append(Point())
                else:
                    geo.append(Point(d['lon'],d['lat']))
            
            df = gpd.GeoDataFrame(df, geometry=geo, crs=f'epsg:{str(epsg)}')        
            
    else:
        df = pd.DataFrame( {
                            '水系名'    :v1
                          , '河川名'    :v2
                          , '観測所名'  :v3
                          , '所在地'    :v4
                          , '観測所記号':v5} )
            
    return df


# ### ダムの観測データを取得（1時間間隔）

# In[ ]:


def getDamVariables(damid, startTime, endTime, verbose=False, nonumericdata=False, returnednonumericdata=False):
    # 0.6.0 update 
    # 出力をdictionary型に変更
    
    """
    Get dam variables
    
    Parameters
    ----------
    damid : str
        dam identifier
    startTime : str
        Start time of data acquisition : %Y/%m/%d'
    endTime : str
        End time of data acquisition : %Y/%m/%d'
    verbose : boolean, default False
        Detailed information output or not
    nonumericdata : default False
        value replaced with nonumeric value
    returnednonumericdata : boolean, default False
        List of nonumeric value output or not
    
    Returns
    -------
    dout : Dictionary
    
    """
    
    st0 = datetime.datetime.strptime(startTime, '%Y/%m/%d')
    st = st0
    et = datetime.datetime.strptime(endTime, '%Y/%m/%d')
    
    ldf = []
    while (et+relativedelta(months=1)) > st :
        strStime = str(st.year) +  str(st.month).zfill(2) + '01'
        ett = st + relativedelta(months=1, days=-1)
        strEtime = str(ett.year) +  str(ett.month).zfill(2) + str(ett.day).zfill(2)
        
        url = 'http://www1.river.go.jp/cgi-bin/DspDamData.exe?KIND=1&ID=' + damid \
                + '&BGNDATE=' + strStime + '&ENDDATE=' + strEtime + '&KAWABOU=NO'
        
        response = request.urlopen(url)
        soup = BeautifulSoup(response, features="lxml")
        
        tmp = soup.findAll("iframe")[0]['src']
        url2 = 'http://www1.river.go.jp' + tmp
        
#        The maximum number of accesses is set to 20 times, as it may take time to issue the URL.
#         response = request.urlopen(url2)
        for n in range(20):
            try:
                response = request.urlopen(url2)
            except error.HTTPError as e:
                continue
            else:
                # read() command can only be executed once.
                dfs = pd.read_html(response.read()) #error:pandas 2.2.2

                if len(dfs)==0 : break
                    
                df = dfs[0] 
                ldf.append(df)
                break

        else :
            print(st)
            print('error code:' + e.code)
        
        st += relativedelta(months=1)
        
    dout = {}

    if len(ldf)==0 : 
        dout['dataframe'] = None
    else:
        df = pd.concat(ldf)
        # change 24:00 
        t0 = pd.to_datetime(df.iloc[:,0].values)
        t1 = np.array( [pd.Timedelta(hours=int(v2.split(':')[0]), minutes=int(v2.split(':')[1])) for v2 in df.iloc[:,1].values] )
        df.index = pd.to_datetime([t0p + t1p for t0p, t1p in zip(t0, t1)])
        df = df.iloc[:,2:7]
    #     df.columns = ['rainfall','volume','discharge:in','discharge:out','volume[%]']    
        df.columns = ['流域平均雨量[mm/h]','貯水量[×10^3 m3]','流入量[m3/s]','放流量[m3/s]','貯水率[%]']
        
        df = df[st0:et]
        
        if (nonumericdata!=False) or returnednonumericdata :
            lnonnum = _getNonnumericValue(df)
            
            if returnednonumericdata : dout['NonNumericValue'] = lnonnum
                
            if nonumericdata!=False : df = _replaceNonnumericValue(df, lnonnum, nonumericdata)
        
        dout['dataframe'] = df
    
    if verbose:
        lon, lat, zero = _getVerbose(stationid)
        dout['lon'] = lon
        dout['lat'] = lat
        dout['最新の零点高'] = zero
        
    return dout

