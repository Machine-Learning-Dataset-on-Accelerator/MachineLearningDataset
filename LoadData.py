#coding=utf-8
"""
filename:       LoadData_New.py
Description:    Load BPECII data from Archiver Channel
Author:         Yusi Qiao, Dengjie Xiao
IDE:            PyCharm
Change:         2019/4/2 0002  14:55    DengjieXiao     Update
                2020/9/2                DengjieXiao     Debug for new engine

"""
import time
import datetime
import numpy as np
import pandas as pd
import xmlrpc.client
import urllib.request,urllib.parse
from interval import Interval
from epics import ca

def datetime2utc(datestr,dtformat='%Y/%m/%d %H:%M:%S'):
    timestamp = time.mktime(datetime.datetime.strptime(datestr, dtformat).timetuple())
    return timestamp

def datetime2utc1(datestr,dtformat='%Y/%m/%d %H:%M:%S'):
    datestr = datestr[-6:]
    timestamp = time.mktime(datetime.datetime.strptime(datestr, dtformat).timetuple())
    return timestamp

def connectChanArch(ipaddr):
    sp = '%s%s%s' % ('http://', ipaddr, '/cgi-bin/archiver/ArchiveDataServer.cgi')
    server = xmlrpc.client.ServerProxy(sp)
    engine = server.archiver.archives()
    return server,engine

def getKeyWithTime(server, engine,pvnames,start,end):
    keypvlist = {}
    namelist = []
    keylists={}
    rekey=None
    for e in engine:
        try:
            namelist = server.archiver.names(e['key'], '')
        except xmlrpc.client.Fault or xmlrpc.client.ProtocolError as err:
            print("A fault occurred")
            print("Fault string: %s" % err.faultString)
        name = {}
        for nl in namelist:
            name[nl['name']] = e['key']
        keypvlist[e['name']] = name
    for pv in pvnames:
        flag = 0
        for key, value in keypvlist.items():
            if pv in value:
                enginestart=datetime2utc1(key.split(':')[1].split('-')[0],'%y%m%d')
                if key.split(':')[1].split('-')[1]=='now':
                    engineend=int(time.time())
                else:
                    engineend=int(datetime2utc1(key.split(':')[1].split('-')[1], '%y%m%d'))
                enginezoom=Interval(enginestart,engineend)
                if (int(datetime2utc(start)) in enginezoom) and (int(datetime2utc(end)) in enginezoom):
                    flag=1
                    print(pv, ":engine name is: ", key, ",engine key is:", value.get(pv))
                    if value.get(pv) in keylists:
                        keylists[value.get(pv)].append(pv)
                    else:
                        keylists[value.get(pv)]=[]
                        keylists[value.get(pv)].append(pv)
                elif (int(datetime2utc(start)) in enginezoom) ^ (int(datetime2utc(end)) in enginezoom):
                    flag=1
                    print('Time period too big,more than mone engine,',pv, ":engine name is: ", key, ",engine key is:", value.get(pv)," Need to be in one engine to export.")
        rekey = keylists
        if flag == 0:
            print(pv, " not found.")
            rekey=None
    return rekey

def getFormatChanArch(server, engine, pvnames, start, end, merge_type='inner', interpolate_type='linear',
                      fillna_type=None, how=0, dropna=True):
    df = pd.DataFrame()
    keylists = getKeyWithTime(server, engine, pvnames, start, end)
    if keylists == {}:
        print('Please change time period.')
        return None
    if how == 0:
        count = 4 * (int(datetime2utc(end)) - int(datetime2utc(start)))
    else:
        count = int(datetime2utc(end)) - int(datetime2utc(start))
    if merge_type.isdigit():
        timeSeries = pd.date_range(start=start, end=end, freq=merge_type + 'S')
        merge_type = 'left'
        df = pd.DataFrame(timeSeries, columns=['time'])
    for key in keylists:
        datalist = server.archiver.values(key, keylists[key], int(datetime2utc(start)), 0, int(datetime2utc(end)), 0,
                                          count, how)
        # print(datalist)
        for l in datalist:
            timelist = []
            valuelist = []
            if len(l.get('values')) == 1:
                gettimestr = str(pd.to_datetime(l.get('values')[0].get('secs'), unit='s') + datetime.timedelta(hours=8))
                gettime = time.mktime(time.strptime(gettimestr, '%Y-%m-%d %H:%M:%S'))
                print(gettime)
                starttime = time.mktime(time.strptime(start, '%Y/%m/%d %H:%M:%S'))
                endtime = time.mktime(time.strptime(end, '%Y/%m/%d %H:%M:%S'))
                if starttime > gettime or endtime < gettime:
                    timelist = list(pd.date_range(start=start, end=end, freq="1" + 'S'))
                    for i in range(len(timelist)):
                        valuelist.append(l.get('values')[0].get('value')[0])
            for d in l.get('values'):
                timelist.append(pd.to_datetime(d.get('secs'), unit='s') + datetime.timedelta(hours=8))
                valuelist.append(d.get('value')[0])
            if df.empty:
                df = pd.DataFrame({'time': timelist, l.get('name'): valuelist}).drop_duplicates('time', keep='first')
            else:
                df = pd.merge(df, pd.DataFrame({'time': timelist, l.get('name'): valuelist}).drop_duplicates('time',
                                                                                                             keep='first'),
                              how=merge_type)
    if (fillna_type != None):
        df = df.set_index(['time']).sort_index(ascending=True).fillna(method=fillna_type)
    else:
        df = df.set_index(['time']).sort_index(ascending=True).interpolate(method=interpolate_type)

    if dropna == True:
        return df.dropna(axis=0)
    else:
        return df

def getFormatChanArch_1(server, engine, pvnames, start, end, merge_type='inner', interpolate_type='linear',
                      fillna_type=None, how=0, dropna=True):
    df = pd.DataFrame()
    result = pd.DataFrame()
    dl = []
    keylists = getKeyWithTime(server, engine, pvnames, start, end)
    if keylists == {}:
        print('Please change time period.')
        return None
    if how == 0:
        count = 4 * (int(datetime2utc(end)) - int(datetime2utc(start)))
    else:
        count = int(datetime2utc(end)) - int(datetime2utc(start))
    if merge_type.isdigit():
        timeSeries = pd.date_range(start=start, end=end, freq=merge_type + 'S')
        merge_type = 'left'
        df = pd.DataFrame(timeSeries, columns=['time'])
    for key in keylists:
        datalist = {}
        datalen = 10000
        newstart = int(datetime2utc(start))
        while datalen == 10000:
            datalist = server.archiver.values(key, keylists[key], newstart, 0, int(datetime2utc(end)), 0, count,
                                              how)
            datalen = len(datalist[0].get('values'))
            # print('len:',datalen)
            # print(datalist[0].get('values')[datalen-1].get('secs'))
            newstart = datalist[0].get('values')[datalen - 1].get('secs')
            dl.append(datalist)
        for d in dl:
            for l in d:
                timelist = []
                valuelist = []
                if len(l.get('values')) == 1:
                    gettimestr = str(
                        pd.to_datetime(l.get('values')[0].get('secs'), unit='s') + datetime.timedelta(hours=8))
                    gettime = time.mktime(time.strptime(gettimestr, '%Y-%m-%d %H:%M:%S'))
                    print(gettime)
                    starttime = time.mktime(time.strptime(start, '%Y/%m/%d %H:%M:%S'))
                    endtime = time.mktime(time.strptime(end, '%Y/%m/%d %H:%M:%S'))
                    if starttime > gettime or endtime < gettime:
                        timelist = list(pd.date_range(start=start, end=end, freq="1" + 'S'))
                        for i in range(len(timelist)):
                            valuelist.append(l.get('values')[0].get('value')[0])
                for d in l.get('values'):
                    timelist.append(pd.to_datetime(d.get('secs'), unit='s') + datetime.timedelta(hours=8))
                    valuelist.append(d.get('value')[0])
                if df.empty:
                    df = pd.DataFrame({'time': timelist, l.get('name'): valuelist}).drop_duplicates('time',
                                                                                                    keep='first')
                else:
                    df = pd.merge(df,
                                  pd.DataFrame({'time': timelist, l.get('name'): valuelist}).drop_duplicates('time',
                                                                                                             keep='first'),
                                  how=merge_type)
            result = pd.concat([result, df])
    if (fillna_type != None):
        result = result.set_index(['time']).sort_index(ascending=True).fillna(method=fillna_type)
    else:
        result = result.set_index(['time']).sort_index(ascending=True).interpolate(method=interpolate_type)
    if dropna == True:
        return result.dropna(axis=0)
    else:
        return result




if __name__ == "__main__":
    ipaddr =  '192.168.44.165'
    server,engine = connectChanArch(ipaddr)
    pvnames = ['BIBPM:R2OBPM07:XPOS']# ["L:PT:P"]#['R3O:BI:DCCT:current'] #'R3IBV04:CISet' 'R3O:BI:DCCT:current','TE-BPM1:POS.PX',
    start = '2019/12/22 08:00:00'
    end = '2019/12/23 20:00:00'
    df = getFormatChanArch_1(server, engine, pvnames, start, end, merge_type='outer', interpolate_type='linear',
                          fillna_type='pad', how=0, dropna=True)
    print (df)




