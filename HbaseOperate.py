# -*- coding;utf-8 -*-

from thrift.transport import TSocket
from thrift.protocol import TBinaryProtocol
from thrift.transport import TTransport
from hbase import Hbase

import struct


# 将Python的值根据格式符，转换为字符串（因为Python中没有字节(Byte)类型，
# 可以把这里的字符串理解为字节流，或字节数组）
# 其函数原型为：struct.pack(fmt, v1, v2, ...)，参数fmt是格式字符串，关于格式字符串的相关信息在下面有所介绍
# v1, v2, ...表示要转换的python值
def encode(n):
    return struct.pack('i',n)


# 将字节流转换成python数据类型
# 它的函数原型为：struct.unpack(fmt, string)，该函数返回一个元组
def decode(s):
    return int(s) if s.isdigit() else struct.unpack('i',s)[0]


class HbaseOperate(object):
    def __init__(self,table='t1',host='localhost',port=9090):
        self.table = table
        self.host = host
        self.port = port

        # 连接hbase thrift server
        self.transport = TTransport.TBufferedTransport(TSocket.TSocket(host, port))
        self.protocol = TBinaryProtocol.TBinaryProtocolAccelerated(self.transport)

        # 创建客户端实例，打开连接
        self.client = Hbase.Client(self.protocol)
        self.transport.open()

        # 设置列族类型和字段
        self.set_column_families([str, str, int], ['cf1', 'cf2', 'cf3'])
        self._build_column_families()

    def set_column_families(self, type_list, col_list=['cf1', 'cf2', 'cf3']):
        self.columnFamiliesType = type_list
        self.columnFamilies = col_list

    def _build_column_families(self):
        tables = self.client.getTableNames()
        if self.table not in tables:
            self.__create_table(self.table)

    def __create_table(self, table):
        columnFamilies = []
        for columnFamily in self.columnFamilies:
            name = Hbase.ColumnDescriptor(name=columnFamily)
            columnFamilies.append(name)
        self.client.createTable(table, columnFamilies)

    def __del__(self):
        self.transport.close()

    def __def_table(self, table):
        self.client.disableTable(table)
        self.client.deleteTable(table)

    def getColumnDescriptors(self):
        return self.client.getColumnDescriptors(self.table)

    def put(self, rowKey, qualifier='0', *args):
        mutations = []
        for j,column in enumerate(args):
            if isinstance(column,str):
                m_name = Hbase.Mutation(column=self.columnFamilies[j]+':'+qualifier, value=column)
            elif isinstance(column,int):
                m_name = Hbase.Mutation(column=self.columnFamilies[j] + ':' + qualifier, value=encode(column))
                mutations.append(m_name)
            self.client.mutateRow(self.table, rowKey, mutations, {})

    def puts(self, rowKeys, values, qualifier='1'):
        mutationsBatch = []
        if not isinstance(rowKeys, list):
            rowKeys = [rowKeys] * len(values)

        for i, value in enumerate(values):
            mutations = []
            for j, column in enumerate(value):
                if isinstance(column, str):
                    m_name = Hbase.Mutation(column=self.columnFamilies[j] + ':' + qualifier, value=column)
                elif isinstance(column, int):
                    m_name = Hbase.Mutation(column=self.columnFamilies[j] + ':' + qualifier, value=encode(column))
                mutations.append(m_name)

            qualifier = str(int(qualifier) + 1)
            mutationsBatch.append(Hbase.BatchMutation(row=rowKeys[i], mutations=mutations))
        self.client.mutateRows(self.table, mutationsBatch, {})

    def getRow(self, row, qualifier='0'):
        rows = self.client.getRow(self.table, row, {})
        ret = []
        for r in rows:
            rd = {'row': r.row}
            for j, column in enumerate(self.columnFamilies):
                if self.columnFamiliesType[j] == str:
                    rd.update({column: r.columns.get(column + ':' + qualifier).value})
                elif self.columnFamiliesType[j] == int:
                    rd.update({column: decode(r.columns.get(column + ':' + qualifier).value)})
            ret.append(rd)
        return ret

    def getRows(self, rows, qualifier='0'):
        grow = True if len(set(rows)) == 1 else False

        for r in rows:
            yield self.getRow(r, qualifier)
            if grow: qualifier = str(int(qualifier) + 1)

    def scanner(self, numRows=100, startRow=None, stopRow=None):
        scan = Hbase.TScan(startRow, stopRow)
        scannerId = self.client.scannerOpenWithScan(self.table, scan, {})
        #        row = self.client.scannerGet(scannerId)

        ret = []
        rowList = self.client.scannerGetList(scannerId, numRows)
        while rowList:
            for r in rowList:
                rd = {'row': r.row}
                for k, v in r.columns.iteritems():
                    cf, qualifier = k.split(':')
                    if qualifier not in rd:
                        rd[qualifier] = {}

                    idx = self.columnFamilies.index(cf)
                    if self.columnFamiliesType[idx] == str:
                        rd[qualifier].update({cf: v.value})
                    elif self.columnFamiliesType[idx] == int:
                        rd[qualifier].update({cf: decode(v.value)})

                ret.append(rd)

            rowList = self.client.scannerGetList(scannerId, numRows)

        self.client.scannerClose(scannerId)
        return ret


def demo():
    ht = HbaseOperate(table='test1')
    values = [['lee', 'f', '27'], ['clark', 'm', 27], ['dan', 'f', '27']]
    rowKey = 'cookie'
    #    ht.put(rowKey, '0', 'fish', 'f', '22')
    #    ht.puts(rowKey, values)
    #    print ht.getColumnDescriptors()

    print(ht.getRow(rowKey))
    for i in ht.getRows([rowKey] * 4):
        print(i)
    print(ht.scanner())


if __name__ == '__main__':
    demo()


