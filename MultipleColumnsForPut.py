# 单column family 多column qualifier的批量写入

import happybase

if __name__ == '__main__':
    conn = happybase.Connection(host='192.168.2.109')  # 自动连接
    # 创建表
    conn.create_table(
        'sentences',
        {
            'sent': dict()
        }
    )
    table = conn.table('sentences')  # 获取表对象
    path = '/data/procjava/pinjie/wenxue.txt'

    with table.batch(batch_size=1000) as b:
        with open(path, mode='rb') as f:
            for i, line in enumerate(f, 1):
                if i%1000 != 0 :
                    b.put(
                        'wenxue-%d' % int(i/1000+1), {
                            str('sent:%d' % (i % 1000)+1000): line
                        }
                    )
                else:
                    b.put(
                        'wenxue-%d' % int(i / 1000), {
                            str('sent:%d' % (i % 1000+1000)): line
                        }
                    )


'''
file 从1行开始读取
hbase 从第1行开始存

'''
