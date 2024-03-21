import numpy as np
import pymysql
import yaml
import pandas as pd
from sqlalchemy import create_engine
import pymysql


class W_RBase:

    def __init__(self):
        with open(r'E:\PyCode\FactorsAnalysis\mysql_config.yaml', mode="r", encoding='utf-8') as f:
            mysql_config = yaml.safe_load(f)
        self.user = mysql_config['mysql']['user']
        self.password = mysql_config['mysql']['password']
        self.host = mysql_config['mysql']['host']
        self.port = mysql_config['mysql']['port']
        self.conn = None
        self.cursor = None

    def __enter__(self):
        """
        上下文管理魔术方法
        :return:
        """
        self.connection()
        print('链接成功')
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        上下文管理魔术方法
        :param exc_type:
        :param exc_val:
        :param exc_tb:
        :return:
        """
        self.close()

    def connection(self):
        """
        sql server 数据库连接
        :return:
        """
        try:
            self.conn = pymysql.connect(host=self.host,
                                        user=self.user,
                                        password=self.password,
                                        port=self.port)
            self.cursor = self.conn.cursor()
            print("登录数据库")
        except Exception as e:
            raise Exception(f"数据库连接失败！！！\n请检查表名、配置参数是否正确或检查本地数据库是否已启动！\n{e}")

    def close(self):
        """
        关闭数据库连接
        :return:
        """
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        print("关闭数据库")

        # 删除表

    def drop_table(self, tableName):
        """
        删除表
        :param tableName:
        :return:
        """
        sql = f'drop table if exists {tableName} '
        print(sql)
        self.cursor.execute(sql)
        self.conn.commit()
        print(f'表-{tableName}-删除成功！')

        # 查询表数据

    def select(self, tableName: str, where=None):
        """
        where 查询表数据
        :param tableName:
        :param fields:
        :return:
        """
        sql = f'select * from {tableName} '
        if where:
            sql += f'where {where} '
        # print(sql)
        self.cursor.execute(sql)
        print(f'查询表-{tableName}-结果：\n', self.cursor.fetchall())

        # #自定义查询
        # def query(self, sql: str):
        #     """
        #     自定义查询
        #     :param sql:
        #     :return:
        #     """
        #     self.cursor.execute(sql)
        #     print(f'查询结果：', self.cursor.fetchall())

        # update

    def update(self, tableName: str, data: dict, where=None):
        """
        更新数据
        :param tableName:
        :param data:
        :param where:
        :return:
        """
        keys = ', '.join(data.keys())
        values = ', '.join(['%s'] * len(data))
        sql = f'update {tableName} set {keys} = {values}'
        if where:
            sql += f' where {where}'
        self.cursor.execute(sql, tuple(data.values()))
        self.conn.commit()
        print(f'表-{tableName}-更新数据成功！')

        # 联合查询

    def join(self, tableName: str, joinTableName: str, on=None):
        """
        联合查询
        :param tableName:
        :param joinTableName:
        :param on:
        :return:
        """
        sql = f'select * from {tableName} '
        if joinTableName:
            sql += f'join {joinTableName} on {on} '
        print(sql)
        self.cursor.execute(sql)
        print(f'表-{tableName}-联合查询结果：', self.cursor.fetchall())

        # 插入数据

    def AddColumn(self, tableName: str, columnName: str, columnType: str):

        sql = f'ALTER TABLE `{tableName}` add {columnName} {columnType};'

        self.cursor.execute(sql)

    def SubractColumns(self, tableName: str, columnName: str):
        sql = f'alter table `{tableName}` drop {columnName}'
        self.cursor.execute(sql)

    def insert(self, tableName: str, data: dict):
        """
        插入数据
        :param tableName:
        :param data:
        :return:
        """
        data = pd.DataFrame(data)
        sql = f"insert into `{tableName}`(id,name,age) values (%s,%s,%s)"
        for r in range(0, len(data)):
            id = data.iloc[r, 0]
            name = data.iloc[r, 1]
            age = data.iloc[r, 2]
            values = (id, name, age)
            self.cursor.execute(sql, values)

        self.conn.commit()
        print(f'表-{tableName}-插入数据成功！')

    def deduplicate1(self,tablename):
        """
        :param tableName:表名  根据时间戳删除重复数据
        :return:
        """
        sql = f"delete from `{tablename}` where id not in (select ID FROM (SELECT min(id) AS ID FROM `{tablename}` GROUP BY code,stime )t);"
        self.cursor.execute(sql)

        sql = f'alter table `{tablename}` drop id;'
        self.cursor.execute(sql)
        sql = f'alter table `{tablename}` add id int not null primary key auto_increment first;'
        self.cursor.execute(sql)

        self.conn.commit()
    def insert2(self, tableName: str, data: dict, database, fields):
        """
        :param tableName:表名
        :param data:插入数据
        :param database:数据库名
        :param AddColumns:增加列
        :param SubractColumns:删除列
        :return:
        """
        sql = f"select COLUMN_NAME from information_schema.COLUMNS where table_name = '{tableName}' and table_schema = '{database}'"
        self.cursor.execute(sql)
        columnslist = [i[0] for i in self.cursor]
        fieldslist = [i for i in fields.keys()]

        for i in fields.keys():
            if i not in columnslist:
                self.AddColumn(tableName, i, fields[i])
        for i in columnslist:
            if i not in fields.keys():
                self.SubractColumns(tableName, i)

        self.conn.commit()

        self.engine = create_engine(f"mysql+pymysql://{self.user}:{self.password}@{self.host}/{database}")

        data = pd.DataFrame(data)
        data.to_sql(tableName, self.engine, if_exists='append', index=False)
        self.conn.commit()
        print(f'表-{tableName}-插入数据成功！')

        # 创建一个表

    def UseDatabaseOrCreaTetableAndUpdata(self, databasename, tablename, data, fields):
        """
        :param databasename:数据库名
        :param tablename:表名
        :param data:插入数据
        :param fields 初始化表名
        :param AddColumns:增加列
        :param SubractColumns:删除列
        :return:
        """

        sql = f'create database if not exists {databasename}'
        print(sql)
        self.cursor.execute(sql)
        sql = f'use {databasename}'
        self.cursor.execute(sql)

        sql = f'create table if not exists `{tablename}` ( '

        for name, type1 in fields.items():
            sql += '%s %s,' % (name, type1)
        sql = sql.rstrip(',') + ') character set utf8;'
        print(sql)
        self.cursor.execute(sql)
        self.conn.commit()

        # self.insert(tablename,data)

        self.insert2(tablename, data, databasename, fields)

        self.deduplicate1(tablename)

    def get_price(self, codelist: list, fields: str, start_date, end_date):
        """
        :param codelist
        :param fields:
        :param start_date
        :param end_date
        :return:
        """
        database = '基本数据_日线'
        df = {}
        engine = create_engine(f"mysql+pymysql://{self.user}:{self.password}@{self.host}/{database}")
        fields_ = fields.strip('"')

        # todo 判断该股票在不在数据库 如果不在返回股票代码
        sql = f"show tables from {database};"
        print(sql)
        self.cursor.execute(sql)
        tablename_ = []
        for table in self.cursor.fetchall():
            tablename_.append(str(table[0])[:6])

        for i in codelist:
            if str(i)[:6] in tablename_:
                query = f"SELECT {fields_} FROM `{i}` where stime >=  '{start_date}' and  stime <=  '{end_date}' "
                data = pd.read_sql(query, engine)
                df[f'{i}'] = data
        return df


    def get_foctor(self,codelist, tablename: str,start_date, end_date,  fields: str):
        """
        :param tablename: 有表名查询数据库foctor
        :param fields:  查询表字段 如果为空 查询全部
        :param codelist: 有列表查询数据库foctor1
        :param tablename 与 codelist 不同时存在

        """
        # todo 获取多个因子 pd.merge
        codelist_ = str(codelist).strip("[").strip("]")

        database = '因子数据库'
        engine = create_engine(f"mysql+pymysql://{self.user}:{self.password}@{self.host}/{database}")


        fields_ = fields.strip('"')

        query = f"select {fields_} from {tablename}  where stime >=  '{start_date}' and  stime <=  '{end_date}' and `code` IN ({codelist_}); "
        data = pd.read_sql(query, engine)
        return data

    def get_price_factor_alphalens(self, codelist,fields,fields2,tablename,start_date, end_date):
        """
        :param codelist
        :param start_date
        :param end_date
        :return:
        """
        fields_ = fields.strip('"')
        data = self.get_price(codelist,fields_, start_date, end_date)
        data2 = self.get_foctor(codelist, tablename,start_date, end_date, fields2)
        prices_factor = pd.DataFrame()
        for i, y in data.items():
            factor = data2[data2['code']==i]
            p_f = pd.merge(y,factor,how='left',on=['stime','code'])
            prices_factor = pd.concat([prices_factor, p_f], axis=0,ignore_index=True)

        prices_factor.index = pd.to_datetime(prices_factor['stime'], utc=True)
        price_result =prices_factor.pivot(index='stime', columns='code', values='va')
        price_result.columns.name = ''
        price_result.index = pd.to_datetime(price_result.index,utc=True)
        prices_factor.set_index([prices_factor.index, 'code'], inplace=True,drop=True)
        prices_factor.dropna(inplace=True)
        factor_result = prices_factor['va']
        return price_result,factor_result



    def get_finance(self, database,tablename,codelist,start_date, end_date,fields):
        """
        :param database: 数据库    '基本数据_日线'
        :param tablename: 表明
        :param codelist
        :param fields: 字段名
        :param start_date
        :param end_date
        :return:
        """
        df = {}
        engine = create_engine(f"mysql+pymysql://{self.user}:{self.password}@{self.host}/{database}")
        fields_ = fields.strip('"')
        codelist_ = str(codelist).strip("[").strip("]")
        # todo 判断该股票在不在数据库 如果不在返回股票代码

        query = f"SELECT {fields_} FROM `{tablename}` where `stime` >=  '{start_date}' and  `stime` <=  '{end_date}'  and `code` IN ({codelist_});"
        # print(query)
        data = pd.read_sql(query, engine)

        return data



if __name__ == '__main__':

    db = W_RBase()
    # print(mysql_config['mysql_qmtdata_d_add_columns'])


    with db:
        print("as")
    #     db.get_foctor('2023-11-17','2024-01-19',tablename='沪股通资金数据',fields="日期,当日成交净买额,买入成交额")

# db.get_foctor('2023-11-17','2024-01-19',tablename='沪股通资金数据',fields="日期,当日成交净买额,买入成交额")


# df = db.get_price(["000001","000002"],"code,close,stime",'2023-03-04','2023-03-10')

# prices = db.get_price_alphalens(["000001","000002"],'2023-03-04','2023-03-10')
# p = prices
# p1 = prices
#
# p2 = pd.concat([p1,p],axis=0)
# print(p2)

# df = db.


# data =  {'id': [1,2,3], 'name': ["nihao","nihao","nihao"], 'age': [5,6,7],'sd4':[4,5,8]}
#
# db.UseDatabaseOrCreaTetableAndUpdata('qmtdata','600887.sh',data,mysql_config['mysql_qmtdata_d'])

# db.get_coon
# db.usedatabase('world')
# db.show_tables_name()
# db.show_field_name('student')

# df = {'id':'int AUTO_INCREMENT PRIMARY KEY', 'name':'text(20)', 'age':'int'}
# db.create_table('df',df)

# db.show_field_name('df')

# db.drop_table('df')

# Tom = 'Lily'
# db.select(tableName='student', where=f'name="{Tom}" and id=1')

# db.select('df')

# data = {'id':1,'name':'ghgh',"age":15}
# db.insert("df", data)

# sql = 'select  id,name from student'
# db.query(sql)

# db.join('student', 'df',on='student.id = df.id')