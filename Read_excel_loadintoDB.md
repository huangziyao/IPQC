```python
import os
import time
import pandas as pd
from datetime import datetime
import psycopg2
from psycopg2 import sql
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine
```


```python
def sort_dataframe_by_date(df, date_column, ascending=False):
    """
    按指定的日期列对 DataFrame 进行排序

    参数:
        df (pd.DataFrame): 输入的 DataFrame
        date_column (str): 日期列的列名
        ascending (bool): 是否按升序排序，默认为 False（降序）

    返回:
        pd.DataFrame: 排序后的 DataFrame
    """
    # 检查日期列是否存在
    if date_column not in df.columns:
        raise ValueError(f"列 '{date_column}' 不存在于 DataFrame 中")

    # 将日期列转换为 datetime 类型，无法转换的值设置为 NaT
    df[date_column] = pd.to_datetime(df[date_column], errors='coerce')

    # 按指定列进行排序
    df_sorted = df.sort_values(by=date_column, ascending=ascending)

    return df_sorted

def filter_today_data(df, date_column):
    """
    筛选出 DataFrame 中日期等于今天的行

    参数:
        df (pd.DataFrame): 输入的 DataFrame
        date_column (str): 日期列的列名，该列应为 datetime 类型

    返回:
        pd.DataFrame: 仅包含今天日期的 DataFrame
    """
    # 检查日期列是否存在
    if date_column not in df.columns:
        raise ValueError(f"列 '{date_column}' 不存在于 DataFrame 中")

    # 检查日期列是否为 datetime 类型
    if not pd.api.types.is_datetime64_any_dtype(df[date_column]):
        raise ValueError(f"列 '{date_column}' 不是 datetime 类型")

    # 获取今天的日期
    today = datetime.today().date()

    # 筛选出日期等于今天的行
    today_data = df[df[date_column].dt.date == today]

    return today_data

# 定义写入 PostgreSQL 的函数
def write_to_postgresql(dbname, user, password, host, port, table_name, data):
    """
    将数据写入 PostgreSQL 数据库。
    
    参数:
        dbname (str): 数据库名称。
        user (str): 数据库用户名。
        password (str): 数据库密码。
        host (str): 数据库主机地址。
        port (str): 数据库端口号。
        table_name (str): 要写入的表名。
        data (pd.DataFrame): 要写入的数据。
    """
    # 构建数据库连接字符串
    connection_string = f"postgresql://{user}:{password}@{host}:{port}/{dbname}"
    
    # 创建数据库引擎
    engine = create_engine(connection_string)
    
    # 将数据写入数据库
    data.to_sql(table_name, engine, if_exists='append', index=False)

def add_create_time_column(df):
    """
    在 DataFrame 中添加一列 create_time，其值为当前时间。
    
    参数:
        df (pd.DataFrame): 输入的 DataFrame。
    
    返回:
        pd.DataFrame: 添加了 create_time 列的 DataFrame。
    """
    # 获取当前时间
    current_time = datetime.now()
    
    # 添加 create_time 列
    df['create_time'] = current_time
    
    return df
```


```python
## Be careful the parameter sheet_name
EXCEL_FILE_PATH = r"S:\Santo\QA\4_Reports\3_QC\IPQC Dashboard Raw data_(勿删）\Key Machine Data.xlsx"  # 替换为你的 Excel 文件路径
df = pd.read_excel(EXCEL_FILE_PATH,sheet_name='Raw_data')
```

    C:\Users\Jim.huang\AppData\Local\anaconda_new\Lib\site-packages\openpyxl\worksheet\_read_only.py:81: UserWarning: Conditional Formatting extension is not supported and will be removed
      for idx, row in parser.parse():
    C:\Users\Jim.huang\AppData\Local\anaconda_new\Lib\site-packages\openpyxl\worksheet\_read_only.py:81: UserWarning: Data Validation extension is not supported and will be removed
      for idx, row in parser.parse():
    


```python
def generate_composite_key(df):
    """
    为给定的 DataFrame 生成复合主键，并重命名为 'cd_jim_test'。
    
    参数:
        df (pd.DataFrame): 输入的 DataFrame。
    
    返回:
        pd.DataFrame: 添加了复合主键列的 DataFrame。
    """
    # 获取当前日期
    current_date = datetime.now().date()

    # 计算当天的更新次数
    # 假设每半小时更新一次，一天有48次更新
    update_count = int((datetime.now() - datetime.combine(current_date, datetime.min.time())) / timedelta(minutes=30) + 1)

    # 生成复合主键
    df['row_number'] = df.index + 1  # 行号从1开始
    df['update_count'] = update_count  # 更新次数
    df['composite_key'] = df['row_number'] * 100 + df['update_count']
    # 删除不必要的列
    df.drop(columns=['row_number', 'update_count'], inplace=True)

    # 重命名 composite_key 列为 cd_jim_test
    df.rename(columns={'composite_key': 'cd_jim_test'}, inplace=True) 

    return df

generate_composite_key(df)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Machine_no</th>
      <th>PN</th>
      <th>Process_desc</th>
      <th>UPH</th>
      <th>Date</th>
      <th>Shift</th>
      <th>Plan_qty</th>
      <th>Actual_qty</th>
      <th>Plan_note</th>
      <th>Actual_note</th>
      <th>Machine_type</th>
      <th>Machine_group</th>
      <th>Workshop</th>
      <th>Machie_Tier</th>
      <th>Detail_machine_type</th>
      <th>Mac_PC_Name</th>
      <th>IPQC</th>
      <th>status</th>
      <th>备注</th>
      <th>cd_jim_test</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>CNM-008</td>
      <td>698308002VM2</td>
      <td>加工中心二次</td>
      <td>1.8</td>
      <td>2024-06-14</td>
      <td>day</td>
      <td>19.8</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>VMC</td>
      <td>VMC</td>
      <td>TPP1</td>
      <td>T1-1 Capable Machine</td>
      <td>加工中心</td>
      <td>占红莲</td>
      <td>石兰平</td>
      <td>OK</td>
      <td>NaN</td>
      <td>127</td>
    </tr>
    <tr>
      <th>1</th>
      <td>CNM-008</td>
      <td>698308002VM2</td>
      <td>加工中心二次</td>
      <td>1.8</td>
      <td>2024-06-14</td>
      <td>night</td>
      <td>19.8</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>VMC</td>
      <td>VMC</td>
      <td>TPP1</td>
      <td>T1-1 Capable Machine</td>
      <td>加工中心</td>
      <td>占红莲</td>
      <td>李水英</td>
      <td>OK</td>
      <td>NaN</td>
      <td>227</td>
    </tr>
    <tr>
      <th>2</th>
      <td>CNM-009</td>
      <td>J696520002</td>
      <td>加工中心一次</td>
      <td>17.9</td>
      <td>2024-06-14</td>
      <td>day</td>
      <td>196.9</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>VMC</td>
      <td>VMC</td>
      <td>TPP1</td>
      <td>T1-1 Capable Machine</td>
      <td>加工中心</td>
      <td>占红莲</td>
      <td>石兰平</td>
      <td>OK</td>
      <td>NaN</td>
      <td>327</td>
    </tr>
    <tr>
      <th>3</th>
      <td>CNM-009</td>
      <td>J696520002</td>
      <td>加工中心一次</td>
      <td>17.9</td>
      <td>2024-06-14</td>
      <td>night</td>
      <td>196.9</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>VMC</td>
      <td>VMC</td>
      <td>TPP1</td>
      <td>T1-1 Capable Machine</td>
      <td>加工中心</td>
      <td>占红莲</td>
      <td>李水英</td>
      <td>OK</td>
      <td>NaN</td>
      <td>427</td>
    </tr>
    <tr>
      <th>4</th>
      <td>CNM-010</td>
      <td>641872001</td>
      <td>加工中心三次</td>
      <td>1.7</td>
      <td>2024-06-14</td>
      <td>night</td>
      <td>18.7</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>VMC</td>
      <td>VMC</td>
      <td>TPP1</td>
      <td>Non-T1 Machine</td>
      <td>加工中心</td>
      <td>占红莲</td>
      <td>黄院梅</td>
      <td>OK</td>
      <td>NaN</td>
      <td>527</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>136244</th>
      <td>HO-012</td>
      <td>622069001HN1</td>
      <td>珩磨一次</td>
      <td>45</td>
      <td>2025-02-06</td>
      <td>night</td>
      <td>495</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>HO</td>
      <td>Honing</td>
      <td>TPP1 2F</td>
      <td>T1-1 Capable Machine</td>
      <td>绗磨</td>
      <td>袁小芳</td>
      <td>许明华</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>13624527</td>
    </tr>
    <tr>
      <th>136245</th>
      <td>ECM-001</td>
      <td>622069001EC1</td>
      <td>ECM</td>
      <td>45</td>
      <td>2025-02-06</td>
      <td>day</td>
      <td>495</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>ECM</td>
      <td>ECM</td>
      <td>TPP1 2F</td>
      <td>T1-1 Capable Machine</td>
      <td>ECM-001</td>
      <td>袁小芳</td>
      <td>刘利芳</td>
      <td>OK</td>
      <td>NaN</td>
      <td>13624627</td>
    </tr>
    <tr>
      <th>136246</th>
      <td>ECM-001</td>
      <td>622069001EC1</td>
      <td>ECM</td>
      <td>45</td>
      <td>2025-02-06</td>
      <td>night</td>
      <td>495</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>ECM</td>
      <td>ECM</td>
      <td>TPP1 2F</td>
      <td>T1-1 Capable Machine</td>
      <td>ECM-001</td>
      <td>袁小芳</td>
      <td>许明华</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>13624727</td>
    </tr>
    <tr>
      <th>136247</th>
      <td>ECM-002</td>
      <td>622621001EC1</td>
      <td>ECM(C3213)</td>
      <td>40</td>
      <td>2025-02-06</td>
      <td>day</td>
      <td>220</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>ECM</td>
      <td>ECM</td>
      <td>TPP1 2F</td>
      <td>T1-1 Capable Machine</td>
      <td>ECM-002</td>
      <td>袁小芳</td>
      <td>刘利芳</td>
      <td>OK</td>
      <td>NaN</td>
      <td>13624827</td>
    </tr>
    <tr>
      <th>136248</th>
      <td>ECM-002</td>
      <td>622621001EC1</td>
      <td>ECM(C3213)</td>
      <td>40</td>
      <td>2025-02-06</td>
      <td>night</td>
      <td>220</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>ECM</td>
      <td>ECM</td>
      <td>TPP1 2F</td>
      <td>T1-1 Capable Machine</td>
      <td>ECM-002</td>
      <td>袁小芳</td>
      <td>许明华</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>13624927</td>
    </tr>
  </tbody>
</table>
<p>136249 rows × 20 columns</p>
</div>




```python
df=filter_today_data(df,'Date')
df
df=add_create_time_column(df)
df
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Machine_no</th>
      <th>PN</th>
      <th>Process_desc</th>
      <th>UPH</th>
      <th>Date</th>
      <th>Shift</th>
      <th>Plan_qty</th>
      <th>Actual_qty</th>
      <th>Plan_note</th>
      <th>Actual_note</th>
      <th>...</th>
      <th>Machine_group</th>
      <th>Workshop</th>
      <th>Machie_Tier</th>
      <th>Detail_machine_type</th>
      <th>Mac_PC_Name</th>
      <th>IPQC</th>
      <th>status</th>
      <th>备注</th>
      <th>cd_jim_test</th>
      <th>create_time</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>135727</th>
      <td>GRC-012</td>
      <td>622577001GC1</td>
      <td>无心磨</td>
      <td>40</td>
      <td>2025-02-06</td>
      <td>day</td>
      <td>440</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>...</td>
      <td>Grinding</td>
      <td>1BC</td>
      <td>Non-T1 Machine</td>
      <td>无蕊磨 (光阳)</td>
      <td>袁小芳</td>
      <td>谭光美</td>
      <td>OK</td>
      <td>NaN</td>
      <td>13572827</td>
      <td>2025-02-06 13:20:58.491991</td>
    </tr>
    <tr>
      <th>135728</th>
      <td>GRC-012</td>
      <td>622577001GC1</td>
      <td>无心磨</td>
      <td>40</td>
      <td>2025-02-06</td>
      <td>night</td>
      <td>440</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>...</td>
      <td>Grinding</td>
      <td>1BC</td>
      <td>Non-T1 Machine</td>
      <td>无蕊磨 (光阳)</td>
      <td>袁小芳</td>
      <td>李水英</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>13572927</td>
      <td>2025-02-06 13:20:58.491991</td>
    </tr>
    <tr>
      <th>135729</th>
      <td>GRC-019</td>
      <td>622592001GC1</td>
      <td>无心磨</td>
      <td>42</td>
      <td>2025-02-06</td>
      <td>day</td>
      <td>231</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>...</td>
      <td>Grinding</td>
      <td>1BC</td>
      <td>T1-1 Capable Machine</td>
      <td>无蕊磨 (光阳)</td>
      <td>袁小芳</td>
      <td>谭光美</td>
      <td>OK</td>
      <td>NaN</td>
      <td>13573027</td>
      <td>2025-02-06 13:20:58.491991</td>
    </tr>
    <tr>
      <th>135730</th>
      <td>GRC-019</td>
      <td>622592001GC1</td>
      <td>无心磨</td>
      <td>42</td>
      <td>2025-02-06</td>
      <td>night</td>
      <td>231</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>...</td>
      <td>Grinding</td>
      <td>1BC</td>
      <td>T1-1 Capable Machine</td>
      <td>无蕊磨 (光阳)</td>
      <td>袁小芳</td>
      <td>李水英</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>13573127</td>
      <td>2025-02-06 13:20:58.491991</td>
    </tr>
    <tr>
      <th>135731</th>
      <td>GRC-020</td>
      <td>623045001GC1</td>
      <td>无心磨</td>
      <td>38</td>
      <td>2025-02-06</td>
      <td>day</td>
      <td>418</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>...</td>
      <td>Grinding</td>
      <td>1BC</td>
      <td>T1-1 Capable Machine</td>
      <td>无蕊磨 (光阳)</td>
      <td>袁小芳</td>
      <td>谭光美</td>
      <td>OK</td>
      <td>NaN</td>
      <td>13573227</td>
      <td>2025-02-06 13:20:58.491991</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>136244</th>
      <td>HO-012</td>
      <td>622069001HN1</td>
      <td>珩磨一次</td>
      <td>45</td>
      <td>2025-02-06</td>
      <td>night</td>
      <td>495</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>...</td>
      <td>Honing</td>
      <td>TPP1 2F</td>
      <td>T1-1 Capable Machine</td>
      <td>绗磨</td>
      <td>袁小芳</td>
      <td>许明华</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>13624527</td>
      <td>2025-02-06 13:20:58.491991</td>
    </tr>
    <tr>
      <th>136245</th>
      <td>ECM-001</td>
      <td>622069001EC1</td>
      <td>ECM</td>
      <td>45</td>
      <td>2025-02-06</td>
      <td>day</td>
      <td>495</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>...</td>
      <td>ECM</td>
      <td>TPP1 2F</td>
      <td>T1-1 Capable Machine</td>
      <td>ECM-001</td>
      <td>袁小芳</td>
      <td>刘利芳</td>
      <td>OK</td>
      <td>NaN</td>
      <td>13624627</td>
      <td>2025-02-06 13:20:58.491991</td>
    </tr>
    <tr>
      <th>136246</th>
      <td>ECM-001</td>
      <td>622069001EC1</td>
      <td>ECM</td>
      <td>45</td>
      <td>2025-02-06</td>
      <td>night</td>
      <td>495</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>...</td>
      <td>ECM</td>
      <td>TPP1 2F</td>
      <td>T1-1 Capable Machine</td>
      <td>ECM-001</td>
      <td>袁小芳</td>
      <td>许明华</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>13624727</td>
      <td>2025-02-06 13:20:58.491991</td>
    </tr>
    <tr>
      <th>136247</th>
      <td>ECM-002</td>
      <td>622621001EC1</td>
      <td>ECM(C3213)</td>
      <td>40</td>
      <td>2025-02-06</td>
      <td>day</td>
      <td>220</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>...</td>
      <td>ECM</td>
      <td>TPP1 2F</td>
      <td>T1-1 Capable Machine</td>
      <td>ECM-002</td>
      <td>袁小芳</td>
      <td>刘利芳</td>
      <td>OK</td>
      <td>NaN</td>
      <td>13624827</td>
      <td>2025-02-06 13:20:58.491991</td>
    </tr>
    <tr>
      <th>136248</th>
      <td>ECM-002</td>
      <td>622621001EC1</td>
      <td>ECM(C3213)</td>
      <td>40</td>
      <td>2025-02-06</td>
      <td>night</td>
      <td>220</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>...</td>
      <td>ECM</td>
      <td>TPP1 2F</td>
      <td>T1-1 Capable Machine</td>
      <td>ECM-002</td>
      <td>袁小芳</td>
      <td>许明华</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>13624927</td>
      <td>2025-02-06 13:20:58.491991</td>
    </tr>
  </tbody>
</table>
<p>522 rows × 21 columns</p>
</div>




```python
# 原始列名
original_columns = [
    'Machine_no', 'PN', 'Process_desc', 'UPH', 'Date', 'Shift', 'Plan_qty', 
    'Machine_type', 'Machine_group', 
    'Workshop', 'Machie_Tier', 'Detail_machine_type', 'Mac_PC_Name', 'IPQC', 
    'status', '备注','cd_jim_test','create_time'
]

# 新的列名
new_columns = [
    'Machine_no', 'PN', 'Process_desc', 'UPH', 'Date', 'Shift', 'Plan_qty', 
    'Machine_type', 'Machine_group', 'Workshop', 'Machie_Tier', 'Detail_machine_type', 
    'Mac_PC_Name', 'IPQC', 'status', 'remark','cd_jim_test','create_time'
]

# 创建一个映射字典
column_mapping = dict(zip(original_columns, new_columns))

column_list = df.columns.tolist()
print("列名列表:", column_list)

df.rename(columns={'IPQC ': 'IPQC'}, inplace=True)
df.rename(columns={'备注': 'remark'}, inplace=True)

print("\n修改后的 DataFrame:")
print(df)

column_list = df.columns.tolist()
print("列名列表:", column_list)
```

    列名列表: ['Machine_no', 'PN', 'Process_desc', 'UPH', 'Date', 'Shift', 'Plan_qty', 'Actual_qty', 'Plan_note', 'Actual_note', 'Machine_type', 'Machine_group', 'Workshop', 'Machie_Tier', 'Detail_machine_type', 'Mac_PC_Name', 'IPQC', 'status', '备注', 'cd_jim_test', 'create_time']
    
    修改后的 DataFrame:
           Machine_no            PN Process_desc UPH       Date  Shift Plan_qty  \
    135727    GRC-012  622577001GC1          无心磨  40 2025-02-06    day      440   
    135728    GRC-012  622577001GC1          无心磨  40 2025-02-06  night      440   
    135729    GRC-019  622592001GC1          无心磨  42 2025-02-06    day      231   
    135730    GRC-019  622592001GC1          无心磨  42 2025-02-06  night      231   
    135731    GRC-020  623045001GC1          无心磨  38 2025-02-06    day      418   
    ...           ...           ...          ...  ..        ...    ...      ...   
    136244     HO-012  622069001HN1         珩磨一次  45 2025-02-06  night      495   
    136245    ECM-001  622069001EC1          ECM  45 2025-02-06    day      495   
    136246    ECM-001  622069001EC1          ECM  45 2025-02-06  night      495   
    136247    ECM-002  622621001EC1   ECM(C3213)  40 2025-02-06    day      220   
    136248    ECM-002  622621001EC1   ECM(C3213)  40 2025-02-06  night      220   
    
           Actual_qty Plan_note Actual_note  ... Machine_group Workshop  \
    135727        NaN       NaN         NaN  ...      Grinding      1BC   
    135728        NaN       NaN         NaN  ...      Grinding      1BC   
    135729        NaN       NaN         NaN  ...      Grinding      1BC   
    135730        NaN       NaN         NaN  ...      Grinding      1BC   
    135731        NaN       NaN         NaN  ...      Grinding      1BC   
    ...           ...       ...         ...  ...           ...      ...   
    136244        NaN       NaN         NaN  ...        Honing  TPP1 2F   
    136245        NaN       NaN         NaN  ...           ECM  TPP1 2F   
    136246        NaN       NaN         NaN  ...           ECM  TPP1 2F   
    136247        NaN       NaN         NaN  ...           ECM  TPP1 2F   
    136248        NaN       NaN         NaN  ...           ECM  TPP1 2F   
    
                     Machie_Tier Detail_machine_type Mac_PC_Name IPQC status  \
    135727        Non-T1 Machine            无蕊磨 (光阳)         袁小芳  谭光美     OK   
    135728        Non-T1 Machine            无蕊磨 (光阳)         袁小芳  李水英    NaN   
    135729  T1-1 Capable Machine            无蕊磨 (光阳)         袁小芳  谭光美     OK   
    135730  T1-1 Capable Machine            无蕊磨 (光阳)         袁小芳  李水英    NaN   
    135731  T1-1 Capable Machine            无蕊磨 (光阳)         袁小芳  谭光美     OK   
    ...                      ...                 ...         ...  ...    ...   
    136244  T1-1 Capable Machine                  绗磨         袁小芳  许明华    NaN   
    136245  T1-1 Capable Machine             ECM-001         袁小芳  刘利芳     OK   
    136246  T1-1 Capable Machine             ECM-001         袁小芳  许明华    NaN   
    136247  T1-1 Capable Machine             ECM-002         袁小芳  刘利芳     OK   
    136248  T1-1 Capable Machine             ECM-002         袁小芳  许明华    NaN   
    
           remark cd_jim_test                create_time  
    135727    NaN    13572827 2025-02-06 13:20:58.491991  
    135728    NaN    13572927 2025-02-06 13:20:58.491991  
    135729    NaN    13573027 2025-02-06 13:20:58.491991  
    135730    NaN    13573127 2025-02-06 13:20:58.491991  
    135731    NaN    13573227 2025-02-06 13:20:58.491991  
    ...       ...         ...                        ...  
    136244    NaN    13624527 2025-02-06 13:20:58.491991  
    136245    NaN    13624627 2025-02-06 13:20:58.491991  
    136246    NaN    13624727 2025-02-06 13:20:58.491991  
    136247    NaN    13624827 2025-02-06 13:20:58.491991  
    136248    NaN    13624927 2025-02-06 13:20:58.491991  
    
    [522 rows x 21 columns]
    列名列表: ['Machine_no', 'PN', 'Process_desc', 'UPH', 'Date', 'Shift', 'Plan_qty', 'Actual_qty', 'Plan_note', 'Actual_note', 'Machine_type', 'Machine_group', 'Workshop', 'Machie_Tier', 'Detail_machine_type', 'Mac_PC_Name', 'IPQC', 'status', 'remark', 'cd_jim_test', 'create_time']
    


```python
import pandas as pd
from sqlalchemy import create_engine

# 数据库连接信息
database = ''
user = ''
password = ''
host = ''
port = ''

# 创建 SQLAlchemy 引擎
engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')

# 将 DataFrame 插入到 PostgreSQL 数据库
try:
    df.to_sql('jim_test', engine, schema='datateam', if_exists='append', index=False)
    print("数据插入成功！")
except Exception as e:
    print(f"发生错误：{e}")
```

    数据插入成功！
    
