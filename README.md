# nem-writer

[![PyPI version](https://badge.fury.io/py/nemwriter.svg)](https://badge.fury.io/py/nemwriter) [![Build Status](https://travis-ci.org/aguinane/nem-writer.svg?branch=master)](https://travis-ci.org/aguinane/nem-writer) [![Coverage Status](https://coveralls.io/repos/github/aguinane/nem-writer/badge.svg?branch=master)](https://coveralls.io/github/aguinane/nem-writer?branch=master)

Write meter readings to AEMO NEM12 (interval metering data) and NEM13 (accumulated metering data) data files

## Accumulated Data (NEM13)

```python
from datetime import datetime
from nemwriter import NEM13

m = NEM13(to_participant='123')
ch = m.add_reading(nmi='123',
                    nmi_configuration='E1B1B2',
                    register_id='1',
                    nmi_suffix='E1',
                    previous_read=412,
                    previous_read_date=datetime(2017,1,1),
                    previous_quality_method='A',
                    current_read=512,
                    current_read_date=datetime(2017,2,1),
                    current_quality_method='A',
                    quantity=100,
                    uom='kWh'
                    )
output = m.output_csv(file_path='output.csv')
```

Will output:
```
100,NEM13,201701010101,,123
250,123,E1B1B2,1,E1,,,E,412,201701010000,A,,,512,201702010000,A,,,100,kWh,,,
900
```

## Interval Data (NEM12)

```python
from datetime import datetime
from nemwriter import NEM12

m = NEM12(to_participant='123')
readings = [
    # read end, read value, quality method, event code, event desc
    [datetime(2004, 4, 18, 0, 30), 10.1, 'A', 79, 'Power Outage Alarm'],
    [datetime(2004, 4, 18, 1, 0), 11.2, 'A'],
    [datetime(2004, 4, 18, 1, 30), 12.3, 'A'],
    [datetime(2004, 4, 18, 2, 0), 13.4, 'A'],
]

ch = m.add_readings(nmi='123',
                    nmi_configuration='E1B1B2',
                    nmi_suffix='E1', uom='kWh',
                    readings=readings)
output = m.output_csv(file_path='output.csv')
```

Will output:
```
100,NEM12,201701010101,,123
200,123,E1B1B2,,E1,,,kWh,30,
300,20040418,10.1,11.2,12.3,13.4,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,V,,,,
400,1,1,A,79,Power Outage Alarm
400,2,48,A,,
900

```

Alternatively, save as a compressed csv in a zip file.
```python
output = m.output_zip(file_path='output.zip')
```

### From Pandas DataFrame

If you create a pandas DataFrame, for example:

```python
num_intervals = 288
index = [datetime(2004, 4, 1) + timedelta(minutes=5*x) for x in range(1,num_intervals+1)]
e1 = [randrange(1,10) for x in range(1,num_intervals+1)]
e2 = [randrange(1,5) for x in range(1,num_intervals+1)]
s1 = pd.Series(data=e1, index=index, name="E1")
s2 = pd.Series(data=e2, index=index, name="E2")
df=pd.concat([s1,s2],axis=1)
print(df)
```

```
                     E1  E2
2004-04-01 00:05:00   2   3
2004-04-01 00:10:00   8   3
2004-04-01 00:15:00   7   2
2004-04-01 00:20:00   4   3
2004-04-01 00:25:00   3   4
...                  ..  ..
2004-04-01 23:40:00   9   2
2004-04-01 23:45:00   1   1
2004-04-01 23:50:00   6   2
2004-04-01 23:55:00   7   1
2004-04-01 00:00:00   4   2
```

You can easily output the dataframe to a NEM12 file:
```python
m = NEM12(to_participant='123')
m.add_dataframe(nmi='123', interval=5, df=df, uoms={'E1': 'kWh', 'E2': 'kWh'})
output = m.output_csv(file_path='output.csv')
```

If your DataFrame has a `Quality` and `EventDesc` column, they will also be handled appropriately.
