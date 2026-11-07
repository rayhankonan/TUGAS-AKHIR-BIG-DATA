"""Behavioral checks: malformed input cannot silently enter the warehouse."""
from pathlib import Path
import sys
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from sales_project import *
import pandas as pd

def main():
    base = pd.read_csv(SOURCE).iloc[:1].copy()
    rows=[]
    for i in range(1,10):
        row=base.iloc[0].copy()
        row['transaction_id']=i
        rows.append(row)
    fixture=pd.DataFrame(rows).reset_index(drop=True)
    fixture.loc[1,'transaction_date']='invalid-date'
    fixture.loc[2,'branch']='   '
    fixture.loc[3,'quantity']=-1
    fixture.loc[4,'discount_rate']=1.5
    fixture.loc[5,'net_sales']+=1
    fixture.loc[6,'unit_price']=None
    fixture.loc[8,'transaction_id']=8  # both rows with ID 8 must be rejected
    fixture.loc[0,'branch']='  Denpasar   '
    path=ROOT/'work/quality_fixture.csv'
    path.parent.mkdir(exist_ok=True)
    fixture.to_csv(path,index=False)
    spark=spark_session()
    try:
        raw,clean,invalid,duplicate=spark_transform(spark,path)
        result=dict(input_rows=raw.count(), accepted_rows=clean.count(),invalid_rows=invalid.count(),duplicate_rows=duplicate.count())
        assert result==dict(input_rows=9,accepted_rows=1,invalid_rows=6,duplicate_rows=2),result
        kept=clean.collect()[0]
        assert kept.transaction_id==1 and kept.branch=='Denpasar'
        absolute=base.copy()
        absolute.loc[0,'net_sales']+=1
        assert not pandas_checks(absolute)['net_formula'], 'Rp1 discrepancy must not pass'
        result['whitespace_normalization']=True
        result['one_rupiah_error_rejected']=True
        save_json(OUT/'validation/quality_policy_test.json',result)
        print(result)
    finally:
        spark.stop()

if __name__=='__main__': main()
