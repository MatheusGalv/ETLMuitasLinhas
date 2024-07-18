import duckdb 
import time

def creat_duckdb():
    result = duckdb.sql("""
    SELECT
        Station,
            min(temperatura) as temperatura_minima,
            avg(temperatura) as temperatura_media,
            max(temperatura) as temperatura_maxima
    FROM read_csv("data/measurements.txt", AUTO_DETECT=FALSE, sep=";" , columns = {'station':VARCHAR, 'temperatura': 'DECIMAL(3,1)'})
    GROUP BY station
    ORDER BY station
    """)
    
    
    result.show()

    result.write_parquet('data/measurements_sumary.parquet')

if __name__ == "__main__":
    import time
    start_time = time.time()
    creat_duckdb()
    took = time.time() - start_time
    print(f"Duckdb took: {took:.2f} sec")
