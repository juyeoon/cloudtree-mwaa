# cloudtree-mwaa
cloudtree 프로젝트 - Amazon Managed Workflows for Apache Airflow (MWAA) Source

[notion 1](https://www.notion.so/MWAA-test-178a579d5bce80418649d30eb2c665c6)  
[notion 2](https://www.notion.so/MWAA-175a579d5bce80d894e1fbced7ab2032?pvs=4)

![flow-chart](https://github.com/user-attachments/assets/e2355f57-42b0-4c33-9518-81af1c3ad5e1)

![s3-raw-bucket](https://github.com/user-attachments/assets/9b23b4c0-9cc4-4987-8867-4bb91af486a2)

![glue-job](https://github.com/user-attachments/assets/f3682336-4184-4f8c-9fbf-25fa9de33575)



- data1 : 인기대출도서

    - 매월 1일 (1개월 주기)

    -  lambda -> athena(raw table msck) -> glue job -> athena(transformed table msck)
    
- data2 : 문화행사

    - 매월 1일 (1개월 주기)

    -  lambda -> athena(raw table msck) -> glue job -> athena(raw table msck) -> glue job -> athena(transformed table msck)

- data3 : 도서관

    - 6월 18일, 12월 18일 (6개월 주기)

    - lambda -> athena(raw table msck) -> glue job -> athena(transformed table msck)

- data4 : 버스정류장

    - 6월 18일, 12월 18일 (6개월 주기)

    - lambda -> athena(raw table msck) -> glue job -> athena(transformed table msck)

- data5 : 지하철역

    - 6월 18일, 12월 18일 (6개월 주기)

    - lambda -> athena(raw table msck) -> glue job -> athena(transformed table msck)
    
- data6 : 도시공원

    - 6월 18일, 12월 18일 (6개월 주기)

    - athena(raw table msck) -> glue job -> athena(transformed table msck)

- DAGs

    - DAG1
        
        - 인기대출도서(data1) 1,2,3,4 
        
        - 문화행사 1,2,3,2,3,4

    - DAG2
        
        - 도서관, 버스정류장, 지하철역 1,2,3,4 
        
        - 공원 2,3,4

    - DAG3: 5,6,7,8

- schedule

-   매월 1일: DAG1 → DAG3

-   6월 18일, 12월 18일: DAG2 → DAG3